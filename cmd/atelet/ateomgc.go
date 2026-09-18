// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

// The ateom directory janitor.
//
// Every ateom registers itself on the node by creating ateoms/<pod-UID>/ and
// listening on ateom.sock inside it. A gracefully terminated ateom removes
// the directory on the way out; one killed outright (SIGKILL after the grace
// period, OOM, node crash) leaves it behind, and nothing else ever removes
// it. The stats sweep enumerates that directory as its discovery registry,
// so each leftover costs a dial and a probe every sweep.
//
// The janitor removes those leftovers. It is deliberately slow to convict,
// because a wrong deletion is severe and self-hiding: a bound unix listener
// keeps serving established connections on the unlinked inode with no
// error, but every NEW connect fails ENOENT forever (nothing re-binds the
// socket), while the worker keeps advertising capacity -- a worker
// blackhole with no checkpoint or stop control over its actor. So a
// directory is removed only when three independent kinds of evidence agree
// across several passes: its pod is absent from the node's pod list, its
// ateom does not answer a probe, and it has been that way for a while.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/pflag"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

var (
	ateomGCPeriod = pflag.Duration("ateom-gc-period", 10*time.Minute, "How often to sweep for the directories of worker pods that no longer exist. 0 disables the janitor.")
	ateomGCMinAge = pflag.Duration("ateom-gc-min-age", 10*time.Minute, "Directories modified more recently than this are never removed.")
	ateomGCDryRun = pflag.Bool("ateom-gc-dry-run", false, "Log removal decisions without removing anything.")
)

// ateomGCStrikes is how many consecutive passes must find a directory
// orphaned -- pod absent from the node's list AND socket unresponsive -- before
// it is removed. Growth is rollout-driven and slow, so cleanup latency is
// free; each extra pass converts "one evidence source was wrong once" into
// "the same source was wrong N times in a row while the socket also stayed
// dead", which is what makes a false positive unreachable in practice.
const ateomGCStrikes = 3

// ateomGCProbeTimeout bounds one liveness probe. A live ateom answers a
// GetActiveWorkloadStats in milliseconds; this only has to outlast a
// momentarily busy one.
const ateomGCProbeTimeout = 5 * time.Second

func validateAteomGCFlags() error {
	if *ateomGCPeriod < 0 {
		return fmt.Errorf("--ateom-gc-period %v must be >= 0", *ateomGCPeriod)
	}
	if *ateomGCMinAge < 0 {
		// A negative min-age would put the cutoff in the future and make a
		// just-created directory removable.
		return fmt.Errorf("--ateom-gc-min-age %v must be >= 0", *ateomGCMinAge)
	}
	return nil
}

// ateomGC is the janitor's state: configuration snapshotted at construction
// (the pass logic never reads globals), the seams that make a pass testable
// without a node, and the strike counters that carry evidence across passes.
type ateomGC struct {
	ateomsDir string
	period    time.Duration
	minAge    time.Duration
	dryRun    bool

	// listNodePodUIDs returns the UIDs of every pod on this node. Unlike the
	// stats sweep's pool fetcher, an error is distinguished from an empty
	// result: a failed list aborts the pass, because deletion evidence
	// cannot be inferred from "the apiserver did not answer".
	listNodePodUIDs func(ctx context.Context) (map[string]struct{}, error)

	// probe asks the ateom behind podUID whether it is alive. A nil error
	// means it answered -- with anything at all, an idle ateom included --
	// which vetoes removal regardless of what the pod list said.
	probe func(ctx context.Context, podUID string) error

	// now is the clock, injected so tests can age directories.
	now func() time.Time

	// strikes counts consecutive orphaned passes per pod UID. Entries are
	// dropped as soon as a directory is seen live, or is gone.
	strikes map[string]int
}

func newAteomGC(client kubernetes.Interface, nodeName string) *ateomGC {
	return &ateomGC{
		ateomsDir:       ateompath.AteomsDir(),
		period:          *ateomGCPeriod,
		minAge:          *ateomGCMinAge,
		dryRun:          *ateomGCDryRun,
		listNodePodUIDs: nodePodUIDLister(client, nodeName),
		probe:           probeAteom,
		now:             time.Now,
		strikes:         make(map[string]int),
	}
}

// Run executes passes on the configured period until ctx is done. Passes
// are strictly serialized. The first pass waits one period rather than
// running at startup: a node that just booted has nothing to reap, and the
// strike rule needs consecutive passes anyway.
func (g *ateomGC) Run(ctx context.Context) {
	ticker := time.NewTicker(g.period)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		g.runPass(ctx)
	}
}

// runPass performs one pass. It recovers from panics: a background janitor
// must not take the node's lifecycle daemon down with it.
func (g *ateomGC) runPass(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "Ateom GC pass panicked; skipping this pass",
				slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
		}
	}()

	entries, err := os.ReadDir(g.ateomsDir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.WarnContext(ctx, "Ateom GC: listing the ateoms directory failed; skipping this pass", slog.Any("err", err))
		}
		return
	}

	// The pod list is taken once per pass and is required. Without it there
	// is no evidence, and a pass with no evidence advances no strikes.
	podUIDs, err := g.listNodePodUIDs(ctx)
	if err != nil {
		slog.WarnContext(ctx, "Ateom GC: listing the node's pods failed; skipping this pass", slog.Any("err", err))
		return
	}

	seen := make(map[string]struct{}, len(entries))
	var removed, kept int
	for _, e := range entries {
		name := e.Name()
		// Only directories named by a pod UID are ours. Anything else is
		// operator debris, and not ours to delete.
		if !e.IsDir() {
			continue
		}
		if _, err := uuid.Parse(name); err != nil {
			slog.WarnContext(ctx, "Ateom GC: skipping an entry that is not a pod UID", slog.String("name", name))
			continue
		}
		seen[name] = struct{}{}

		if _, live := podUIDs[name]; live {
			delete(g.strikes, name)
			continue
		}
		// The pod is not on this node. Before counting that as evidence, ask
		// the ateom itself: a pod list can be momentarily incomplete, and a
		// directory whose ateom answers must never be removed.
		if err := g.probeAteom(ctx, name); err == nil {
			delete(g.strikes, name)
			continue
		}

		g.strikes[name]++
		if g.strikes[name] < ateomGCStrikes {
			kept++
			continue
		}
		dir := filepath.Join(g.ateomsDir, name)
		if age, ok := g.dirAge(dir); !ok || age < g.minAge {
			kept++
			continue
		}

		if g.dryRun {
			slog.InfoContext(ctx, "Ateom GC: would remove the orphaned directory (dry run)",
				slog.String("pod_uid", name), slog.Int("strikes", g.strikes[name]))
			continue
		}
		if err := os.RemoveAll(dir); err != nil {
			slog.WarnContext(ctx, "Ateom GC: removing the orphaned directory failed",
				slog.String("pod_uid", name), slog.Any("err", err))
			continue
		}
		delete(g.strikes, name)
		removed++
		slog.InfoContext(ctx, "Ateom GC: removed the orphaned directory", slog.String("pod_uid", name))
	}

	// Strikes are evidence about directories that exist; drop the rest so
	// the map cannot outgrow the directory set.
	for name := range g.strikes {
		if _, ok := seen[name]; !ok {
			delete(g.strikes, name)
		}
	}

	if removed > 0 || kept > 0 {
		slog.InfoContext(ctx, "Ateom GC pass complete",
			slog.Int("removed", removed), slog.Int("pending", kept), slog.Bool("dry_run", g.dryRun))
	}
}

// probeAteom runs the configured probe under the probe timeout.
func (g *ateomGC) probeAteom(ctx context.Context, podUID string) error {
	probeCtx, cancel := context.WithTimeout(ctx, ateomGCProbeTimeout)
	defer cancel()
	return g.probe(probeCtx, podUID)
}

// dirAge is how long ago dir was last modified. Not ok when it cannot be
// stated -- a directory that vanished between the listing and now is simply
// not a candidate this pass.
func (g *ateomGC) dirAge(dir string) (time.Duration, bool) {
	info, err := os.Stat(dir)
	if err != nil {
		return 0, false
	}
	return g.now().Sub(info.ModTime()), true
}

// nodePodUIDLister returns the real pod lister: every pod on nodeName, by
// UID. No label selector -- membership by node and UID is the evidence, and a
// pod that lost a label must not become removable.
func nodePodUIDLister(client kubernetes.Interface, nodeName string) func(ctx context.Context) (map[string]struct{}, error) {
	return func(ctx context.Context) (map[string]struct{}, error) {
		pods, err := client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
			FieldSelector: "spec.nodeName=" + nodeName,
		})
		if err != nil {
			return nil, err
		}
		uids := make(map[string]struct{}, len(pods.Items))
		for _, pod := range pods.Items {
			uids[string(pod.UID)] = struct{}{}
		}
		return uids, nil
	}
}

// probeAteom is the real probe: the stats sweep's discovery read over a
// short-lived connection. Any response is proof of life; the reply's content
// does not matter.
func probeAteom(ctx context.Context, podUID string) error {
	conn, closer, err := dialAteomStats(podUID)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, err = ateompb.NewAteomClient(conn).GetActiveWorkloadStats(ctx, &ateompb.GetActiveWorkloadStatsRequest{})
	return err
}
