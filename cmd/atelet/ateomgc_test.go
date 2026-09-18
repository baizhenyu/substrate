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

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

const (
	gcUIDLive   = "11111111-1111-4111-8111-111111111111"
	gcUIDOrphan = "22222222-2222-4222-8222-222222222222"
)

// gcFixture is a janitor over a temp ateoms directory with scripted evidence:
// which pods the list returns (or an error), and which UIDs answer a probe.
type gcFixture struct {
	g       *ateomGC
	dir     string
	pods    map[string]struct{}
	listErr error
	alive   map[string]bool
	now     time.Time
	probes  int
}

func newGCFixture(t *testing.T) *gcFixture {
	t.Helper()
	f := &gcFixture{
		dir:   t.TempDir(),
		pods:  map[string]struct{}{},
		alive: map[string]bool{},
		now:   time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	}
	f.g = &ateomGC{
		ateomsDir: f.dir,
		minAge:    10 * time.Minute,
		listNodePodUIDs: func(context.Context) (map[string]struct{}, error) {
			if f.listErr != nil {
				return nil, f.listErr
			}
			return f.pods, nil
		},
		probe: func(_ context.Context, uid string) error {
			f.probes++
			if f.alive[uid] {
				return nil
			}
			return errors.New("connect: no such file or directory")
		},
		now:     func() time.Time { return f.now },
		strikes: map[string]int{},
	}
	return f
}

// mkdir creates an ateom directory whose mtime is age before the fixture
// clock, so the min-age veto is deterministic.
func (f *gcFixture) mkdir(t *testing.T, uid string, age time.Duration) {
	t.Helper()
	dir := filepath.Join(f.dir, uid)
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("mkdir %s: %v", uid, err)
	}
	mtime := f.now.Add(-age)
	if err := os.Chtimes(dir, mtime, mtime); err != nil {
		t.Fatalf("chtimes %s: %v", uid, err)
	}
}

func (f *gcFixture) exists(uid string) bool {
	_, err := os.Stat(filepath.Join(f.dir, uid))
	return err == nil
}

func (f *gcFixture) passes(n int) {
	for range n {
		f.g.runPass(context.Background())
	}
}

func TestAteomGCRemovesOrphanAfterStrikes(t *testing.T) {
	f := newGCFixture(t)
	f.mkdir(t, gcUIDOrphan, time.Hour)

	f.passes(ateomGCStrikes - 1)
	if !f.exists(gcUIDOrphan) {
		t.Fatalf("directory removed after %d passes, want it kept until %d", ateomGCStrikes-1, ateomGCStrikes)
	}
	f.passes(1)
	if f.exists(gcUIDOrphan) {
		t.Errorf("directory still present after %d orphaned passes", ateomGCStrikes)
	}
	if _, ok := f.g.strikes[gcUIDOrphan]; ok {
		t.Errorf("strike entry kept after removal")
	}
}

// TestAteomGCLivePodNeverCandidate: a pod in the node's list is never probed
// and never struck, however old its directory is.
func TestAteomGCLivePodNeverCandidate(t *testing.T) {
	f := newGCFixture(t)
	f.mkdir(t, gcUIDLive, time.Hour)
	f.pods[gcUIDLive] = struct{}{}

	f.passes(ateomGCStrikes + 2)
	if !f.exists(gcUIDLive) {
		t.Fatal("live pod's directory removed")
	}
	if f.probes != 0 {
		t.Errorf("live pod probed %d times, want 0", f.probes)
	}
}

// TestAteomGCSocketAnswerVetoes pins the backstop against a partial pod
// list: a UID absent from the list whose ateom answers is never removed, and
// its strikes reset.
func TestAteomGCSocketAnswerVetoes(t *testing.T) {
	f := newGCFixture(t)
	f.mkdir(t, gcUIDOrphan, time.Hour)

	f.passes(ateomGCStrikes - 1) // two strikes accrued
	f.alive[gcUIDOrphan] = true
	f.passes(1)
	if got := f.g.strikes[gcUIDOrphan]; got != 0 {
		t.Errorf("strikes after an answering probe = %d, want 0", got)
	}
	f.alive[gcUIDOrphan] = false
	f.passes(ateomGCStrikes - 1)
	if !f.exists(gcUIDOrphan) {
		t.Error("directory removed without the full run of consecutive strikes after the veto")
	}
}

// TestAteomGCListErrorAbortsPass: no pod list, no evidence -- strikes must
// not advance.
func TestAteomGCListErrorAbortsPass(t *testing.T) {
	f := newGCFixture(t)
	f.mkdir(t, gcUIDOrphan, time.Hour)

	f.listErr = errors.New("apiserver unavailable")
	f.passes(ateomGCStrikes * 2)
	if !f.exists(gcUIDOrphan) {
		t.Fatal("directory removed while the pod list was failing")
	}
	if got := f.g.strikes[gcUIDOrphan]; got != 0 {
		t.Errorf("strikes advanced to %d during list failures, want 0", got)
	}
	if f.probes != 0 {
		t.Errorf("probed %d times during list failures, want 0", f.probes)
	}
}

func TestAteomGCMinAgeVetoes(t *testing.T) {
	f := newGCFixture(t)
	f.mkdir(t, gcUIDOrphan, time.Minute) // younger than the 10m min-age

	f.passes(ateomGCStrikes + 1)
	if !f.exists(gcUIDOrphan) {
		t.Fatal("young directory removed")
	}
	// Age it past the cutoff: strikes are already at the threshold, so the
	// next pass removes it.
	f.now = f.now.Add(time.Hour)
	f.passes(1)
	if f.exists(gcUIDOrphan) {
		t.Error("aged directory not removed")
	}
}

func TestAteomGCSkipsNonUUIDEntries(t *testing.T) {
	f := newGCFixture(t)
	for _, name := range []string{"lost+found", "notes.txt"} {
		p := filepath.Join(f.dir, name)
		var err error
		if name == "notes.txt" {
			err = os.WriteFile(p, []byte("x"), 0o600)
		} else {
			err = os.Mkdir(p, 0o700)
		}
		if err != nil {
			t.Fatal(err)
		}
	}

	f.passes(ateomGCStrikes + 1)
	for _, name := range []string{"lost+found", "notes.txt"} {
		if _, err := os.Stat(filepath.Join(f.dir, name)); err != nil {
			t.Errorf("%s removed; only pod-UID directories are the janitor's", name)
		}
	}
	if f.probes != 0 {
		t.Errorf("non-UUID entries probed %d times, want 0", f.probes)
	}
}

func TestAteomGCDryRunRemovesNothing(t *testing.T) {
	f := newGCFixture(t)
	f.g.dryRun = true
	f.mkdir(t, gcUIDOrphan, time.Hour)

	f.passes(ateomGCStrikes + 2)
	if !f.exists(gcUIDOrphan) {
		t.Error("dry run removed a directory")
	}
}

// TestAteomGCStrikesPrunedWithDirectory: a strike entry does not outlive its
// directory.
func TestAteomGCStrikesPrunedWithDirectory(t *testing.T) {
	f := newGCFixture(t)
	f.mkdir(t, gcUIDOrphan, time.Hour)

	f.passes(1)
	if got := f.g.strikes[gcUIDOrphan]; got != 1 {
		t.Fatalf("strikes = %d, want 1", got)
	}
	if err := os.RemoveAll(filepath.Join(f.dir, gcUIDOrphan)); err != nil {
		t.Fatal(err)
	}
	f.passes(1)
	if _, ok := f.g.strikes[gcUIDOrphan]; ok {
		t.Error("strike entry kept for a directory that no longer exists")
	}
}

// TestAteomGCMissingAteomsDirIsQuiet: a node whose first ateom has not booted
// has no ateoms directory; that is empty work, not an error.
func TestAteomGCMissingAteomsDirIsQuiet(t *testing.T) {
	f := newGCFixture(t)
	f.g.ateomsDir = filepath.Join(f.dir, "absent")
	f.passes(1) // must not panic or strike
}

func TestAteomGCRunPassRecoversPanic(t *testing.T) {
	f := newGCFixture(t)
	f.mkdir(t, gcUIDOrphan, time.Hour)
	f.g.probe = func(context.Context, string) error { panic("probe bug") }
	f.passes(1) // must return
}

func TestNodePodUIDLister(t *testing.T) {
	client := k8sfake.NewSimpleClientset(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "ns", UID: gcUIDLive}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "other", UID: gcUIDOrphan}},
	)
	// The fake clientset ignores field selectors, so node scoping is not
	// assertable here; what is pinned is the UID keying across namespaces.
	got, err := nodePodUIDLister(client, "node-1")(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, uid := range []string{gcUIDLive, gcUIDOrphan} {
		if _, ok := got[uid]; !ok {
			t.Errorf("uid %s missing from %v", uid, got)
		}
	}
}

func TestValidateAteomGCFlags(t *testing.T) {
	origPeriod, origMinAge := *ateomGCPeriod, *ateomGCMinAge
	t.Cleanup(func() { *ateomGCPeriod, *ateomGCMinAge = origPeriod, origMinAge })

	*ateomGCPeriod, *ateomGCMinAge = 0, 0
	if err := validateAteomGCFlags(); err != nil {
		t.Errorf("zero values rejected: %v", err)
	}
	*ateomGCPeriod = -time.Second
	if err := validateAteomGCFlags(); err == nil {
		t.Error("negative period accepted")
	}
	*ateomGCPeriod, *ateomGCMinAge = time.Minute, -time.Second
	if err := validateAteomGCFlags(); err == nil {
		t.Error("negative min-age accepted")
	}
}
