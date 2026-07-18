/*
Copyright (c) 2026 Microbus LLC and various contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package seamster

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestSeamster_DisabledIsInert(t *testing.T) {
	s := New(false)
	// Arming has no effect because consults short-circuit on the disabled gate.
	s.Inject("boom")
	s.InjectN(5, "boom")
	if s.IsFault("boom") {
		t.Fatal("disabled Seamster fired a fault")
	}
	if s.Enabled() {
		t.Fatal("Enabled reported true for a disabled Seamster")
	}
	// Checkpoint on a disabled Seamster must not block even with a breakpoint armed.
	s.Break("cp")
	done := make(chan struct{})
	go func() {
		s.Checkpoint(context.Background(), "cp")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Checkpoint blocked on a disabled Seamster")
	}
}

func TestSeamster_FaultFiresOnce(t *testing.T) {
	s := New(true)
	s.Inject("boom")
	if !s.IsFault("boom") {
		t.Fatal("armed fault did not fire")
	}
	if s.IsFault("boom") {
		t.Fatal("one-shot fault fired twice")
	}
}

func TestSeamster_FaultCountAndClear(t *testing.T) {
	s := New(true)
	s.InjectN(3, "boom")
	fires := 0
	for s.IsFault("boom") {
		fires++
		if fires > 10 {
			t.Fatal("fault never stopped firing")
		}
	}
	if fires != 3 {
		t.Fatalf("expected 3 fires, got %d", fires)
	}

	s.InjectN(5, "boom")
	s.Withdraw("boom")
	if s.IsFault("boom") {
		t.Fatal("cleared fault still fired")
	}
}

func TestSeamster_ScopedFaultsAreIndependent(t *testing.T) {
	s := New(true)
	s.Inject("task", "Charge")
	if s.IsFault("task", "Refund") {
		t.Fatal("fault scoped to Charge fired for Refund")
	}
	if !s.IsFault("task", "Charge") {
		t.Fatal("fault scoped to Charge did not fire for Charge")
	}
}

func TestSeamster_WaitRendezvous(t *testing.T) {
	s := New(true)
	reached := make(chan struct{})
	go func() {
		s.Wait("cp")
		close(reached)
	}()
	// Give the waiter a moment to register, then the host passes the checkpoint.
	time.Sleep(10 * time.Millisecond)
	s.Checkpoint(context.Background(), "cp")
	select {
	case <-reached:
	case <-time.After(time.Second):
		t.Fatal("Wait did not observe the checkpoint")
	}
}

func TestSeamster_BreakpointFreezeAndRelease(t *testing.T) {
	s := New(true)
	s.Break("cp")

	frozen := make(chan struct{})
	released := make(chan struct{})
	go func() {
		close(frozen)
		s.Checkpoint(context.Background(), "cp") // blocks until cleared
		close(released)
	}()

	<-frozen
	// Wait must return once the host is frozen at the breakpoint, even racing arrival.
	s.Wait("cp")

	select {
	case <-released:
		t.Fatal("host proceeded past the breakpoint before it was cleared")
	case <-time.After(20 * time.Millisecond):
	}

	s.Resume("cp")
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("host did not proceed after the breakpoint was cleared")
	}
}

func TestSeamster_VisitsCountsCheckpointArrivals(t *testing.T) {
	s := New(true)
	ctx := context.Background()

	if got := s.Visits("cp"); got != 0 {
		t.Fatalf("expected 0 visits before any arrival, got %d", got)
	}
	for i := 1; i <= 3; i++ {
		s.Checkpoint(ctx, "cp")
		if got := s.Visits("cp"); got != i {
			t.Fatalf("after %d arrivals expected %d visits, got %d", i, i, got)
		}
	}
	// Distinct names count independently, and reading does not consume.
	if got := s.Visits("other"); got != 0 {
		t.Fatalf("unrelated checkpoint should have 0 visits, got %d", got)
	}
	if got := s.Visits("cp"); got != 3 {
		t.Fatalf("Visits is a passive read; expected 3 still, got %d", got)
	}

	// A visit is counted even when the arrival then blocks on a breakpoint.
	s.Break("cp")
	go s.Checkpoint(ctx, "cp") // reaches the checkpoint (counted), then blocks
	s.Wait("cp")               // returns once frozen at the breakpoint
	if got := s.Visits("cp"); got != 4 {
		t.Fatalf("a breakpoint-blocked arrival should still count; expected 4, got %d", got)
	}
	s.Resume("cp")

	// Disabled Seamster counts nothing.
	d := New(false)
	d.Checkpoint(ctx, "cp")
	if got := d.Visits("cp"); got != 0 {
		t.Fatalf("disabled Seamster should report 0 visits, got %d", got)
	}
}

func TestSeamster_CheckpointReleasesOnContextCancel(t *testing.T) {
	s := New(true)
	s.Break("cp")
	ctx, cancel := context.WithCancel(context.Background())

	returned := make(chan struct{})
	go func() {
		s.Checkpoint(ctx, "cp")
		close(returned)
	}()
	// Never clear the breakpoint; cancelling ctx must unwedge the host.
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("Checkpoint did not return on context cancellation")
	}
	s.Resume("cp")
}

func TestSeamster_ConcurrentArmAndConsult(t *testing.T) {
	// Exercised under -race: arming and consulting from many goroutines must not race.
	s := New(true)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); s.Inject("boom") }()
		go func() { defer wg.Done(); s.IsFault("boom") }()
	}
	wg.Wait()
}
