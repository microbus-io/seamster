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

func TestSeamster_WaiterRendezvous(t *testing.T) {
	s := New(true)
	// Arm first, then let the host pass the checkpoint - no goroutine and no sleep-to-register, because Waiter
	// registers the waiter before it returns.
	reached := s.Waiter("cp")
	s.Checkpoint(context.Background(), "cp")
	select {
	case <-reached:
	case <-time.After(time.Second):
		t.Fatal("Waiter did not observe the checkpoint")
	}
}

// The property Waiter exists for: a checkpoint the host passes between arming and receiving is still observed.
// This test cannot be written against the blocking Wait - there the receive IS the arming, so the host's arrival
// would be missed and the rendezvous would hang.
func TestSeamster_WaiterArmedBeforeArrivalIsNotMissed(t *testing.T) {
	s := New(true)
	reached := s.Waiter("cp")

	// The host arrives (and departs) entirely before anyone receives.
	s.Checkpoint(context.Background(), "cp")
	time.Sleep(10 * time.Millisecond)

	select {
	case <-reached:
	default:
		t.Fatal("a checkpoint reached after arming but before receiving was lost")
	}
}

// A disabled Seamster must never block a receive, so host code carrying a wait cannot wedge in production.
func TestSeamster_WaiterOnDisabledDoesNotBlock(t *testing.T) {
	s := New(false)
	select {
	case <-s.Waiter("cp"):
	case <-time.After(time.Second):
		t.Fatal("Waiter blocked on a disabled Seamster")
	}
}

// Wait must end on ctx rather than hang when the host never arrives, and must say which happened.
func TestSeamster_WaitAbortsOnContext(t *testing.T) {
	s := New(true)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if s.Wait(ctx, "never") {
		t.Fatal("Wait reported an arrival that never happened")
	}
	// An arrival still reports true even on an already-cancelled ctx. A plain two-case select would pick at
	// random here and turn a checkpoint the host DID reach into an intermittent miss.
	s.Break("cp")
	go s.Checkpoint(context.Background(), "cp")
	for s.Visits("cp") == 0 {
		time.Sleep(time.Millisecond)
	}
	if !s.Wait(ctx, "cp") {
		t.Fatal("Wait reported a miss for a checkpoint the host had reached")
	}
	s.Resume("cp")
}

func TestSeamster_WaitTimeoutBoundsTheWait(t *testing.T) {
	s := New(true)
	if s.WaitTimeout(context.Background(), 20*time.Millisecond, "never") {
		t.Fatal("WaitTimeout reported an arrival that never happened")
	}
	// A disabled Seamster has nothing to wait for, so it reports arrival immediately rather than burning the
	// timeout - host code carrying a wait must not stall in production.
	if !New(false).WaitTimeout(context.Background(), time.Hour, "never") {
		t.Fatal("WaitTimeout on a disabled Seamster did not report immediately")
	}
}

// Scoped checkpoints are independent keys, and a scoped fire does not wake an unscoped waiter - the same
// contract faults carry, so the two seams are spelled and reasoned about the same way.
func TestSeamster_ScopedCheckpointsAreIndependent(t *testing.T) {
	s := New(true)
	ctx := context.Background()

	unscoped := s.Waiter("stopped")
	other := s.Waiter("stopped", "flow-2", "failed")
	target := s.Waiter("stopped", "flow-1", "completed")

	s.Checkpoint(ctx, "stopped", "flow-1", "completed")

	select {
	case <-target:
	default:
		t.Fatal("a waiter scoped to the fired checkpoint was not woken")
	}
	select {
	case <-other:
		t.Fatal("a differently-scoped waiter was woken")
	default:
	}
	select {
	case <-unscoped:
		t.Fatal("an unscoped waiter was woken by a scoped fire")
	default:
	}

	// Visits counts per key, so the scoped arrival is invisible to the unscoped counter.
	if got := s.Visits("stopped", "flow-1", "completed"); got != 1 {
		t.Fatalf("expected 1 scoped visit, got %d", got)
	}
	if got := s.Visits("stopped"); got != 0 {
		t.Fatalf("expected 0 unscoped visits, got %d", got)
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
	s.Wait(context.Background(), "cp")

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

func TestSeamster_BreakpointHoldsConcurrentArrivals(t *testing.T) {
	s := New(true)
	s.Break("cp")

	const goroutines = 8
	released := make(chan struct{}, goroutines)
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Checkpoint(context.Background(), "cp") // every arrival freezes, none panics on a double close
			released <- struct{}{}
		}()
	}

	// Wait returns as soon as the first of the fan-out arrives.
	s.Wait(context.Background(), "cp")
	select {
	case <-released:
		t.Fatal("a goroutine proceeded past the breakpoint before it was cleared")
	case <-time.After(20 * time.Millisecond):
	}

	// One Resume releases the whole fan-out.
	s.Resume("cp")
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("only %d of %d goroutines proceeded after the breakpoint was cleared", len(released), goroutines)
	}
	if got := s.Visits("cp"); got != goroutines {
		t.Fatalf("expected %d visits, got %d", goroutines, got)
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
	go s.Checkpoint(ctx, "cp")         // reaches the checkpoint (counted), then blocks
	s.Wait(context.Background(), "cp") // returns once frozen at the breakpoint
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
	for range 50 {
		wg.Add(2)
		go func() { defer wg.Done(); s.Inject("boom") }()
		go func() { defer wg.Done(); s.IsFault("boom") }()
	}
	wg.Wait()
}
