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
	"time"
)

// breakpoint is one armed breakpoint: release is closed by Resume to let every held goroutine proceed.
type breakpoint struct {
	release chan struct{}
	// held counts the goroutines blocked on release right now, so a rendezvous armed after the host froze still
	// observes the arrival. A count, not a channel closed on first arrival: see CLAUDE.md. Only under Seamster.mu.
	held int
}

// Checkpoint is the host-side consult at an instrumented site, and the only one a host calls. It makes the site
// OBSERVABLE and PAUSABLE: a test rendezvouses with the host's arrival ([Seamster.Waiter] / [Seamster.Wait] /
// [Seamster.WaitTimeout]), freezes the host there ([Seamster.Break] / [Seamster.Resume]), or counts arrivals
// ([Seamster.Visits]).
//
// Free in production: it returns on the enabled bool read, before any lock. When enabled it counts the visit,
// wakes any waiters, and - if a breakpoint is armed - blocks until [Seamster.Resume], or until ctx is done, so a
// stuck test or a shutdown can never wedge the host goroutine forever.
//
// Distinct names are DIFFERENT checkpoints: firing one does not wake a waiter armed on another, and each keeps
// its own visit count. A host wanting both a fleet-wide and a per-entity granularity fires both.
func (s *Seamster) Checkpoint(ctx context.Context, checkpointName string) {
	if !s.enabled {
		return
	}
	s.mu.Lock()
	if s.visits == nil {
		s.visits = make(map[string]int)
	}
	s.visits[checkpointName]++
	for _, ch := range s.waitFors[checkpointName] {
		close(ch)
	}
	delete(s.waitFors, checkpointName)
	bp := s.breakpoints[checkpointName]
	if bp != nil {
		bp.held++
	}
	s.mu.Unlock()

	if bp != nil {
		select {
		case <-bp.release:
		case <-ctx.Done():
		}
		s.mu.Lock()
		bp.held--
		s.mu.Unlock()
	}
}

// Waiter registers a waiter for the named checkpoint and returns a channel closed when the host reaches it. It
// does not block: arming and receiving are separate steps. Arm before the operation that triggers the
// checkpoint, receive after it:
//
//	reached := s.Waiter("beforeCommit")
//	host.DoTheThing()
//	<-reached
//
// Use it whenever the caller's OWN next statement drives the host to the checkpoint: [Seamster.Wait] and
// [Seamster.WaitTimeout] can only arm once that statement is already running, so an arrival in between is lost.
//
// The channel is already closed if a breakpoint is currently holding the host at name, and on a disabled
// Seamster - so a receive never blocks in production.
func (s *Seamster) Waiter(checkpointName string) <-chan struct{} {
	ch := make(chan struct{})
	if !s.enabled {
		close(ch)
		return ch
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if bp := s.breakpoints[checkpointName]; bp != nil && bp.held > 0 {
		close(ch) // host already frozen at this breakpoint
		return ch
	}
	if s.waitFors == nil {
		s.waitFors = make(map[string][]chan struct{})
	}
	s.waitFors[checkpointName] = append(s.waitFors[checkpointName], ch)
	return ch
}

// Wait blocks until the host reaches the named checkpoint and reports whether it did, abandoning the wait if ctx
// is done. A false return is the caller's cue to fail:
//
//	assert.True(s.Wait(ctx, "beforeCommit"))
//
// Returns true immediately on a disabled Seamster, or if a breakpoint is already holding the host at name.
//
// Use it where the host is ALREADY running into the checkpoint: frozen at a breakpoint, or driven from another
// goroutine. If the caller's own next statement is the trigger, this cannot arm in time - use [Seamster.Waiter].
func (s *Seamster) Wait(ctx context.Context, checkpointName string) bool {
	reached := s.Waiter(checkpointName)
	// Prefer the arrival when both are ready. A lone two-case select picks at random, so a checkpoint the host
	// DID reach would report as a miss whenever ctx expires in the same instant.
	select {
	case <-reached:
		return true
	default:
	}
	select {
	case <-reached:
		return true
	case <-ctx.Done():
		return false
	}
}

// WaitTimeout is [Seamster.Wait] bounded by a duration - the usual shape in a test, where the bound is "this
// long" rather than a deadline inherited from elsewhere. ctx still aborts, so a cancelled parent ends it early.
func (s *Seamster) WaitTimeout(ctx context.Context, checkpointName string, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return s.Wait(ctx, checkpointName)
}

// Break arms a breakpoint so the host blocks the next time it reaches the named checkpoint, until
// [Seamster.Resume] releases it. It holds every goroutine that reaches the checkpoint while armed, not only the
// first, so a fan-out can be gathered at one point and released together by a single Resume; a rendezvous
// returns as soon as the first of them arrives.
//
// To release concurrent arrivals one at a time instead, give each entity its own checkpoint name and break on
// each; resume one, wait for it to reach a later checkpoint, then resume the next.
//
// It composes with a rendezvous to drive a concurrent operation into a precise window with no timing hammer:
// Break, start the host operation, wait for it to freeze there, run the racing operation, Resume. A waiter armed
// for a checkpoint that is already holding the host fires immediately, so this is race-free whichever happens
// first.
//
// Reach for it only to genuinely HOLD the host. A rendezvous is already race-free on its own, and freezing
// perturbs the timing under test - a Break armed merely to make an observation reliable is doing nothing a
// [Seamster.Waiter] would not.
//
// Arming a breakpoint that is already armed is a no-op: whatever it holds stays held, and one Resume releases it.
func (s *Seamster) Break(checkpointName string) {
	if !s.enabled {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.breakpoints[checkpointName] != nil {
		// Replacing the entry would strand the goroutines it holds and hide them from Waiter. See CLAUDE.md.
		return
	}
	if s.breakpoints == nil {
		s.breakpoints = make(map[string]*breakpoint)
	}
	s.breakpoints[checkpointName] = &breakpoint{release: make(chan struct{})}
}

// Visits reports how many times the host has passed the named checkpoint, counting a call that then blocked on a
// breakpoint (reaching the checkpoint is the visit; blocking comes after). It never blocks, consumes nothing, and
// can be read repeatedly. The count is monotonic for the Seamster's lifetime, so capture a baseline first to
// assert on visits accrued within a window. Zero for a checkpoint never reached, and always zero on a disabled
// Seamster.
func (s *Seamster) Visits(checkpointName string) int {
	if !s.enabled {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.visits[checkpointName]
}

// Resume releases the host frozen at the named breakpoint and disarms it. A no-op if none is armed.
func (s *Seamster) Resume(checkpointName string) {
	if !s.enabled {
		return
	}
	s.mu.Lock()
	bp := s.breakpoints[checkpointName]
	delete(s.breakpoints, checkpointName)
	s.mu.Unlock()
	if bp != nil {
		close(bp.release)
	}
}
