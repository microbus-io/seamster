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

// Where a fault makes a site misbehave, a checkpoint makes a site OBSERVABLE and PAUSABLE. The host passes through
// a named checkpoint by calling [Seamster.Checkpoint]; a test drives it three ways:
//
//   - Rendezvous - wait for the host to reach the checkpoint. [Seamster.Waiter] arms a waiter and returns a
//     channel to receive on; [Seamster.Wait] arms and blocks; [Seamster.WaitTimeout] arms and blocks with a
//     bound. All arm for the host's NEXT arrival, so a checkpoint already passed does not re-fire.
//   - Freeze - [Seamster.Break] holds the host at the checkpoint until [Seamster.Resume] clears it, so a test
//     can run a racing operation while the host sits at an exact point.
//   - Count - [Seamster.Visits] reports how many times the host has passed the checkpoint ("the retry ran
//     exactly 3 times"). Passive: no arming, never blocks.
//
// Checkpoints scope like faults: every method takes trailing scope args, joined into the key the same way at both
// ends, so a test targets one entity rather than the next thing that happens. A scoped fire and an unscoped one
// are DIFFERENT checkpoints - passing scope does not also wake an unscoped waiter, and each key counts its own
// visits - so a host wanting both fires both. ([Seamster.WaitTimeout] takes its timeout before the name to keep
// the scope trailing and variadic, as it is everywhere else.)
//
// Choosing a rendezvous is a question about the caller. Use Waiter when the caller's OWN next statement is what
// drives the host to the checkpoint - only it can arm before that statement runs. Use Wait or WaitTimeout when
// the host is already running into the checkpoint: frozen at a breakpoint, or driven from another goroutine.
//
// Rendezvous and freeze compose to drive a concurrent operation into a precise window with no timing hammer:
// Break, start the host operation, wait for it to freeze there, run the racing operation, Resume. A waiter armed
// for a checkpoint already holding the host fires immediately, so this is race-free whichever happens first.
// Break is for genuinely HOLDING the host, though - a rendezvous alone is already race-free, and freezing
// perturbs the timing under test.

// breakpoint is one armed breakpoint: release is closed by Resume to let the frozen host proceed; hit is closed
// by the host when it reaches the checkpoint and is about to block, so a rendezvous armed after the host froze
// still observes the arrival instead of hanging.
type breakpoint struct {
	release chan struct{}
	hit     chan struct{}
	// hitClosed guards against a double close of hit when several goroutines reach the same armed checkpoint
	// concurrently. Read and written only under Seamster.mu.
	hitClosed bool
}

// Checkpoint is the host-side consult at an instrumented site, and the only one a host calls. Free in production:
// it returns on the enabled bool read, before any lock or key build. When enabled it counts the visit, wakes any
// waiters, and - if a breakpoint is armed - blocks until [Seamster.Resume], or until ctx is done, so a stuck test
// or a shutdown can never wedge the host goroutine forever.
func (s *Seamster) Checkpoint(ctx context.Context, checkpointName string, scope ...string) {
	if !s.enabled {
		return
	}
	key := scopedKey(checkpointName, scope)
	s.mu.Lock()
	if s.visits == nil {
		s.visits = make(map[string]int)
	}
	s.visits[key]++
	for _, ch := range s.waitFors[key] {
		close(ch)
	}
	delete(s.waitFors, key)
	bp := s.breakpoints[key]
	if bp != nil && !bp.hitClosed {
		// First arrival marks the breakpoint hit; later arrivals (concurrent ones racing this same lock hold, or
		// sequential ones before Resume disarms the entry) just join the freeze without re-closing hit.
		bp.hitClosed = true
		close(bp.hit)
	}
	s.mu.Unlock()

	if bp != nil {
		select {
		case <-bp.release:
		case <-ctx.Done():
		}
	}
}

// Waiter registers a waiter for the named checkpoint and returns a channel closed when the host reaches it. It
// does not block: arming and receiving are separate steps, which is the whole point. Arm before the operation
// that triggers the checkpoint, receive after it:
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
func (s *Seamster) Waiter(checkpointName string, scope ...string) <-chan struct{} {
	ch := make(chan struct{})
	if !s.enabled {
		close(ch)
		return ch
	}
	key := scopedKey(checkpointName, scope)
	s.mu.Lock()
	defer s.mu.Unlock()
	if bp := s.breakpoints[key]; bp != nil {
		select {
		case <-bp.hit:
			close(ch) // host already frozen at this breakpoint
			return ch
		default:
		}
	}
	if s.waitFors == nil {
		s.waitFors = make(map[string][]chan struct{})
	}
	s.waitFors[key] = append(s.waitFors[key], ch)
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
func (s *Seamster) Wait(ctx context.Context, checkpointName string, scope ...string) bool {
	reached := s.Waiter(checkpointName, scope...)
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
func (s *Seamster) WaitTimeout(ctx context.Context, timeout time.Duration, checkpointName string, scope ...string) bool {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return s.Wait(ctx, checkpointName, scope...)
}

// Break arms a breakpoint so the host blocks the next time it reaches the named checkpoint, until
// [Seamster.Resume] releases it. It holds every goroutine that reaches the checkpoint while armed, not only the
// first, so a fan-out can be gathered at one point and released together by a single Resume; a rendezvous
// returns as soon as the first of them arrives.
func (s *Seamster) Break(checkpointName string, scope ...string) {
	if !s.enabled {
		return
	}
	key := scopedKey(checkpointName, scope)
	s.mu.Lock()
	if s.breakpoints == nil {
		s.breakpoints = make(map[string]*breakpoint)
	}
	s.breakpoints[key] = &breakpoint{release: make(chan struct{}), hit: make(chan struct{})}
	s.mu.Unlock()
}

// Visits reports how many times the host has passed the named checkpoint, counting a call that then blocked on a
// breakpoint (reaching the checkpoint is the visit; blocking comes after). It never blocks, consumes nothing, and
// can be read repeatedly. The count is monotonic for the Seamster's lifetime, so capture a baseline first to
// assert on visits accrued within a window. Zero for a checkpoint never reached, and always zero on a disabled
// Seamster.
func (s *Seamster) Visits(checkpointName string, scope ...string) int {
	if !s.enabled {
		return 0
	}
	key := scopedKey(checkpointName, scope)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.visits[key]
}

// Resume releases the host frozen at the named breakpoint and disarms it. A no-op if none is armed.
func (s *Seamster) Resume(checkpointName string, scope ...string) {
	if !s.enabled {
		return
	}
	key := scopedKey(checkpointName, scope)
	s.mu.Lock()
	bp := s.breakpoints[key]
	delete(s.breakpoints, key)
	s.mu.Unlock()
	if bp != nil {
		close(bp.release)
	}
}
