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

import "context"

// Where a fault makes a site misbehave, a checkpoint makes a site OBSERVABLE and PAUSABLE. The host passes through
// a named checkpoint by calling [Seamster.Checkpoint]; a test drives it two composable ways:
//
//   - Wait(name): block the TEST until the host reaches the checkpoint. A one-way rendezvous - "do the next
//     thing only once the host has gotten to X". It blocks until the host NEXT reaches name, so a checkpoint the
//     host already passed does not re-fire; arm a breakpoint when you need to catch a point the host may already
//     have reached.
//   - Break(name) / Resume(name): freeze the HOST at the checkpoint until the test clears it. A
//     debugger breakpoint for the host - arm it, let the host run into it and block, do whatever the test needs
//     while the host is frozen, then clear to release it.
//
// A third, passive observation needs no arming: Visits(name) reports how many times the host has passed the
// checkpoint - a plain counter for assertions ("the retry ran exactly 3 times"), never blocking.
//
// The two compose to drive a concurrent operation into a precise window deterministically, with no timing hammer:
// Break(name); start the host op in a goroutine; Wait(name) (returns once the host is frozen at the
// breakpoint); run the racing op while the host is held; Resume(name) to release. Wait returns
// immediately if a breakpoint is already holding the host at name, so the compose case is race-free regardless of
// whether Wait or the host's arrival happens first.

// breakpoint is one armed breakpoint: release is closed by Resume to let the frozen host proceed; hit is
// closed by the host (under the lock) when it reaches the checkpoint and is about to block, so a Wait that
// arrives after the host froze still observes the arrival instead of hanging.
type breakpoint struct {
	release chan struct{}
	hit     chan struct{}
}

// Checkpoint is the host-side consult at an instrumented site. Free in production (returns on the enabled bool
// read). When enabled it wakes any Wait(name) waiters and, if a breakpoint is armed for name, blocks until
// Resume(name) - or until ctx is done, so a stuck test or a shutdown can never wedge the host goroutine
// forever. Waking waiters and marking the breakpoint hit happen under one lock hold, so a Wait racing the
// host's arrival is woken or sees hit - never lost between the two.
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
		close(bp.hit) // one-shot: Resume deletes the entry, so a re-arrival reaching here finds bp==nil
	}
	s.mu.Unlock()

	if bp != nil {
		select {
		case <-bp.release:
		case <-ctx.Done():
		}
	}
}

// Wait blocks until the host reaches the named checkpoint. If a breakpoint is already holding the host at name,
// it returns immediately; otherwise it registers a waiter woken by the host's next arrival.
func (s *Seamster) Wait(checkpointName string) {
	if !s.enabled {
		return
	}
	s.mu.Lock()
	if bp := s.breakpoints[checkpointName]; bp != nil {
		select {
		case <-bp.hit:
			s.mu.Unlock()
			return // host already frozen at this breakpoint
		default:
		}
	}
	ch := make(chan struct{})
	if s.waitFors == nil {
		s.waitFors = make(map[string][]chan struct{})
	}
	s.waitFors[checkpointName] = append(s.waitFors[checkpointName], ch)
	s.mu.Unlock()
	<-ch
}

// Break arms a breakpoint so the host blocks the next time it reaches the named checkpoint, until
// Resume releases it.
func (s *Seamster) Break(checkpointName string) {
	if !s.enabled {
		return
	}
	s.mu.Lock()
	if s.breakpoints == nil {
		s.breakpoints = make(map[string]*breakpoint)
	}
	s.breakpoints[checkpointName] = &breakpoint{release: make(chan struct{}), hit: make(chan struct{})}
	s.mu.Unlock()
}

// Visits reports how many times the host has passed the named checkpoint - each Checkpoint(name) call counts
// one, including a call that then blocked on a breakpoint (reaching the checkpoint is the visit; blocking comes
// after). It is a passive counter for assertions: it never blocks, consumes nothing, and can be read repeatedly.
// The count is monotonic for the Seamster's lifetime (never reset), so capture a baseline first if a test asserts
// on visits accrued only within a window. Zero for a checkpoint never reached, and always zero on a disabled
// Seamster (Checkpoint counts nothing when inert).
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
