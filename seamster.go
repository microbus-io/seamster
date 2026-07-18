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

/*
Package seamster provides two test-only instrumentation seams that a white-box test uses to exercise a host's
recovery and race paths deterministically, without forging state or hammering on timing. Both are inert unless
the host constructs the [Seamster] enabled:

  - Fault injection makes a site MISBEHAVE: a test arms a named fault, the host consults it at the exact point it
    affects, and simulates an error, drop, or stale write that is otherwise hard to trigger on demand.
  - Execution checkpoints make a site OBSERVABLE and PAUSABLE: a test rendezvouses with the host's progress
    ([Seamster.Wait]) or freezes the host at an exact point ([Seamster.Break] / [Seamster.Resume]).

A "seam" is a place where a test can alter or observe behavior without editing the code in place; a Seamster is the
object that works a host's seams. The host embeds one Seamster, plants consult sites ([Seamster.IsFault] /
[Seamster.Checkpoint]) at the points they affect, and names the valid fault/checkpoint set next to those sites so a
test cannot arm a fault no site consumes.

Production inertness is by construction: every consult short-circuits on the enabled bool the host passes to [New]
(typically testing.Testing()), so a disabled Seamster pays a single lock-free bool read per site and neither seam
can ever fire. The package imports only the standard library, so a host may embed it on a production hot path
without dragging in test-only dependencies.
*/
package seamster

import "sync"

// Seamster works one host's instrumentation seams. It is safe for concurrent use. The zero value is a disabled
// Seamster; construct one with [New]. A single Seamster is meant to be embedded per host instance (one per engine,
// one per connector, ...), not shared as a package global, so distinct hosts arm and consult independently.
type Seamster struct {
	// enabled gates every consult. When false, IsFault and Checkpoint return before touching the lock or the
	// maps, so a production host pays one bool read per site. Set once at construction and never mutated.
	enabled bool

	// mu guards faults, waitFors, breakpoints, and visits together. One lock keeps the arm/consult/wake
	// operations across both seams mutually consistent - e.g. Checkpoint bumping the visit count, waking
	// waiters, and marking a breakpoint hit under a single hold.
	mu sync.Mutex

	// faults maps an armed fault key to its remaining fire count. A key is the fault name plus any scope,
	// joined with ":" - IsFault, Inject, and Withdraw all build it the same way.
	faults map[string]int

	// waitFors holds tests blocked in Wait(name), woken by the host's next Checkpoint(name).
	waitFors map[string][]chan struct{}

	// breakpoints holds armed Break(name) freezes, released by Resume(name).
	breakpoints map[string]*breakpoint

	// visits counts how many times the host has passed each named checkpoint, bumped by Checkpoint and read
	// by Visits. Monotonic for the Seamster's lifetime - never reset.
	visits map[string]int
}

// New returns a Seamster. When enabled is false every consult is inert, so a host passes its own under-test
// signal - most commonly testing.Testing() - and embeds the result unconditionally.
func New(enabled bool) *Seamster {
	return &Seamster{enabled: enabled}
}

// Enabled reports whether the seams are live. A host uses it to skip building a scoped consult (a state read, a
// key concatenation) that would only feed IsFault, so the setup work is also elided in production.
func (s *Seamster) Enabled() bool {
	return s.enabled
}
