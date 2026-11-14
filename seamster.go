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
Package seamster provides three test-only instrumentation seams that a white-box test uses to exercise a host's
recovery and race paths deterministically, without forging state or hammering on timing. All are inert unless
the host constructs the [Seamster] enabled:

  - Fault injection makes a site MISBEHAVE: a test arms a named fault, the host consults it at the exact point it
    affects, and simulates an error, drop, or stale write that is otherwise hard to trigger on demand.
  - Execution checkpoints make a site OBSERVABLE and PAUSABLE: a test rendezvouses with the host's progress
    ([Seamster.Waiter] / [Seamster.Wait] / [Seamster.WaitTimeout]), freezes the host at an exact point
    ([Seamster.Break] / [Seamster.Resume]), or counts arrivals ([Seamster.Visits]).
  - Recorded variables make a host-side VALUE readable: the host records what it has computed
    ([Seamster.Variable]) and a test reads the last recording ([Seamster.Capture]) - for a value a test cannot
    reach otherwise, such as one the host computes and consumes without ever storing it.

A "seam" is a place where a test can alter or observe behavior without editing the code in place; a Seamster is the
object that works a host's seams. The host embeds one Seamster, plants consult sites ([Seamster.IsFault] /
[Seamster.Checkpoint] / [Seamster.Variable]) at the points they affect, and names the valid fault/checkpoint set
next to those sites so a test cannot arm a fault or checkpoint no site consumes.

# Names

Every seam is addressed by a plain string name. A consult and the arming aimed at it meet on that string and
nothing else. A name identifying one entity - a particular task, URL, or shard, so a test targets it rather than
the next thing that happens - is best built by a function defined once and called at both ends, so the two
cannot drift apart:

	func taskFault(task string) string { return "task:" + task }

Production inertness is by construction: every consult short-circuits on the enabled bool the host passes to [New]
(typically testing.Testing()), so a disabled Seamster pays a single lock-free bool read per site and no seam can
ever fire. What the host must watch is the step BEFORE the gate, which is the caller's own work: a constant name
costs nothing, but a name assembled at the call site is assembled on every pass whether or not the seams are
live, as is the value handed to [Seamster.Variable]. Wrap those sites in an [Seamster.Enabled] check.

The package imports only the standard library, so a host may embed it on a production hot path without dragging
in test-only dependencies.
*/
package seamster

import "sync"

// Seamster works one host's instrumentation seams. It is safe for concurrent use. The zero value is a disabled
// Seamster; construct one with [New]. Embed one Seamster per host instance rather than
// sharing a package global, so distinct hosts arm and consult independently.
type Seamster struct {
	// enabled is set once at construction and never mutated, which is what lets every consult read it without
	// the lock.
	enabled bool

	// mu guards every map below TOGETHER. One lock is what keeps operations spanning two of them consistent -
	// Checkpoint bumps a visit count, wakes waiters, and counts an arrival held at a breakpoint under a single
	// hold. Splitting it per map would tear that.
	mu sync.Mutex

	// Every map below is keyed by the seam's name. faults holds an armed fault's REMAINING fire count, decremented
	// by each consult; an exhausted key is deleted rather than left at zero, so presence means armed.
	faults map[string]int

	// waitFors holds tests blocked in Wait(name), woken by the host's next Checkpoint(name).
	waitFors map[string][]chan struct{}

	// breakpoints holds armed Break(name) freezes, released by Resume(name).
	breakpoints map[string]*breakpoint

	// visits is monotonic for the Seamster's lifetime - never reset, which is why a test asserting on a window
	// captures a baseline first.
	visits map[string]int

	// variables holds only the LAST value recorded under each key; a second recording drops the first.
	variables map[string]any
}

// New returns a Seamster. When enabled is false every consult is inert, so a host passes its own under-test
// signal - most commonly testing.Testing() - and embeds the result unconditionally.
func New(enabled bool) *Seamster {
	return &Seamster{enabled: enabled}
}

// Enabled reports whether the seams are live, so a host can skip work that would only feed a consult and would
// otherwise be paid for in production. Anything the caller builds before the call is such work: an assembled
// name (a concatenation, or a helper that returns one), a state read that only the name or the fault decision
// needs, and the value handed to [Seamster.Variable]. A constant name needs no guard.
//
//	if h.seams.Enabled() {
//		h.seams.Checkpoint(ctx, "shard:"+h.shard)
//	}
func (s *Seamster) Enabled() bool {
	return s.enabled
}
