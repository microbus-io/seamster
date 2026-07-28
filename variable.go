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

// Where a fault makes a site MISBEHAVE and a checkpoint makes it OBSERVABLE and PAUSABLE, a variable makes a
// host-side VALUE readable. The host records what it has computed with [Seamster.Variable]; a test reads the
// last recording with [Seamster.Capture]. It earns its place for a value a test cannot reach any other way -
// one the host computes and consumes without storing it, one written inside a transaction that then rolls
// back, one overwritten before the test can look. A value the host already exposes - a return, a field, a
// persisted row - is better read there, where the test is asserting on the real thing rather than on whatever
// the host chose to publish.
//
// Recording OVERWRITES: a name holds the LAST value recorded under it, never a history. A site on a hot path
// may therefore record on every pass at a cost bounded by the number of distinct keys, not by how often it
// runs. To capture an intermediate value that a later pass would replace, freeze the host at a checkpoint
// planted beside the recording site and capture while it is held.
//
// There is no rendezvous of its own, deliberately. A capture reads whatever has been recorded by the moment it
// runs, and most reads need nothing more - the host has finished the work under test by the time the test
// looks. Where a read must be ORDERED against the host, compose with the checkpoint seam rather than expecting
// this one to synchronize.
//
// Variables scope like faults and checkpoints, through the same key join: a scoped recording and an unscoped
// one are different variables.

// Variable records value as the current value of the named host-side variable, replacing whatever was recorded
// under the same name and scope before. A test reads it back with [Seamster.Capture]. Recording is inert on a
// disabled Seamster.
//
// Two rules govern the call sites:
//
// Wrap every call in an [Seamster.Enabled] check. Unlike the other seams, this one takes a value: converting it
// to the any parameter happens at the CALL SITE, before the enabled gate can short-circuit, so an unguarded
// call costs a production allocation at every pass.
//
//	if h.seams.Enabled() {
//		h.seams.Variable("chosenBand", band)
//	}
//
// Record a value, never a reference to live state. The recording is guarded, but a recorded slice, map, or
// pointer leaves the host free to keep mutating what it points at while a test reads it from another
// goroutine - a data race the guard cannot see and the race detector will. Record a scalar, or snapshot first.
func (s *Seamster) Variable(variableName string, value any, scope ...string) {
	if !s.enabled {
		return
	}
	key := scopedKey(variableName, scope)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.variables == nil {
		s.variables = make(map[string]any)
	}
	s.variables[key] = value
}

// Capture returns the last value the host recorded under the named variable, and whether anything was ever
// recorded there. It never blocks, consumes nothing, and can be read repeatedly. The ok result distinguishes a
// variable the host has not reached from one it recorded a zero value into. Always false on a disabled
// Seamster.
//
// The caller asserts the type, and should do so WITHOUT the comma-ok:
//
//	v, ok := h.seams.Capture("chosenBand")
//	if !ok {
//		t.Fatal("host never recorded chosenBand")
//	}
//	band := v.(int)
//
// A bare assertion panics if the host's type drifts, which fails the test loudly. Discarding the assertion's
// ok instead yields a zero value that the test goes on to assert against, and it passes or fails for a reason
// that has nothing to do with what it was written to check.
func (s *Seamster) Capture(variableName string, scope ...string) (any, bool) {
	if !s.enabled {
		return nil, false
	}
	key := scopedKey(variableName, scope)
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.variables[key]
	return v, ok
}
