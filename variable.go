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

// Variable records value as the current value of the named host-side variable, making it readable by a test
// through [Seamster.Capture]. It is for a value a test cannot reach any other way - one the host computes and
// consumes without storing it, one written inside a transaction that then rolls back, one overwritten before
// the test can look. A value the host already exposes, as a return, a field, or a persisted row, is better read
// there. Recording is inert on a disabled Seamster.
//
// Recording OVERWRITES: a name holds the LAST value recorded under it, never a history, so a site on a hot path
// may record on every pass at a cost bounded by the number of distinct names rather than by how often it runs.
//
// Two rules govern the call sites.
//
// Wrap every call in an [Seamster.Enabled] check. This consult always takes a value, and converting it to the
// any parameter happens at the CALL SITE, before the enabled gate can short-circuit - so an unguarded call
// costs a production allocation on every pass, whatever the name costs.
//
//	if h.seams.Enabled() {
//		h.seams.Variable("chosenBand", band)
//	}
//
// Record a value, never a reference to live state. A recorded pointer, slice, or map leaves the host free to go
// on mutating what it points at while a test reads it from another goroutine - a data race no guard here can
// cover. Record a scalar, or snapshot first.
func (s *Seamster) Variable(variableName string, value any) {
	if !s.enabled {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.variables == nil {
		s.variables = make(map[string]any)
	}
	s.variables[variableName] = value
}

// Capture returns the last value the host recorded under the named variable, and whether anything was ever
// recorded there. It never blocks, consumes nothing, and can be read repeatedly. The ok result distinguishes a
// variable the host has not reached from one it recorded a zero value into. Always false on a disabled
// Seamster.
//
// It has no rendezvous of its own: it reads whatever has been recorded by the moment it runs, which is all most
// reads need, since the host has finished the work under test by the time the test looks. Where a read must be
// ORDERED against the host - to catch an intermediate value a later pass would overwrite - plant a
// [Seamster.Checkpoint] beside the recording site and drive it with the checkpoint seam: [Seamster.Break]
// there, capture while the host is held, then [Seamster.Resume].
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
func (s *Seamster) Capture(variableName string) (any, bool) {
	if !s.enabled {
		return nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.variables[variableName]
	return v, ok
}
