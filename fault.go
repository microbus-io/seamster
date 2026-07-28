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

// IsFault reports whether the named fault is armed, consuming one fire, and makes the site around it MISBEHAVE -
// simulating an error, drop, or stale write that is otherwise hard to trigger on demand. It is the only consult
// entry point and is free in production: it returns on the enabled bool read before any lock.
//
// A fault name usually carries the entity it targets, so a test aims at one task, URL, or shard rather than the
// next thing that happens. The consult and the arming must produce the same string, which is why a composite
// name is best built by a shared function: a site consulting taskFault(taskName) is armed by [Seamster.Inject]
// with taskFault("Charge").
//
// To keep a fault armed for the rest of a test (until [Seamster.Withdraw]), inject a large count, e.g.
// s.InjectN(name, math.MaxInt).
func (s *Seamster) IsFault(faultName string) bool {
	if !s.enabled {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.faults[faultName]
	if n <= 0 {
		return false
	}
	if n == 1 {
		delete(s.faults, faultName)
	} else {
		s.faults[faultName] = n - 1
	}
	return true
}

// Inject arms the named fault to fire once (additive: calling twice fires twice).
func (s *Seamster) Inject(faultName string) {
	s.InjectN(faultName, 1)
}

// InjectN arms the named fault to fire n more times, added to any current count. A non-positive n is a no-op.
func (s *Seamster) InjectN(faultName string, n int) {
	if !s.enabled || n <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.faults == nil {
		s.faults = make(map[string]int)
	}
	s.faults[faultName] += n
}

// Withdraw disarms the named fault regardless of its remaining count.
func (s *Seamster) Withdraw(faultName string) {
	if !s.enabled {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.faults, faultName)
}
