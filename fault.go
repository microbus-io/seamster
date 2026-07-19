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

// A fault makes a consult site MISBEHAVE. The host consults it where it affects with [Seamster.IsFault]; a test
// arms it with [Seamster.Inject]. Faults are usually SCOPED: the consult site appends an identifier so a test
// targets one entity (one task, one URL, one message) rather than "the next thing that happens". Both sides take
// the scope as trailing args and join it the same way, so the scope is spelled one way at both ends:
// s.IsFault("executeTask", taskName) is armed by s.Inject("executeTask", "Charge"). A few faults are
// process-wide and take no scope.

// IsFault reports whether the named fault is armed, consuming one fire. It is the only consult entry point and is
// free in production: it returns on the enabled bool read before any lock or key build. Optional scope args target
// the fault (a task name, a URL, ...) and are joined into the key, but only once the enabled gate has
// passed, so a scoped consult on a disabled Seamster allocates no throwaway key. To keep a fault armed for the rest
// of a test (until [Seamster.Withdraw]), inject a large count, e.g. s.InjectN(math.MaxInt, name).
func (s *Seamster) IsFault(faultName string, scope ...string) bool {
	if !s.enabled {
		return false
	}
	key := scopedKey(faultName, scope)
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.faults[key]
	if n <= 0 {
		return false
	}
	if n == 1 {
		delete(s.faults, key)
	} else {
		s.faults[key] = n - 1
	}
	return true
}

// Inject arms the named fault to fire once (additive: calling twice fires twice). Optional scope args target it,
// joined the same way [Seamster.IsFault] joins its scope.
func (s *Seamster) Inject(faultName string, scope ...string) {
	s.InjectN(1, faultName, scope...)
}

// InjectN arms the named fault to fire n more times, added to any current count. Optional scope args target it,
// joined the same way [Seamster.IsFault] joins its scope. A non-positive n is a no-op.
func (s *Seamster) InjectN(n int, faultName string, scope ...string) {
	if !s.enabled || n <= 0 {
		return
	}
	key := scopedKey(faultName, scope)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.faults == nil {
		s.faults = make(map[string]int)
	}
	s.faults[key] += n
}

// Withdraw disarms the named fault regardless of its remaining count. Optional scope args target it, joined the
// same way [Seamster.IsFault] joins its scope.
func (s *Seamster) Withdraw(faultName string, scope ...string) {
	if !s.enabled {
		return
	}
	key := scopedKey(faultName, scope)
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.faults, key)
}
