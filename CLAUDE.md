# seamster — design notes

> Load when: editing anything in this package.

## Documentation conventions

Four places carry prose, and each has exactly one reader. Know which before writing a line.

**Written for the agent USING the module:**

- **`README.md`** — orientation: what the seams are, what each one is for, and the shapes a caller writes. The
  only doc read before any source is opened.
- **Godoc** — the package comment and every exported symbol (type, method, constant, variable): what it does,
  what to pass, what comes back, what the caller must hold up. The how-to-USE-it altitude, never the
  how-it-works one.

**Written for the agent DEVELOPING the module:**

- **`CLAUDE.md`** (this file) — the design rationale: why a mechanism is shaped this way, which alternatives were
  weighed and lost, which invariants are load-bearing, which test pins each one. **Most developer prose belongs
  here.**
- **Private comments** — unexported identifiers, and comments inside function bodies. They live in the code
  **because they are location-sensitive**: they say something a maintainer needs *at that line* and nowhere else.

Rules that follow from the split:

- **Godoc carries no rationale**, no rejected alternatives, and no examples borrowed from a consuming project — a
  user cannot see any of that and should not have to read past it. The *instruction* is godoc's ("wrap this call
  in an `Enabled()` check"); the *argument* for why the API forces it is this file's.
- **Developer prose never restates the code.** If a sentence is recoverable by reading the three lines beneath
  it, delete it.
- **Embedded prose must be location-sensitive.** A comment that would read just as well here belongs here —
  proximity to the code is the only thing it has that this file does not, so it had better be using it. The
  clearest failure is a **free-floating block comment at the top of a file**: Go attaches it to no declaration,
  so no user ever sees it in godoc, and it names no line, so it is not location-sensitive either. Its
  user-facing half belongs in godoc or `README.md`; its rationale half belongs here.
- **One home per fact, per audience.** `README.md` orients and godoc specifies, so the two overlap by design.
  Within the developer half there is no such licence: a trap may be named twice — briefly at the line where
  someone would walk into it, in full here — and nothing else may be. Two copies of an argument means one of
  them is already going stale.

## The package owns mechanism, never assertion

seamster imports only the standard library, and that is load-bearing rather than incidental: a host embeds a
`Seamster` unconditionally, on production hot paths, so the package must never drag in `testing`. The direct
consequence is that **no method can fail a test** — a bounded wait reports `bool` and the caller asserts. Do not
"improve" this by taking a `*testing.T` or a `TestingT` interface.

`Variable` is the entry point most likely to erode this, because it is the only one that carries data out of the
host. The line it must not cross: record a value the host **already computes** for its own reasons, never one
derived for the seam's benefit. A value computed solely so a test can assert on it has moved the assertion into
production source, which is the thing the whole package is arranged to prevent.

## Why a rendezvous both returns a channel and blocks

A rendezvous is two steps — *arm*, then *receive* — and fusing them is a correctness problem, not a style one. A
wait that arms and blocks in one call can only arm **after** the operation it wants to observe has started, so any
arrival in that gap is lost forever. Hence `Waiter` returns the channel: the caller arms before the trigger and
receives after, leaving no gap.

The two ways of papering over a fused wait were both tried in a consuming test suite and both cost something
real. A goroutine calling a blocking wait merely **moves** the race (it may not have registered before the host
arrives) and leaks on timeout. A breakpoint closes the race honestly but **freezes the host**, perturbing the
timing under test and obliging a `Resume`. Because `Waiter` is race-free on its own, `Break` is now only for
genuinely *holding* the host while a racing operation runs — if you find `Break` armed purely to make an
observation reliable, that is a leftover worth deleting.

`Wait` and `WaitTimeout` still exist because the fused shape is correct and simpler whenever the host is
*already* running into the checkpoint (frozen at a breakpoint, or driven from another goroutine). They take
`ctx` — matching `Checkpoint`, so an abort is spelled one way throughout — and `WaitTimeout` takes its timeout
**before** the name purely so `scope ...string` stays trailing and variadic like everywhere else.

## `Wait` prefers the arrival over ctx, deliberately

`Wait` does a non-blocking check of the arrival channel before its two-case select. A lone two-case select picks
**at random** when both are ready, so a checkpoint the host genuinely reached would report as a miss whenever ctx
expires in the same instant — an intermittent false failure, in a package whose whole purpose is removing those.
`TestSeamster_WaitAbortsOnContext` covers it; deleting the pre-check makes that test fail, so it is not decoration.

## Scoping is one mechanism across all three seams

Faults, checkpoints and variables scope identically, through `scopedKey`. That sharing is the point: a consult and
the arming that targets it must build the same key, and hand-rolled copies of the join (the state before it was
extracted) are exactly how they drift apart. Build the key **after** the `enabled` gate at every site, so a
disabled Seamster allocates nothing.

A scoped fire and an unscoped one are **different keys** — a scoped `Checkpoint` does not wake an unscoped waiter,
and each key counts its own `Visits`. A host wanting both granularities fires both explicitly. This mirrors fault
semantics exactly, which is why it is left as-is rather than made to cascade: cascading would make `Visits`
ambiguous (does an unscoped count include scoped arrivals?) and split the two seams' behavior.

## A variable overwrites, and holds no history

`Variable` replaces the value under its key; there is no accumulating form, and adding one is the obvious wrong
turn. Overwrite bounds the cost by the number of **distinct keys**, which the test controls through the scope it
chooses. Appending would bound it by the number of **calls**, which the test does not control at all — a
recording site sitting in a host's execution loop, exercised by a soak test, accumulates a slot per pass at a
single key. That asymmetry is the whole argument: the growth an author can reason about versus the growth they
cannot.

Nothing is lost that composition does not return. The value a history would be wanted for is an intermediate one
that a later pass overwrites, and the way to read it is to stop the host at the moment it holds: plant a
`Checkpoint` beside the recording site, `Break` there, `Capture` while the host is frozen, `Resume`.
`TestSeamster_VariableCapturedUnderABreakpoint` pins exactly that composition, so it is checkable rather than
merely asserted here.

## The variable seam owns no rendezvous

`Variable` deliberately does **not** fire a checkpoint, and `Capture` never blocks. Fusing them would look
economical — one namespace, `Visits` and `Waiter` working on variables for free — and it costs more than it
saves: a recording would trip a breakpoint armed on the same name, `Visits` would count recordings alongside
genuine checkpoint arrivals, and `Variable` would need a `ctx` at every call site to honor a freeze it has no
business honoring.

The reason it can stay unfused is that a variable is **state, not an edge**. A checkpoint arrival is gone the
instant it happens, which is why that seam needs the arm-before-trigger discipline; a recorded value is still
sitting there when a late `Capture` runs. Most captures therefore need no ordering at all — the host has
finished the work under test by the time the test looks. The ones that do need ordering compose with the
checkpoint seam, which already solves it correctly.

## `Capture` returns `any`, not a generic

The typed alternative — a package-level `Capture[T any](s *Seamster, ...) (T, bool)`, since Go has no generic
methods — was weighed and dropped. It would be the only free function in a package where every entry point is a
method on `Seamster`, and it buys less than it appears to: with `(any, bool)`, a **bare** type assertion
(`v.(int)`) panics on a host-side type drift, and a panic in the test goroutine is a loud, correct failure. Only
`v, _ := x.(int)` is silent, and a discarded type assertion is visible in review. The godoc spells out the bare
form for this reason.

## Two constraints that live at the recording site

Both are the host's to honor; neither can be enforced from inside the package.

**Wrap every `Variable` call in an `Enabled()` check.** Every other consult is free in production because its
arguments are constants — the `enabled` read short-circuits before anything happens. `Variable` takes a value,
and the conversion to the `any` parameter is performed by the **caller**, before the gate can be reached. An
unguarded call therefore costs a production allocation on every pass, on whatever hot path it was planted in.

**Record a value, never a reference to live state.** `mu` guards the map, not what a recorded pointer, slice, or
map points at. The host goes on mutating that while a test reads it from another goroutine — a data race the
guard cannot see and the race detector will. Record a scalar, or snapshot before recording.

## Breakpoints hold every arrival

An armed breakpoint freezes *every* goroutine reaching the checkpoint, not just the first, so a fan-out can be
gathered at one point and released by a single `Resume`. `hitClosed` (under `mu`) exists because concurrent
arrivals must not double-close `hit`. A rendezvous returns as soon as the first arrival lands.
