# seamster — design notes

> Load when: editing anything in this package. Godoc here is written for **library users** (what the API does,
> how to call it); this file holds the **why** — the decisions a maintainer needs and a user does not.

Keep both audiences straight. Godoc must not carry build rationale, rejected alternatives, or examples borrowed
from any consuming project — a user cannot see those and should not have to read past them. This file may.

## The package owns mechanism, never assertion

seamster imports only the standard library, and that is load-bearing rather than incidental: a host embeds a
`Seamster` unconditionally, on production hot paths, so the package must never drag in `testing`. The direct
consequence is that **no method can fail a test** — a bounded wait reports `bool` and the caller asserts. Do not
"improve" this by taking a `*testing.T` or a `TestingT` interface.

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

## Scoping is one mechanism across both seams

Faults and checkpoints scope identically, through `scopedKey`. That sharing is the point: a consult and the arming
that targets it must build the same key, and three hand-rolled copies of the join (the state before it was
extracted) is exactly how they drift apart. Build the key **after** the `enabled` gate at every site, so a
disabled Seamster allocates nothing.

A scoped fire and an unscoped one are **different keys** — a scoped `Checkpoint` does not wake an unscoped waiter,
and each key counts its own `Visits`. A host wanting both granularities fires both explicitly. This mirrors fault
semantics exactly, which is why it is left as-is rather than made to cascade: cascading would make `Visits`
ambiguous (does an unscoped count include scoped arrivals?) and split the two seams' behavior.

## Breakpoints hold every arrival

An armed breakpoint freezes *every* goroutine reaching the checkpoint, not just the first, so a fan-out can be
gathered at one point and released by a single `Resume`. `hitClosed` (under `mu`) exists because concurrent
arrivals must not double-close `hit`. A rendezvous returns as soon as the first arrival lands.
