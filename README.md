# Seamster

`Seamster` works the test-only instrumentation *seams* of a host package: named points planted in the host's code
that a white-box test hooks into to drive failure and timing paths deterministically. A "seam" is a place where a
test can alter or observe behavior without editing the code in place; a `Seamster` is the object that works them.

It has two halves:

- **Fault injection** makes a site *misbehave*. A test arms a named fault; the host consults it at the exact point
  it affects and simulates an error, drop, or stale write that is otherwise hard to trigger on demand.
- **Execution checkpoints** make a site *observable and pausable*. A test rendezvouses with the host's progress,
  freezes the host at an exact point (so a concurrent operation can be driven into a precise window with no timing
  hammer), or simply counts how many times the host has passed the point.

Both are **inert in production by construction**: every consult short-circuits on the `enabled` bool the host
passes to `New` (typically `testing.Testing()`), so a disabled `Seamster` pays a single lock-free bool read per
site and neither seam can ever fire. The package imports only the standard library, so a host may embed it on a
production hot path without pulling in test-only dependencies.

## Usage

The host embeds one `Seamster` per instance, plants consult sites where they affect, and names the valid
fault/checkpoint set next to those sites.

```go
type Host struct {
    // ...
    seams *seamster.Seamster
}

func NewHost() *Host {
    return &Host{seams: seamster.New(testing.Testing())}
}
```

Production consult sites (free when disabled):

```go
// Make this write misbehave when a test arms the fault, scoped to one task.
if h.seams.IsFault("transitionCommit", taskName) {
    return errors.New("injected fault")
}

// Mark this point observable/pausable.
h.seams.Checkpoint(ctx, "beforeTransitionTx")
```

Test side:

```go
// Fault injection: arm a scoped fault to fire once (scope args match IsFault's).
h.seams.Inject("transitionCommit", "Charge")

// Rendezvous: block the test until the host reaches a checkpoint.
h.seams.Wait("beforeTransitionTx")

// Freeze: drive a concurrent op into a precise window.
h.seams.Break("beforeTransitionTx")
go h.Process(ctx)          // runs into the breakpoint and blocks
h.seams.Wait("beforeTransitionTx")  // returns once frozen
// ... run the racing operation while the host is held ...
h.seams.Resume("beforeTransitionTx")

// Count: assert how many times the host passed a checkpoint.
if h.seams.Visits("beforeTransitionTx") != 3 {
    t.Fatalf("expected 3 attempts, got %d", h.seams.Visits("beforeTransitionTx"))
}
```

## API

| Method | Side | Purpose |
|---|---|---|
| `New(enabled bool)` | host | Construct a Seamster; inert when `enabled` is false. |
| `Enabled() bool` | host | Skip building a scoped consult that would only feed `IsFault`. |
| `IsFault(name, scope...) bool` | host | Consult a fault, consuming one fire. |
| `Inject(name, scope...)` | test | Arm a fault once (additive); scope args match `IsFault`. |
| `InjectN(n, name, scope...)` | test | Arm a fault n times (additive); scope args match `IsFault`. |
| `Withdraw(name, scope...)` | test | Disarm a fault. |
| `Checkpoint(ctx, name)` | host | Pass through a checkpoint; counts the visit, wakes waiters, honors a breakpoint. |
| `Wait(name)` | test | Block until the host next reaches the checkpoint. |
| `Break(name)` | test | Freeze the host at the checkpoint. |
| `Resume(name)` | test | Unfreeze the host at the checkpoint. |
| `Visits(name) int` | test | Count how many times the host has passed the checkpoint (monotonic; never blocks). |

Scoping: a fault is usually scoped so a test targets one entity rather than "the next thing that happens." The
consult passes the scope to `IsFault`; the test arms the fault with the same scope args (`Inject`/`InjectN`/
`Withdraw` all join scope the same way), so it is spelled one way at both ends. Unscoped (process-wide) faults
take no scope.
