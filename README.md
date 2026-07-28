# Seamster

`Seamster` works the test-only instrumentation *seams* of a host package: named points planted in the host's code
that a white-box test hooks into to drive failure and timing paths deterministically. A "seam" is a place where a
test can alter or observe behavior without editing the code in place; a `Seamster` is the object that works them.

It has three halves:

- **Fault injection** makes a site *misbehave*. A test arms a named fault; the host consults it at the exact point
  it affects and simulates an error, drop, or stale write that is otherwise hard to trigger on demand.
- **Execution checkpoints** make a site *observable and pausable*. A test rendezvouses with the host's progress,
  freezes the host at an exact point (so a concurrent operation can be driven into a precise window with no timing
  hammer), or simply counts how many times the host has passed the point.
- **Recorded variables** make a host-side *value* readable. The host records what it has computed; a test reads
  the last recording — for a value a test cannot reach otherwise, such as one the host computes and consumes
  without ever storing it.

All are **inert in production by construction**: every consult short-circuits on the `enabled` bool the host
passes to `New` (typically `testing.Testing()`), so a disabled `Seamster` pays a single lock-free bool read per
site and no seam can ever fire. The package imports only the standard library, so a host may embed it on a
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
// Make this write misbehave when a test arms the fault, targeting one task.
if h.seams.IsFault(taskFault(taskName)) {
    return errors.New("injected fault")
}

// Mark this point observable/pausable.
h.seams.Checkpoint(ctx, "beforeTransitionTx")

// Publish a value a test cannot otherwise see. Guarded, because the value is boxed at the call site,
// before the enabled gate is read.
if h.seams.Enabled() {
    h.seams.Variable("chosenBand", band)
}
```

Test side:

```go
// Fault injection: arm a fault to fire once, naming the same entity the consult names.
h.seams.Inject(taskFault("Charge"))

// Rendezvous: arm BEFORE the operation, receive after it, so the checkpoint cannot fire in the gap.
reached := h.seams.Waiter("beforeTransitionTx")
h.Process(ctx)
<-reached

// Freeze: drive a concurrent op into a precise window.
h.seams.Break("beforeTransitionTx")
go h.Process(ctx)                                  // runs into the breakpoint and blocks
h.seams.Wait(ctx, "beforeTransitionTx")            // returns once frozen
// ... run the racing operation while the host is held ...
h.seams.Resume("beforeTransitionTx")

// Count: assert how many times the host passed a checkpoint.
if h.seams.Visits("beforeTransitionTx") != 3 {
    t.Fatalf("expected 3 attempts, got %d", h.seams.Visits("beforeTransitionTx"))
}

// Capture: read the last value the host recorded. Assert the type WITHOUT the comma-ok, so a drift in
// the host's type fails loudly instead of yielding a zero value the test goes on to assert against.
v, ok := h.seams.Capture("chosenBand")
if !ok {
    t.Fatal("host never recorded chosenBand")
}
band := v.(int)
```

## API

| Method | Side | Purpose |
|---|---|---|
| `New(enabled bool)` | host | Construct a Seamster; inert when `enabled` is false. |
| `Enabled() bool` | host | Skip building anything that only a consult would use. |
| `IsFault(faultName) bool` | host | Consult a fault, consuming one fire. |
| `Inject(faultName)` | test | Arm a fault once (additive). |
| `InjectN(faultName, n)` | test | Arm a fault n times (additive). |
| `Withdraw(faultName)` | test | Disarm a fault. |
| `Checkpoint(ctx, checkpointName)` | host | Pass through a checkpoint; counts the visit, wakes waiters, honors a breakpoint. |
| `Waiter(checkpointName) <-chan struct{}` | test | Arm a waiter for the host's next arrival; the channel closes when it arrives. Does not block - arm before the triggering operation, receive after it. |
| `Wait(ctx, checkpointName) bool` | test | Arm and block until the host arrives; reports whether it did, aborting on `ctx`. Only where the host is already running into the checkpoint. |
| `WaitTimeout(ctx, checkpointName, timeout) bool` | test | `Wait` bounded by a duration - the common shape in a test. |
| `Break(checkpointName)` | test | Freeze the host at the checkpoint. |
| `Resume(checkpointName)` | test | Unfreeze the host at the checkpoint. |
| `Visits(checkpointName) int` | test | Count how many times the host has passed the checkpoint (monotonic; never blocks). |
| `Variable(variableName, value)` | host | Record the current value of a host-side variable, replacing the previous recording. Wrap in an `Enabled()` check. |
| `Capture(variableName) (any, bool)` | test | Read the last recorded value, and whether one was ever recorded. Never blocks. |

## Names

Every name above is a plain `string`, and a consult and the arming aimed at it meet on that string and nothing
else. A name identifying one entity, so a test targets it rather than "the next thing that happens," is best
built by a function the host writes once and calls at both ends, so the two cannot drift apart:

```go
func taskFault(task string) string { return "task:" + task }

// consult:  h.seams.IsFault(taskFault(taskName))
// arming:   h.seams.Inject(taskFault("Charge"))
```

Distinct names are distinct keys, with no cascade: firing `stopName("flow-1")` does not wake a waiter armed on
`"stopped"`, and each keeps its own visit count. A site wanting both granularities fires both.

On a hot path, mind what the name costs to assemble, since the caller assembles it before the gate is reached —
it is paid whether or not the seams are live. A constant name is free. An assembled one costs one allocation,
inline or through a helper. `Variable` costs one even with a constant name, because its value is boxed — which
is why every `Variable` call is wrapped. `if h.seams.Enabled() { ... }` around the call elides all of it.

Recording **overwrites**: a variable holds the last value recorded under it, never a history, so a site on a hot
path may record on every pass at a cost bounded by the number of distinct names. `Capture` has no rendezvous of
its own — it reads whatever has been recorded by the moment it runs, which is all most reads need. Where a read
must be *ordered* against the host, plant a checkpoint beside the recording site and compose: freeze, capture,
resume.
