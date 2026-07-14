# Architecture

The module has a small coordinator at its root and explicit backend packages.
It deliberately avoids a universal transport API that would erase correctness
differences.

## Components

- `Queue` owns concurrency, retries, cancellation, handler execution, metrics,
  observation, and final delivery settlement.
- `core.Worker` is the compatibility contract used by the coordinator.
- `job.Message` carries payload, timeout, retry policy, and optional settlement
  callbacks.
- `Ring` is the in-memory worker.
- Backend packages own connection, publish, receive, and transport-specific
  settlement behavior.

The processing path is:

```text
producer -> Worker.Queue -> backend -> Worker.Request -> Queue.handle
         -> retry/backoff -> Ack on success | Nack on final failure
```

## Lifecycle

`Start` launches the scheduling loop. The loop requests work only when a worker
slot is available. Each handler runs with a timeout context. `Shutdown` prevents
new work, asks the backend worker to stop, and signals the scheduler. `Release`
also waits for owned goroutines.

## Compatibility boundary

The root and backend option names remain close to upstream. Intentional v1
divergences are limited to error-returning constructors, correct post-handler
settlement, usable metric injection, and optional structured observation. See
[migration.md](migration.md).

## Dependency boundary

All backends live in this module and share one version. Transport clients remain
external protocol implementations, but consumers no longer select separately
released `golang-queue` adapter modules.
