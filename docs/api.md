# Public API reference

Go package documentation remains authoritative for exact signatures. This page
maps the stable concepts new adopters need.

## Root package

- `NewQueue(options...) (*Queue, error)` creates a coordinator around a worker.
- `NewPool(size, options...) *Queue` creates an in-memory queue.
- `NewRing(options...) *Ring` creates an in-memory worker.
- `Queue.Start`, `Shutdown`, `Release`, and `Wait` control lifecycle.
- `Queue.Queue` submits byte-backed messages; `Queue.QueueTask` submits local
  functions.
- `WithWorkerCount`, `WithQueueSize`, `WithRetryInterval`, `WithLogger`,
  `WithMetric`, `WithObserver`, and `WithAfterFn` configure coordination.
- `Metric` exposes busy, submitted, success, failure, and completed counters.
- `Observer` receives `Event` values for lifecycle transitions.

## Job package

- `job.Message` is the wire envelope.
- `job.AllowOption` configures retries, backoff, jitter, and timeout.
- `job.Int64`, `Float64`, `Time`, and `Bool` build pointer options.
- `Message.SetAcknowledgement` is intended for backend implementers.

## Backend constructors

Every backend provides `NewWorkerE(options...) (*Worker, error)` for production
startup and a compatibility `NewWorker(options...) *Worker` wrapper. New code
should use the error-returning form.

Backend-specific options are documented in [backend setup guides](backends/redis.md)
and in Go doc comments beside each option.
