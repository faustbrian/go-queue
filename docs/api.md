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
- `core.WorkerMetadata` lets workers add backend and queue identity to every
  lifecycle event.

## Job package

- `job.Message` is the wire envelope.
- `job.AllowOption` configures retries, backoff, jitter, and timeout.
- `job.Int64`, `Float64`, `Time`, and `Bool` build pointer options.
- `job.DecodeE(data, maxBytes)` returns classifiable decode, size, and metadata
  errors. `Decode` remains a legacy panic wrapper.
- `job.DefaultMaxMessageBytes` is one mebibyte and `job.MaxRetryCount` is 100.
- `Message.Validate` checks execution metadata before enqueue or delivery.
- `Message.SetAcknowledgement` is intended for backend implementers.

## Backend constructors

Every backend provides `NewWorkerE(options...) (*Worker, error)` for production
startup and a compatibility `NewWorker(options...) *Worker` wrapper. New code
should use the error-returning form.

Backend-specific options are documented in [backend setup guides](backends/redis.md)
and in Go doc comments beside each option.

All network backends provide `WithRequestTimeout`. Redis Pub/Sub and Redis
Streams provide `WithConnectTimeout`; NATS and NSQ provide the same startup
bound, while RabbitMQ uses `WithReconnectConfig`. NSQ also provides
`WithTouchInterval`. RabbitMQ provides `WithPublishTimeout` for publish and
publisher-confirm waiting.

Redis Streams exposes `Worker.Stats(context.Context)`. Its result reports
consumer-group `Depth`, `Pending`, `Lag`, whether lag is known, and
`OldestJobAge`. `Depth` is `-1` when Redis reports indeterminate lag.
