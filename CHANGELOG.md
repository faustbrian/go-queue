# Changelog

All notable changes are documented here. The project follows semantic
versioning and Keep a Changelog structure.

## [Unreleased]

### Added

- Evidence-driven audit and hardening goal covering lifecycle safety,
  backend-specific delivery semantics, failure injection, and operations.
- Consolidated core, Redis, Redis Streams, NATS, NSQ, and RabbitMQ packages.
- Error-returning backend constructors.
- Structured lifecycle observation.
- Explicit post-handler settlement for durable backends.
- Bounded backend connection, request, and NSQ touch configuration.
- Backend and logical queue identity on lifecycle events.
- Redis Streams group depth, lag, pending, and oldest-job-age statistics.
- Hermetic Redis and NATS scenarios plus Redis enqueue, consume, ack, retry,
  and shutdown benchmarks.
- Consistent repository automation for Go 1.25.12, CI, dependency review,
  guarded semantic releases, and generated portable AI documentation.
- Bounded non-panicking delivery decoding and fuzz targets for every backend.
- Lifecycle, failure-model, performance, integration-evidence, and hardening
  reports.
- Hermetic Sentinel, lossy-delivery, and same-endpoint broker restart evidence.

### Fixed

- Custom metric collectors are now honored.
- Backend startup errors no longer continue with nil clients.
- Redis Streams, NSQ, and RabbitMQ no longer acknowledge before handling.
- Core NATS no longer rejects valid messages by calling its reply-based `Ack`.
- Malformed backend deliveries return decoding errors instead of zero-valued jobs.
- Benchmark smoke gates use bounded iterations so local Redis harnesses are not
  overloaded by Go's adaptive benchmark scaling.
- Repeated startup and release-before-start no longer duplicate schedulers or
  deadlock the in-memory ring.
- Callback and settlement panics are isolated without corrupting worker counts.
- Redis debug output no longer exposes credentials or connection strings.
- Redis Pub/Sub constructors now await subscription acknowledgement before an
  immediate publish can proceed.
- Redis Streams consumes pre-start work and does not duplicate PEL entries on
  shutdown.
- RabbitMQ publishes persistent jobs, waits for publisher confirms, rejects
  malformed deliveries without requeue, and bounds publish waits.
- NSQ finishes malformed poison messages instead of redelivering forever.
- Network messages, retry metadata, scheduler intervals, and default in-memory
  admission now have explicit safety bounds.
- NSQ startup and Redis Streams reader shutdown are serialized with teardown;
  Core NATS drains callbacks without duplicate completion-channel closes.
- RabbitMQ establishes its queue binding before confirmed publication.
- Credential-bearing Redis, NATS, and RabbitMQ client errors are redacted while
  retaining their causes for programmatic inspection.
- Queue request wake-ups now converge on one shutdown check so full-coverage CI
  results do not depend on scheduler selection between simultaneously ready
  channels.

[Unreleased]: https://github.com/faustbrian/go-queue/compare/v0.0.0...HEAD
