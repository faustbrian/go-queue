# Changelog

All notable changes are documented here. The project follows semantic
versioning and Keep a Changelog structure.

## Unreleased

### Added

- Consolidated core, Redis, Redis Streams, NATS, NSQ, and RabbitMQ packages.
- Error-returning backend constructors.
- Structured lifecycle observation.
- Explicit post-handler settlement for durable backends.
- Bounded backend connection, request, and NSQ touch configuration.
- Backend and logical queue identity on lifecycle events.
- Redis Streams group depth, lag, pending, and oldest-job-age statistics.
- Hermetic Redis and NATS scenarios plus Redis enqueue, consume, ack, retry,
  and shutdown benchmarks.

### Fixed

- Custom metric collectors are now honored.
- Backend startup errors no longer continue with nil clients.
- Redis Streams, NSQ, and RabbitMQ no longer acknowledge before handling.
- Core NATS no longer rejects valid messages by calling its reply-based `Ack`.
- Malformed backend deliveries return decoding errors instead of zero-valued jobs.
