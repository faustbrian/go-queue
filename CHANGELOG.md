# Changelog

All notable changes are documented here. The project follows semantic
versioning and Keep a Changelog structure.

## Unreleased

### Added

- Consolidated core, Redis, Redis Streams, NATS, NSQ, and RabbitMQ packages.
- Error-returning backend constructors.
- Structured lifecycle observation.
- Explicit post-handler settlement for durable backends.

### Fixed

- Custom metric collectors are now honored.
- Backend startup errors no longer continue with nil clients.
- Redis Streams, NSQ, and RabbitMQ no longer acknowledge before handling.
