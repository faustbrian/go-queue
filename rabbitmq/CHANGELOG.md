# Changelog

All notable changes to the RabbitMQ compatibility module are documented here.

## [Unreleased]

### Changed

- Publish schema-v2 cohesion metadata for the RabbitMQ compatibility module
  and include it in the repository's v1.3.0 local and CI cohesion gates.
- Advance the RabbitMQ compatibility module to the repository's v1.4.0 local
  and CI cohesion gates.

### Documentation

- Link module guidance to the immutable v1.3.0 Golib ecosystem index and
  package-family selection contract.
- Advance module guidance links to the immutable v1.4.0 Golib ecosystem index
  and package-family selection contract.

## [1.0.0] - 2026-08-28

### Added

- Replace direct AMQP ownership with an explicit compatibility adapter over
  `go-rabbitmq-queues` while retaining the `go-queue` worker, option, request,
  and settlement surfaces.
- Require stable message identity, mandatory persistent publications, manual
  settlement, bounded pull requests, confirmed retry and terminal replacement
  publications, and independent producer and consumer lifecycles.
- Add CI-hosted RabbitMQ 4.3.5 TLS evidence for confirmed queueing, request and
  settlement, retry-before-ACK, terminal replacement, and producer/consumer
  failure isolation.

### Changed

- Require `WithNativeConfig`; automatic acknowledgement, fanout, and headers
  exchanges are rejected until their legacy migration semantics are defined.
- Open the producer during construction and open the consumer only when
  `Request` first needs it.
- Preserve malformed, permanent, exhausted, canceled, infrastructure, repeated
  settlement, and repeated shutdown outcomes without exposing broker details.
- Reject invalid exchange policy without logging caller-controlled identities.

[Unreleased]: https://github.com/faustbrian/go-queue/compare/rabbitmq/v1.0.0...HEAD
[1.0.0]: https://github.com/faustbrian/go-queue/releases/tag/rabbitmq/v1.0.0
