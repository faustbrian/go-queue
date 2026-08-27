# Changelog

All notable changes to the RabbitMQ compatibility module are documented here.

## [Unreleased]

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
