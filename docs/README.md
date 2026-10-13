# Documentation

[Package README](../README.md) ·
[Versioned ecosystem index](https://github.com/faustbrian/go-library-tools/blob/v1.8.4/docs/ecosystem/README.md)

Use this index to evaluate backend guarantees before deploying workers.

## Getting started

- [Quickstart](quickstart.md)
- [When to use this package](adoption.md)
- [API reference](api.md)
- [Examples](examples.md)
- Backend setup: [Redis Pub/Sub](backends/redis.md),
  [Redis Streams](backends/redis-streams.md),
  [Valkey 9 Streams](backends/valkey-streams.md), [Core NATS](backends/nats.md),
  [NSQ](backends/nsq.md), and [RabbitMQ](backends/rabbitmq.md)

## Delivery and operations

- [Architecture](architecture.md)
- [Go safety and concurrency](go-safety-and-concurrency.md)
- [Backend support](backend-support.md)
- [Backend integration evidence](integration-evidence.md)
- [Delivery semantics](delivery-semantics.md)
- [Failure model](failure-model.md)
- [Lifecycle](lifecycle.md)
- [`service` integration](service-integration.md)
- [Management protocol](management.md)
- [Dead-letter data-plane architecture](dead-letter-architecture.md)
- [Dead-letter operations and recovery](dead-letter-operations.md)
- [Laravel Horizon failed-job migration](horizon-migration.md)
- [Compatibility](compatibility.md)
- [Performance](performance.md)
- [Security](security.md)

## Troubleshooting and migration

- [Cookbook](cookbook.md)
- [FAQ](faq.md)
- [Troubleshooting](troubleshooting.md)
- [Migration](migration.md)
- [Releasing](releasing.md)

## Backend and integration entry points

- [Redis Streams package (root module)](../adapters/redisstream/README.md)
- [RabbitMQ adapter](../adapters/rabbitmq/README.md)
- [Service lifecycle adapter (independent module)](../adapters/service/docs/README.md)
- [Deprecated compatibility paths](migration.md)

## Maintainers

- [Repository standards](repository-standards.md)
- [Source documentation audit](source-documentation-audit.md)
- [Contributing](../CONTRIBUTING.md)
- [Release history](../CHANGELOG.md)
