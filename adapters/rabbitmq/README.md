# RabbitMQ queue adapter

`rabbitmq` is the target-oriented compatibility adapter between
[`go-queue`](../..) and `go-rabbitmq-queues`. It preserves bounded publication,
consumption, settlement, retry, dead-letter, and shutdown behavior without
owning broker topology, credentials, or application idempotency.

## Status and requirements

This is the stable, supported RabbitMQ adapter. It requires Go 1.27.0 or later.

## Install

```sh
go get github.com/faustbrian/go-queue/adapters/rabbitmq@v1
```

## Quick start

```go
worker, err := rabbitmq.NewWorkerE(
	rabbitmq.WithNativeConfig(nativeConfig),
	rabbitmq.WithQueue("jobs"),
	rabbitmq.WithExchangeName("jobs"),
	rabbitmq.WithExchangeType(rabbitmq.ExchangeDirect),
	rabbitmq.WithRoutingKey("jobs"),
)
if err != nil {
	return err
}
defer worker.Shutdown()
```

The caller owns topology and policy inputs. Construction opens the producer;
the consumer opens on first request. `Shutdown` owns bounded adapter cleanup.
Delivery is at least once, and confirmed replacement publication remains
separate from source acknowledgement. Credentials, TLS material, payloads,
and caller-controlled routing identifiers are sensitive.

Use this module when an existing `go-queue` job workflow must use RabbitMQ.
Use `go-rabbitmq-queues` directly for RabbitMQ-native APIs. The former
`github.com/faustbrian/go-queue/rabbitmq` path remains available during
successor publication and becomes a deprecated compatibility facade in its
following patch release.

See the [RabbitMQ guide](../../docs/backends/rabbitmq.md),
[migration guide](../../docs/migration.md), [changelog](CHANGELOG.md),
[security policy](../../SECURITY.md), [support policy](../../SUPPORT.md), and
[MIT license](LICENSE). API documentation is published at
[`pkg.go.dev`](https://pkg.go.dev/github.com/faustbrian/go-queue/adapters/rabbitmq).
