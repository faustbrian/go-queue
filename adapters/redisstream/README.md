# Redis Streams queue adapter

`redisstream` is the target-oriented Redis Streams implementation of the
[`go-queue`](../..) worker contract. It owns Redis Streams delivery,
settlement, retry, dead-letter, management, and shutdown behavior; it does not
own Redis deployment, application idempotency, or service lifecycle.

## Status and requirements

This preview successor ships in root module v1.1.0 and requires Go 1.26.6 or
later.

## Install

```sh
go get github.com/faustbrian/go-queue/adapters/redisstream@v1
```

## Quick start

```go
worker, err := redisstream.NewWorkerE(
	redisstream.WithAddr("127.0.0.1:6379"),
	redisstream.WithStreamName("jobs"),
	redisstream.WithGroup("workers"),
	redisstream.WithConsumer("worker-1"),
)
if err != nil {
	return err
}
defer worker.Shutdown()
```

Configuration is caller-owned and validated by `NewWorkerE`. The worker owns
its Redis client, read loop, timers, and settlement state until `Shutdown`.
Values are safe for their documented concurrent worker use; callbacks remain
application-owned. Delivery is at least once, and unknown settlement outcomes
must be reconciled rather than retried blindly. Addresses, credentials,
payloads, and tenant identifiers are sensitive and must not be logged.

Use this package in root module `github.com/faustbrian/go-queue` for Redis
Streams consumer-group work. Use the root
`redisdb` package for Redis Pub/Sub, or `valkeystream` for Valkey-native
Streams. The former `github.com/faustbrian/go-queue/redisstream` path remains a
deprecated compatibility facade during the v1 support interval.

See the [Redis Streams guide](../../docs/backends/redis-streams.md),
[migration guide](../../docs/migration.md), [changelog](CHANGELOG.md),
[security policy](../../SECURITY.md), [support policy](../../SUPPORT.md), and
[MIT license](LICENSE). API documentation is published at
[`pkg.go.dev`](https://pkg.go.dev/github.com/faustbrian/go-queue/adapters/redisstream).
