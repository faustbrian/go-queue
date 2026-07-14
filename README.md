# go-queue

`go-queue` is a consolidated Go worker queue with owned implementations for
in-memory, Redis Pub/Sub, Redis Streams, NATS, NSQ, and RabbitMQ. It began as a
compatibility-focused merge of the `golang-queue` ecosystem and intentionally
keeps that programming model recognizable while fixing correctness and
operability gaps in one release unit.

## Status

The repository is a pre-v1 release candidate. The core and five upstream
backends are consolidated, meaningful production-code coverage is enforced at
100%, Redis-critical benchmarks run in CI, and backend integrations are either
hermetic in-process tests or repeatable tagged containers. Remaining post-v1
ideas are tracked in [ROADMAP.md](ROADMAP.md).

## Install

go-queue requires Go 1.25.12 or newer.

```sh
go get github.com/faustbrian/go-queue
```

Backend packages are part of the same module:

```go
import (
	queue "github.com/faustbrian/go-queue"
	"github.com/faustbrian/go-queue/redisdb"
)
```

## Redis-first quick start

```go
package main

import (
	"context"
	"log"

	queue "github.com/faustbrian/go-queue"
	"github.com/faustbrian/go-queue/core"
	"github.com/faustbrian/go-queue/redisdb"
)

func main() {
	worker, err := redisdb.NewWorkerE(
		redisdb.WithAddr("127.0.0.1:6379"),
		redisdb.WithChannel("jobs"),
		redisdb.WithRunFunc(func(ctx context.Context, task core.TaskMessage) error {
			log.Printf("received %q", task.Payload())
			return nil
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	q, err := queue.NewQueue(queue.WithWorker(worker), queue.WithWorkerCount(8))
	if err != nil {
		log.Fatal(err)
	}
	q.Start()
	defer q.Release()
}
```

Use `NewWorkerE` in new applications. The legacy `NewWorker` constructors are
kept for upstream source compatibility and panic on startup failure.

Redis Pub/Sub is low-latency but non-durable. Use Redis Streams when work must
remain pending until the handler succeeds. Read the
[delivery semantics matrix](docs/delivery-semantics.md) before choosing.

## Backends

| Package | Transport | Durable | Explicit settlement | Primary use |
| --- | --- | --- | --- | --- |
| root `Ring` | memory | No | No | tests and in-process work |
| `redisdb` | Redis Pub/Sub | No | No | transient notifications |
| `redisstream` | Redis Streams | Yes | Yes | primary durable Redis path |
| `nats` | Core NATS | No | No | low-latency fan-out/work groups |
| `nsq` | NSQ | Yes | Yes | distributed work queues |
| `rabbitmq` | AMQP 0-9-1 | Yes | Yes | routed durable work queues |

See [backend support](docs/backend-support.md) and each
[backend setup guide](docs/backends/redis.md).

## Retries and shutdown

Retries are configured per job with `job.AllowOption`. A delivery is settled
only after all handler retries succeed or final failure is known. `Release`
initiates shutdown and waits for queue goroutines; handlers receive cancellation
through their context.

```go
err := q.QueueTask(handler, job.AllowOption{
	RetryCount: job.Int64(5),
	RetryMin:   job.Time(100 * time.Millisecond),
	RetryMax:   job.Time(10 * time.Second),
	Timeout:    job.Time(2 * time.Minute),
})
```

## Observability

Use `WithMetric` for counters and `WithObserver` for structured lifecycle
events. Events include backend and queue identity, enqueue, handler timing,
retry count and delay, settlement failures, and shutdown transitions. Redis
Streams also exposes consumer-group `Stats`, including outstanding depth,
pending count, lag, and oldest-job age. Exporters remain application choices;
the core does not require an observability framework.

```go
observer := queue.ObserverFunc(func(event queue.Event) {
	log.Printf("kind=%s backend=%s duration=%s err=%v",
		event.Kind, event.Backend, event.Duration, event.Err)
})
```

## Documentation

- [Architecture](docs/architecture.md)
- [Public API](docs/api.md)
- [Adoption guide](docs/adoption.md)
- [Migration from golang-queue](docs/migration.md)
- [Compatibility policy](docs/compatibility.md)
- [Scenario cookbook](docs/cookbook.md)
- [FAQ](docs/faq.md)
- [Troubleshooting](docs/troubleshooting.md)
- [Versioning and releases](docs/releases.md)
- [Roadmap](ROADMAP.md)
- [Changelog](CHANGELOG.md)
- [Portable AI documentation index](llms.txt)
- [Complete AI documentation bundle](llms-full.txt)

## Development

```sh
go test ./...
go test -race ./...
go vet ./...
./scripts/check-coverage.sh
go test -run='^$' -bench=. -benchmem ./...
```

Integration tests use the `integration` build tag and require their documented
backend services. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Provenance and license

The initial consolidation baseline and exact source commits are recorded in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md). The project is MIT licensed.
