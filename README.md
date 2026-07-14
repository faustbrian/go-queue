# go-queue

`go-queue` is a consolidated worker queue with owned implementations for
in-memory, Redis Pub/Sub, Redis Streams, NATS, NSQ, and RabbitMQ. It preserves
the recognizable `golang-queue` programming model while owning correctness,
operations, and releases in one module.

## Status

The package is pre-v1 and undergoing hardening. Production code is held to
meaningful 100% coverage; durable delivery claims require backend-specific
integration evidence.

## Requirements

- Go 1.25 or later
- a supported broker for non-memory backends

## Installation

```sh
go get github.com/faustbrian/go-queue
```

Backend packages ship in the same module and are imported explicitly.

## Quickstart

```go
worker, err := redisdb.NewWorkerE(
    redisdb.WithAddr("127.0.0.1:6379"),
    redisdb.WithChannel("jobs"),
    redisdb.WithRunFunc(func(ctx context.Context, task core.TaskMessage) error {
        return handle(ctx, task.Payload())
    }),
)
if err != nil {
    return err
}

q, err := queue.NewQueue(queue.WithWorker(worker), queue.WithWorkerCount(8))
if err != nil {
    return err
}
q.Start()
defer q.Release()
```

Redis Pub/Sub is low-latency and non-durable. Use Redis Streams when work must
remain pending until settlement. Read [delivery semantics](docs/delivery-semantics.md)
before selecting a backend.

## Package Guarantees

- explicit retry, acknowledgement, redelivery, cancellation, and shutdown
  behavior
- durable Redis Streams, NSQ, and RabbitMQ paths with explicit settlement
- observable lifecycle events, metrics, and backend identity
- one module and release unit for all maintained backends
- backend-specific guarantees documented without abstraction leakage

## Documentation

Start with the [documentation index](docs/README.md), [quickstart](docs/quickstart.md),
[adoption guide](docs/adoption.md), and [API reference](docs/api.md). Review the
[backend matrix](docs/backend-support.md), [failure model](docs/failure-model.md),
and [integration evidence](docs/integration-evidence.md) before production use.

AI tools can use [llms.txt](llms.txt) and [llms-full.txt](llms-full.txt).
Release history is maintained in [CHANGELOG.md](CHANGELOG.md).

## Development

Run `make check` before submitting a change. Backend changes must also pass
`make integration` with the services documented in
[CONTRIBUTING.md](CONTRIBUTING.md).

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) and follow the
[code of conduct](CODE_OF_CONDUCT.md). Every backend change must document its
delivery and settlement impact.

## Security

Report vulnerabilities privately according to [SECURITY.md](SECURITY.md).
Review [docs/security.md](docs/security.md) before processing untrusted jobs.

## License

`go-queue` is available under the [MIT License](LICENSE). Fork provenance and
third-party attribution are recorded in [NOTICE](NOTICE) and
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
