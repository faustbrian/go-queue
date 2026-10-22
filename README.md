# queue

[![CI](https://github.com/faustbrian/go-queue/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/faustbrian/go-queue/actions/workflows/ci.yml)
[![CodeQL](https://img.shields.io/badge/CodeQL-required-blue)](https://github.com/faustbrian/go-queue/actions/workflows/ci.yml)
[![Documentation](https://img.shields.io/badge/docs-structural_checks_in_CI-blue)](docs/)
[![Go Reference](https://pkg.go.dev/badge/github.com/faustbrian/go-queue/v2.svg)](https://pkg.go.dev/github.com/faustbrian/go-queue/v2)
[![Release](https://img.shields.io/github/v/release/faustbrian/go-queue?sort=semver)](https://github.com/faustbrian/go-queue/releases)
[![Go](https://img.shields.io/badge/go-1.27.0-00ADD8?logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

`queue` is a consolidated worker queue with root-module implementations for
in-memory, Redis Pub/Sub, Redis Streams, Valkey Streams, NATS, and NSQ.
Independently released target-oriented adapters provide
[RabbitMQ](adapters/rabbitmq/) and [service lifecycle](adapters/service/)
integration. The RabbitMQ adapter delegates AMQP policy to
[`go-rabbitmq-queues`](https://github.com/faustbrian/go-rabbitmq-queues).

## Status

The root module is stable at v2; the independently released adapters keep
their own versions. Version 2 removed the former `redisstream`, `rabbitmq` and
`queueservice` compatibility facades; see the
[migration guide](docs/migration.md#to-v2). Durable delivery
claims require [backend-specific integration evidence](docs/integration-evidence.md).

## Requirements

- Go 1.27.0 or later
- a supported broker for non-memory backends

CI and the development workspace use Go 1.27.3. Build applications with
a patched Go release; this CI selection does not raise module language
minimums or patch the runtime of already-built applications.

## Installation

```sh
go get github.com/faustbrian/go-queue/v2@v2.0.0
```

Root backend packages, including `adapters/redisstream`, ship in the same
module and are imported explicitly. Install the independently versioned
`github.com/faustbrian/go-queue/adapters/rabbitmq` or
`github.com/faustbrian/go-queue/adapters/service` module when needed. Both
still build on root v1 until their root-v2 majors are published; keep their
users on root v1 until then.

## Choosing this package

Use `queue` for bounded worker coordination with explicit backend delivery,
retry, settlement, and shutdown semantics. Use the in-memory backend for local
work, Pub/Sub or Core NATS for disposable delivery, and a durable backend only
after checking the [backend matrix](docs/backend-support.md).

Applications own handlers, idempotency, configuration, broker deployment,
and scheduling. The separate
[`go-queue-control-plane`](https://github.com/faustbrian/go-queue-control-plane)
owns administrative authorization, audit, and policy; this library exposes
worker management contracts, not an operator dashboard. For RabbitMQ-native
messaging use [`go-rabbitmq-queues`](https://github.com/faustbrian/go-rabbitmq-queues);
RabbitMQ Streams use the separate
[`go-rabbitmq-streams`](https://github.com/faustbrian/go-rabbitmq-streams), not
this AMQP job adapter. No backend provides exactly-once application effects.

## Package and module map

| Import below `github.com/faustbrian/go-queue` | Responsibility | Release ownership |
| --- | --- | --- |
| root, `core`, `job` | Coordination, worker contracts, job envelopes | Root module |
| `redisdb`, `adapters/redisstream`, `valkeystream`, `nats`, `nsq` | Explicit native backends | Root module |
| `management`, `managementhttp` | Capability-negotiated worker status and control | Root module |
| [`adapters/rabbitmq`](adapters/rabbitmq/README.md) | AMQP job integration over `go-rabbitmq-queues` | Independent nested module |
| [`adapters/service`](adapters/service/README.md) | `go-service` admission, drain, and shutdown integration | Independent nested module |

Select and install only the modules needed. See the
[release guide](docs/releasing.md) for nested-module tags and
[compatibility guidance](docs/compatibility.md) before an upgrade.

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

Redis Pub/Sub is low-latency and non-durable. Use Redis Streams or Valkey
Streams when work must remain pending until settlement. They are independent
native backends; adopting Valkey does not require removing Redis. Read
[delivery semantics](docs/delivery-semantics.md) before selecting a backend.
Scheduler and API processes that only submit Valkey work should use
`valkeystream.NewPublisherE`; it appends jobs without joining a consumer group
or starting worker loops.

Services should compose concrete producers and workers through
[`adapters/service`](docs/service-integration.md). The adapter keeps concrete queue
APIs visible, closes queue admission during service drain, drains accepted
publishers before closing an owned transport,
uses the existing correlation queue boundary for every message and delivery
attempt, and optionally propagates bounded W3C trace context through an
explicit caller-owned OpenTelemetry propagator.

External control planes should depend on the backend-neutral contracts in
[`management`](docs/management.md). Incompatible workers remain visible, but
management capabilities are enabled only when both peers report support.
The [`managementhttp`](docs/management.md#authenticated-http-transport)
package makes those contracts remotely callable without exposing backend
clients. `managementhttp.NewFleetClient` can resolve a changing set of worker
endpoints for multi-replica deployments, aggregate their status, route
worker-specific commands, and fan queue or worker-group lifecycle commands to
every current replica.

## Package Guarantees

- explicit retry, acknowledgement, redelivery, cancellation, and shutdown
  behavior
- safe failure classification and codes that preserve `errors.Is` while
  redacting arbitrary handler, panic, and settlement text
- handler backoff limited to retryable failures so terminal and uncertain
  classifications reach backend settlement without repeated side effects
- optional one-time decoded-delivery validation before handler retry execution
- durable Redis Streams, Valkey Streams, and NSQ paths with explicit settlement
- an independently versioned RabbitMQ adapter with confirmed
  publish-before-ack settlement
- observable lifecycle events, metrics, and backend identity
- stable management-protocol version and capability negotiation for external
  control planes
- bounded worker and queue status contracts that distinguish unsupported
  backend measurements from measured zero values, with paginated readers
- bounded authenticated HTTP transport for remote status, records, and control
  commands
- bounded dynamic management fleets with fail-closed discovery, complete status
  aggregation, worker routing, and explicit partial fan-out outcomes
- backend-neutral command enforcement contracts with explicit confirmation,
  bounded bulk retry, acknowledgement, timeout, partial, and unknown outcomes
- revisioned desired-state reconciliation with monotonic per-target application,
  retry-safe failures, and caller-owned scheduling
- queue-owned pause, resume, drain, and terminate enforcement that stops
  admission at safe boundaries and reports in-flight work honestly
- bounded failed-job and dead-letter inspection with payloads hidden by
  default and privileged content capped at one mebibyte
- one root release unit for maintained root backends, with explicit nested
  release units for optional integrations
- backend-specific guarantees documented without abstraction leakage

## Documentation

Start with the [documentation index](docs/README.md), [quickstart](docs/quickstart.md),
[adoption guide](docs/adoption.md), and [API reference](docs/api.md). Review the
[backend matrix](docs/backend-support.md), [failure model](docs/failure-model.md),
[`service` integration](docs/service-integration.md), and
[integration evidence](docs/integration-evidence.md) before production use.
Valkey adopters should use the [Valkey 9 Streams guide](docs/backends/valkey-streams.md)
and [runnable example](examples/valkey).
RabbitMQ migrations should use the [target-oriented adapter guide](adapters/rabbitmq/README.md).
Release history is maintained in [CHANGELOG.md](CHANGELOG.md).

For ecosystem-wide selection and ownership guidance, see the versioned
[Golib ecosystem index](https://github.com/faustbrian/go-library-tools/blob/v1.8.4/docs/ecosystem/README.md)
and [Persistence and durability family guidance](https://github.com/faustbrian/go-library-tools/blob/v1.8.4/docs/ecosystem/design-language.md#package-families-and-selection).

## Development

Use `GOWORK=off go test ./... -count=1 -timeout=3m` for standalone untagged
tests. `make check` and `make ci` are bounded, public-verifier routes with the
manifest's real service and integration-tag requirements; `make check-full`
retains explicit full qualification. See [CONTRIBUTING.md](CONTRIBUTING.md) for
focused active fuzzing, lifecycle races, and backend fixtures.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) and follow the
[code of conduct](CODE_OF_CONDUCT.md). Every backend change must document its
delivery and settlement impact.

## Security

Report vulnerabilities privately according to [SECURITY.md](SECURITY.md).
Review [docs/security.md](docs/security.md) before processing untrusted jobs.

## License

`queue` is available under the [MIT License](LICENSE). Fork provenance and
third-party attribution are recorded in [NOTICE](NOTICE) and
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
