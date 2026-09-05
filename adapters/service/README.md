# Queue service lifecycle adapter

`queueservice` is the target-oriented lifecycle integration between
[`go-queue`](../..) and `go-service`. It attaches caller-owned producers and
workers to startup, readiness, supervision, drain, and shutdown without
choosing a backend or owning retry, acknowledgement, scheduling, or
dead-letter policy.

## Status and requirements

This is the stable, supported service lifecycle adapter. It requires Go 1.26.6
or later.

## Install

```sh
go get github.com/faustbrian/go-queue/adapters/service@v1
```

## Quick start

```go
producer, err := queueservice.NewProducer(
	queueservice.ProducerOptions[*queue.Queue]{
		Name:        "orders-producer",
		Resource:    concreteQueue,
		Correlation: correlationFactory,
		Publish:     publish,
	},
)
if err != nil {
	return err
}
runtime, err := service.New(service.Config{
	Components: []service.Component{producer.Component()},
})
```

Configuration and application callbacks remain caller-owned. The adapter owns
admission and lifecycle state; transferred resources are released once through
the service shutdown plan. Publish acceptance distinguishes known rejection
from unknown outcomes. Callback errors preserve causes without disclosing
payloads, endpoints, trace baggage, or credentials.

Use this module only to connect queue resources to `go-service`. The former
`github.com/faustbrian/go-queue/queueservice` path remains available during
successor publication and becomes a deprecated compatibility facade in its
following patch release.

See the [technical guide](docs/reference.md), [documentation index](docs/README.md),
[migration guide](../../docs/migration.md), [changelog](CHANGELOG.md),
[security policy](../../SECURITY.md), [support policy](../../SUPPORT.md), and
[MIT license](LICENSE). API documentation is published at
[`pkg.go.dev`](https://pkg.go.dev/github.com/faustbrian/go-queue/adapters/service).
