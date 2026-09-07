# Queue service lifecycle compatibility facade

> Deprecated: use `github.com/faustbrian/go-queue/adapters/service`. This
> released v1 path delegates to that semantic owner without copying mutable
> runtime state.

`queueservice` is the independently versioned lifecycle integration between
[`github.com/faustbrian/go-queue`](..) and
`github.com/faustbrian/go-service`. It connects caller-owned producers
and workers to service startup, readiness, supervision, drain, and shutdown
without choosing a backend or moving retry, scheduling, acknowledgement,
redelivery, or dead-letter policy out of `queue`.

The module is a deprecated stable-v1 compatibility facade at
`queueservice/v1.0.1`. It requires Go 1.26.6 or later, and consumers should pin
an exact released version.

## Install

```sh
go get github.com/faustbrian/go-queue/queueservice@v1
```

## Quick start

```go
producer, err := queueservice.NewProducer(
	queueservice.ProducerOptions[*queue.Queue]{
		Name:        "orders-producer",
		Resource:    concreteQueue,
		Correlation: correlationFactory,
		Publish: func(
			_ context.Context,
			resource *queue.Queue,
			message core.QueuedMessage,
			options ...job.AllowOption,
		) error {
			return resource.Queue(message, options...)
		},
	},
)
if err != nil {
	return err
}

runtime, err := service.New(service.Config{
	Components: []service.Component{producer.Component()},
})
if err != nil {
	return err
}
```

The [facade-owned executable examples](example_test.go) contain complete
imports and setup and are checked by this module's documentation gate.

## Guarantees and limitations

The [successor guide](../adapters/service/docs/reference.md) defines ownership, failure semantics,
bounds, concurrency, security, and unsupported behavior. Do not infer
additional guarantees beyond the documented module boundary.

## Documentation

For ecosystem-wide selection and ownership guidance, see the versioned
[Golib ecosystem index](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/README.md)
and [Persistence and durability family guidance](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/design-language.md#package-families-and-selection).

- [Successor documentation index](../adapters/service/docs/README.md)
- [Complete technical guide](../adapters/service/docs/reference.md)
- [Import migration](../docs/migration.md)
- [Go API reference](https://pkg.go.dev/github.com/faustbrian/go-queue/queueservice)
- [Parent package documentation](../docs/README.md)

## Compatibility and support

This module follows Semantic Versioning.

- [Support policy](../SUPPORT.md)
- [Security policy](../SECURITY.md)
- [MIT license](LICENSE)

## License

MIT. See [LICENSE](LICENSE).
