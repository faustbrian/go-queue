# Queue service lifecycle adapter

`queueservice` is the independently versioned lifecycle integration between
[`github.com/faustbrian/go-queue`](..) and
`github.com/faustbrian/go-service`. It connects caller-owned producers
and workers to service startup, readiness, supervision, drain, and shutdown
without choosing a backend or moving retry, scheduling, acknowledgement,
redelivery, or dead-letter policy out of `queue`.

The module follows stable v1 compatibility. Consumers should pin an exact
released version.

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

The compiling examples in this module contain complete imports and setup.

## Guarantees and limitations

The [complete guide](docs/reference.md) defines ownership, failure semantics,
bounds, concurrency, security, and unsupported behavior. Do not infer
additional guarantees beyond the documented module boundary.

## Documentation

For ecosystem-wide selection and ownership guidance, see the versioned
[Golib ecosystem index](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/README.md)
and [Persistence and durability family guidance](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/design-language.md#package-families-and-selection).

- [Documentation index](docs/README.md)
- [Complete technical guide](docs/reference.md)
- [Go API reference](https://pkg.go.dev/github.com/faustbrian/go-queue/queueservice)
- [Parent package documentation](../docs/README.md)

## Compatibility and support

This module follows Semantic Versioning. Report vulnerabilities through the
[parent security policy](../SECURITY.md).

## License

MIT. See [LICENSE](LICENSE).
