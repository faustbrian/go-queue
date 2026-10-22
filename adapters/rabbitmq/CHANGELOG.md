# Changelog

## [2.0.0] - 2026-10-22

### Changed

- Move to `github.com/faustbrian/go-queue/adapters/rabbitmq/v2` and build on root
  `github.com/faustbrian/go-queue/v2`.
  Adapter behaviour is unchanged.

## [1.1.0] - 2026-10-22

- Republished from the root 1.1.5 source snapshot; no adapter change.

## [1.0.2] - 2026-10-08

### Changed

- Adopt go-rabbitmq-queues v1.1.1 for bounded channel startup and admission
  before header and configuration copying. Preserve adapter APIs, manual
  settlement, and application-owned broker and callback responsibilities.

## [1.0.1] - 2026-10-05

### Changed

- Refresh indirect dependency selection while retaining RabbitMQ adapter
  APIs, message formats, and settlement behavior.

## [1.0.0] - 2026-09-05

### Added

- Add the target-oriented RabbitMQ adapter as the semantic owner of the
  existing queue compatibility behavior.

### Changed

- Adopt exact-module release rehearsal support so this independently
  releasable successor can be published without colliding with existing tags.

### Migration

- Replace imports of `github.com/faustbrian/go-queue/rabbitmq` with
  `github.com/faustbrian/go-queue/adapters/rabbitmq`; public types and behavior
  remain compatible.

[2.0.0]: https://github.com/faustbrian/go-queue/releases/tag/adapters%2Frabbitmq%2Fv2.0.0
[1.1.0]: https://github.com/faustbrian/go-queue/releases/tag/adapters%2Frabbitmq%2Fv1.1.0
[1.0.2]: https://github.com/faustbrian/go-queue/releases/tag/adapters%2Frabbitmq%2Fv1.0.2
[1.0.0]: https://github.com/faustbrian/go-queue/releases/tag/adapters%2Frabbitmq%2Fv1.0.0
[1.0.1]: https://github.com/faustbrian/go-queue/releases/tag/adapters%2Frabbitmq%2Fv1.0.1
