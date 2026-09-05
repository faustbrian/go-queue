# Changelog

## [Unreleased]

## [1.0.0] - 2026-09-05

### Added

- Add the target-oriented RabbitMQ adapter as the semantic owner of the
  existing queue compatibility behavior.

### Migration

- Replace imports of `github.com/faustbrian/go-queue/rabbitmq` with
  `github.com/faustbrian/go-queue/adapters/rabbitmq`; public types and behavior
  remain compatible.

[Unreleased]: https://github.com/faustbrian/go-queue/compare/adapters%2Frabbitmq%2Fv1.0.0...HEAD
[1.0.0]: https://github.com/faustbrian/go-queue/releases/tag/adapters%2Frabbitmq%2Fv1.0.0
