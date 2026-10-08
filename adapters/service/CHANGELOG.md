# Changelog

## [Unreleased]

### Changed

- Refresh indirect compression and Go support dependencies selected with
  the parent queue update while preserving service lifecycle and admission
  behavior.

## [1.0.2] - 2026-10-05

### Changed

- Refresh dependency selection and broker-test dependencies while
  retaining service lifecycle and queue admission behavior.

## [1.0.0] - 2026-09-05

### Added

- Add the target-oriented service lifecycle adapter as the semantic owner of
  the existing producer, worker, admission, drain, and shutdown behavior.

### Changed

- Adopt exact-module release rehearsal support so this independently
  releasable successor can be published without colliding with existing tags.

### Migration

- Replace imports of `github.com/faustbrian/go-queue/queueservice` with
  `github.com/faustbrian/go-queue/adapters/service`; public types and behavior
  remain compatible.

[Unreleased]: https://github.com/faustbrian/go-queue/compare/adapters%2Fservice%2Fv1.0.2...HEAD
[1.0.0]: https://github.com/faustbrian/go-queue/releases/tag/adapters%2Fservice%2Fv1.0.0
[1.0.2]: https://github.com/faustbrian/go-queue/releases/tag/adapters%2Fservice%2Fv1.0.2
