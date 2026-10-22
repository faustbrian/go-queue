# Changelog

## [2.0.0] - 2026-10-22

### Changed

- Move to `github.com/faustbrian/go-queue/adapters/service/v2` and build on root
  `github.com/faustbrian/go-queue/v2` and `github.com/faustbrian/go-service/v3`.
  Adapter behaviour is unchanged.

## [1.1.0] - 2026-10-22

### Changed

- Align Redis client selection at 9.23.0 with the parent queue graph.
  Native clients inherit the driver's five-second socket defaults; the
  service adapter retains its lifecycle, admission, and resource ownership.

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

[2.0.0]: https://github.com/faustbrian/go-queue/releases/tag/adapters%2Fservice%2Fv2.0.0
[1.1.0]: https://github.com/faustbrian/go-queue/releases/tag/adapters%2Fservice%2Fv1.1.0
[1.0.0]: https://github.com/faustbrian/go-queue/releases/tag/adapters%2Fservice%2Fv1.0.0
[1.0.2]: https://github.com/faustbrian/go-queue/releases/tag/adapters%2Fservice%2Fv1.0.2
