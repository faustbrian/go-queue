# Changelog

## [Unreleased]

## [1.0.0] - 2026-09-05

### Added

- Add the target-oriented service lifecycle adapter as the semantic owner of
  the existing producer, worker, admission, drain, and shutdown behavior.

### Migration

- Replace imports of `github.com/faustbrian/go-queue/queueservice` with
  `github.com/faustbrian/go-queue/adapters/service`; public types and behavior
  remain compatible.

[Unreleased]: https://github.com/faustbrian/go-queue/compare/adapters%2Fservice%2Fv1.0.0...HEAD
[1.0.0]: https://github.com/faustbrian/go-queue/releases/tag/adapters%2Fservice%2Fv1.0.0
