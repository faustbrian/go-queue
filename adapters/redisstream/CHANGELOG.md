# Changelog

## v1.1.0 - 2026-09-05

### Added

- Add the target-oriented Redis Streams adapter as the semantic owner of the
  existing worker, settlement, management, retry, and shutdown behavior. The
  package is released with root module v1.1.0.

### Migration

- Replace imports of `github.com/faustbrian/go-queue/redisstream` with
  `github.com/faustbrian/go-queue/adapters/redisstream`; public types and
  behavior remain compatible.
