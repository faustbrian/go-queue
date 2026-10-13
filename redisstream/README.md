# Redis Streams compatibility facade

This package preserves `github.com/faustbrian/go-queue/redisstream` and its
historical `redisdb` package identifier for existing v1 consumers.

Deprecated: new code should install and import
`github.com/faustbrian/go-queue/adapters/redisstream`. All exported types and
functions delegate to that target-oriented semantic owner; no worker or
mutable runtime state is copied.

## Status and requirements

This is a deprecated stable-v1 compatibility package in the root module. It
requires Go 1.27.0 or later.

See the [successor README](../adapters/redisstream/README.md) and
[migration guide](../docs/migration.md). The compatibility path remains
supported for the documented v1 deprecation interval.

Continue with the [parent documentation index](../docs/README.md) or the
[versioned Golib ecosystem index](https://github.com/faustbrian/go-library-tools/blob/v1.8.4/docs/ecosystem/README.md).
