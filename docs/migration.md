# Migration from golang-queue

## Import mapping

| Upstream | Consolidated import |
| --- | --- |
| `github.com/golang-queue/queue` | `github.com/faustbrian/go-queue` |
| `github.com/golang-queue/redisdb` | `github.com/faustbrian/go-queue/redisdb` |
| `github.com/golang-queue/redisdb-stream` | `github.com/faustbrian/go-queue/redisstream` |
| `github.com/golang-queue/nats` | `github.com/faustbrian/go-queue/nats` |
| `github.com/golang-queue/nsq` | `github.com/faustbrian/go-queue/nsq` |
| `github.com/golang-queue/rabbitmq` | `github.com/faustbrian/go-queue/rabbitmq` |

The `redisstream` package retains the upstream Go package name `redisdb`, so an
explicit import alias is recommended.

## Intentional divergences

1. Prefer `NewWorkerE`; it returns connection/configuration errors. Compatibility
   `NewWorker` now panics immediately instead of logging and using nil state.
2. Redis Streams, NSQ, and RabbitMQ settle after handler completion. This fixes
   upstream early acknowledgements and can expose redeliveries previously lost.
3. `WithMetric` is honored and each queue owns independent defaults.
4. `WithObserver` exposes structured lifecycle events.
5. Lifecycle events now carry backend and logical queue identity.
6. Backend startup/request waits are configurable, and malformed wire payloads
   return errors instead of producing zero-valued jobs.
7. Core NATS no longer calls `Msg.Ack`; Core NATS has no durable settlement and
   the inherited call rejected valid messages without reply subjects.
8. Integration tests are build-tagged and separated from hermetic unit runs.

Migrate one backend at a time, compare retry and shutdown behavior in staging,
and verify handler idempotency before enabling explicit redelivery paths.
