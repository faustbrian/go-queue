# Migration from golang-queue

## Target-oriented adapter paths

Three integration paths have additive successors. The root-module Redis
Streams pair is already released; RabbitMQ and service lifecycle publish their
successors before converting the existing v1 paths into deprecated facades:

| Compatibility path | Successor path | Package identifier |
| --- | --- | --- |
| `github.com/faustbrian/go-queue/redisstream` | `github.com/faustbrian/go-queue/adapters/redisstream` | `redisdb` becomes unambiguous `redisstream` |
| `github.com/faustbrian/go-queue/rabbitmq` | `github.com/faustbrian/go-queue/adapters/rabbitmq` | `rabbitmq` |
| `github.com/faustbrian/go-queue/queueservice` | `github.com/faustbrian/go-queue/adapters/service` | `queueservice` |

For RabbitMQ and service integration, change only the import path. The existing
v1 implementations remain source-compatible during successor publication and
become delegating facades in their following patch releases. Redis Streams also
requires callers that relied on the implicit package identifier to rename
`redisdb` references to `redisstream`, or to retain an explicit `redisdb`
import alias. Do not deploy both paths as separate workers: they implement the
same delivery and ownership rules.

Release order is mandatory. Root `v1.1.0` publishes the Redis Streams successor
and compatibility facade atomically. Publish `adapters/service/v1.0.0` after
root `v1.1.0`, then publish the `queueservice` compatibility patch. RabbitMQ is
an independent sequence: publish `adapters/rabbitmq/v1.0.0`, verify it as a
clean external consumer, then publish the `rabbitmq` compatibility patch.
Rollback changes imports back to the v1 path without changing queue topology,
message identity, retry policy, or shutdown order.

## Import mapping

| Upstream | Consolidated import |
| --- | --- |
| `github.com/golang-queue/queue` | `github.com/faustbrian/go-queue` |
| `github.com/golang-queue/redisdb` | `github.com/faustbrian/go-queue/redisdb` |
| `github.com/golang-queue/redisdb-stream` | `github.com/faustbrian/go-queue/redisstream` |
| No upstream package | `github.com/faustbrian/go-queue/valkeystream` |
| `github.com/golang-queue/nats` | `github.com/faustbrian/go-queue/nats` |
| `github.com/golang-queue/nsq` | `github.com/faustbrian/go-queue/nsq` |
| `github.com/golang-queue/rabbitmq` | `github.com/faustbrian/go-queue/adapters/rabbitmq` |

The compatibility `github.com/faustbrian/go-queue/redisstream` package retains
the upstream Go package name `redisdb`; the successor package identifier is
`redisstream`.

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
9. Encoded broker deliveries above one mebibyte and retry counts above 100 are
   rejected before execution.
10. The in-memory ring defaults to 10,000 queued jobs. Pass
    `WithQueueSize(0)` only when intentionally preserving unlimited growth.
11. RabbitMQ publishes are persistent and synchronously confirmed within
    `WithPublishTimeout` (five seconds by default).
12. RabbitMQ now requires explicit `NativeConfig`, stable message identity,
    verified TLS, infrastructure-owned topology, manual settlement, and a
    distinct configured terminal route. Upgrade the parent module first, then
    install the independently tagged `adapters/rabbitmq` module. The former
    `rabbitmq` module becomes a deprecated compatibility facade in its following
    patch release.
13. NSQ now publishes bounded terminal envelopes before `FIN`; configure the
    terminal topic and update operations that previously expected malformed
    work to disappear.
14. Valkey Streams is a new additive native backend. It does not replace or
    alias the retained Redis Streams package.

Migrate one backend at a time, compare retry and shutdown behavior in staging,
and verify handler idempotency before enabling explicit redelivery paths.
For RabbitMQ, canary the adapter against the existing topology, drain legacy
consumers before cutover, and retain the same topology and message-ID derivation
for rollback. Do not run old and new workers together when retry or terminal
policy differs.

For Redis-to-Valkey adoption, deploy a separate Valkey stream and group. If a
temporary dual-publish is necessary, carry the same application idempotency key
in both messages, compare independent metrics, stop Redis producers, and drain
Redis lag plus pending work before stopping its consumers. Stream entries can
be copied, but consumer-group pending ownership cannot be migrated safely as a
client-side operation.
