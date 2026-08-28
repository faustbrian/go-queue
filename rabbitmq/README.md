# go-queue RabbitMQ compatibility adapter

This module preserves the backend-neutral `go-queue` worker contract while
delegating RabbitMQ connections, publishing, consumption, recovery, and broker
settlement to [`go-rabbitmq-queues`](https://github.com/faustbrian/go-rabbitmq-queues).
New RabbitMQ-native applications should use that package directly.

## Install

```sh
go get github.com/faustbrian/go-queue/rabbitmq
```

## Quick start

```go
config := rabbitmq.NativeConfig{
    Connection: rabbitmqqueue.ConnectionConfig{
        Endpoints: []rabbitmqqueue.Endpoint{{Host: "rabbitmq.internal", Port: 5671}},
        VirtualHost: "/orders",
        Credentials: credentialProvider,
        TLS: rabbitmqqueue.TLSConfig{ServerName: "rabbitmq.internal"},
        DialTimeout: 5 * time.Second,
        Heartbeat: 30 * time.Second,
        Recovery: rabbitmqqueue.RecoveryPolicy{
            MaxAttempts: 8,
            InitialDelay: 100 * time.Millisecond,
            MaxDelay: 5 * time.Second,
        },
    },
    Producer: rabbitmqqueue.ProducerConfig{
        Limits: rabbitmqqueue.DefaultLimits(),
        MaxOutstanding: 256,
        PublishTimeout: 5 * time.Second,
    },
    Consumer: rabbitmqqueue.ConsumerConfig{
        Limits: rabbitmqqueue.DefaultLimits(),
        Queue: rabbitmqqueue.QueueReference{Name: "orders", Type: rabbitmqqueue.QueueQuorum},
        Name: "orders-worker",
        Prefetch: 32,
        Concurrency: 8,
        HandlerTimeout: time.Minute,
        MaxRequeues: 1,
        Failure: rabbitmqqueue.Reject(false),
    },
    MessageID: func(task core.TaskMessage) (string, error) {
        return stableApplicationID(task)
    },
}

worker, err := rabbitmq.NewWorkerE(
    rabbitmq.WithNativeConfig(config),
    rabbitmq.WithQueue("orders"),
    rabbitmq.WithTag("orders-worker"),
    rabbitmq.WithExchangeName("orders.events"),
    rabbitmq.WithExchangeType(rabbitmq.ExchangeTopic),
    rabbitmq.WithRoutingKey("orders.created"),
)
```

The producer opens during construction. The consumer opens lazily on the first
`Request`, so publishing never creates a consumer.

## API reference

- `NativeConfig` binds the adapter to explicit native connection, producer,
  consumer, queue-type, and message-identity policy.
- `NewWorkerE` returns setup failures; `NewWorker` retains the legacy panic-on-
  construction-failure behavior.
- `Worker.Queue`, `Worker.Request`, `Worker.Run`, and `Worker.Shutdown` preserve
  the `go-queue` worker surface.
- Existing routing and lifecycle options remain source-compatible where the
  adapter can preserve their semantics. `WithNativeConfig` is mandatory.

The complete exported API is available on
[`pkg.go.dev`](https://pkg.go.dev/github.com/faustbrian/go-queue/rabbitmq).

## Topology ownership

Production exchanges, queues, bindings, dead-letter policy, and permissions
remain infrastructure-owned, preferably by the RabbitMQ Kubernetes Operators.
Configure `NativeConfig` with the same identities and queue type. The adapter
does not declare or repair production topology.

## Settlement and failure policy

- Successful handlers ACK only after the native broker settlement completes.
- A bare `Nack` and canceled or infrastructure failures requeue the source.
- Retryable failures publish a mandatory persistent replacement with the same
  message ID and an incremented bounded attempt, then ACK the source only after
  confirmation.
- Permanent, malformed, and exhausted failures publish to the configured
  terminal route with stable classification and source metadata before ACK.
- Returned, rejected, not-sent, ambiguous, or failed replacement publications
  leave the source recoverable.

Replacement publication and source acknowledgement remain separate effects.
A crash after the replacement is confirmed but before the source ACK can
produce a duplicate. Applications must remain idempotent. The adapter never
claims exactly-once processing.

## Migration and rollback

1. Create operator-owned source and terminal topology matching `NativeConfig`.
2. Supply a stable outbound `MessageID`. For legacy deliveries without an AMQP
   message ID, optionally supply `DeliveryMessageID`; an error or empty result
   requeues the source.
3. Deploy one bounded canary worker and verify publish confirmations,
   redeliveries, retry attempts, terminal records, backlog, and shutdown.
4. Expand only after the old and adapter workers demonstrate compatible job
   outcomes. Do not run both implementations against incompatible topology or
   retry policy.
5. Roll back by draining adapter consumers before restoring the prior worker.
   Retain the same topology and message-ID derivation so confirmed replacements
   and redeliveries remain deduplicatable.

## Limitations

- `WithNativeConfig` is required.
- Manual acknowledgement is required; `WithAutoAck(true)` is rejected.
- Direct and topic exchanges are supported. Fanout and headers are rejected
  because their legacy job migration semantics have not been established.
- Native event distribution, RPC, independent fan-out, topology APIs, and
  advanced RabbitMQ policy are intentionally not projected through `go-queue`.
- TLS verification is mandatory through `go-rabbitmq-queues`.

## Security

Credential providers must return fresh owned credentials. Do not include
credentials, certificates, payloads, or arbitrary headers in logs or errors.
Message identity and failure codes must be stable, bounded, and non-sensitive.

CI broker evidence runs with Go 1.27.0 on Ubuntu 24.04 amd64 and RabbitMQ
4.3.5 over verified TLS.

## FAQ

### Should new services use this module?

Only when they must participate in a `go-queue` job workflow. Use
`go-rabbitmq-queues` for RabbitMQ-native messaging.

### Does a successful publish mean the handler ran?

No. It means RabbitMQ confirmed the mandatory publication and did not return it
as unroutable. Consumer execution and settlement happen later.

### Does broker recovery make processing exactly once?

No. Delivery and settlement remain at least once, and replacement publication
has an explicit duplicate window.

## Release notes

See [CHANGELOG.md](CHANGELOG.md) for module-specific behavior and migration
changes. This module uses directory-prefixed tags such as `rabbitmq/v1.0.0`.
