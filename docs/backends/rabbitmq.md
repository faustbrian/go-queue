# RabbitMQ compatibility adapter

RabbitMQ support is released from the nested
`github.com/faustbrian/go-queue/rabbitmq` module. It preserves the backend-
neutral worker contract while delegating connections, TLS, publishing,
consumption, recovery, and broker settlement to `go-rabbitmq-queues`. New
RabbitMQ-native applications should use `go-rabbitmq-queues` directly.

Configure `WithNativeConfig` with verified TLS, bounded recovery, explicit
classic or quorum queue identity, producer and consumer limits, and a stable
application message-ID function. Configure the legacy queue, exchange,
routing-key, and consumer-tag options with the same infrastructure-owned
topology identities. The adapter performs no topology declaration or repair.

Published jobs are mandatory, persistent, and synchronously confirmed.
`WithPublishTimeout` bounds transmission, return reconciliation, and
confirmation. Returned, rejected, not-sent, ambiguous, and failed outcomes do
not report acceptance.

Manual settlement is required. Handler success precedes ACK. A bare NACK and
canceled or infrastructure failures requeue the source. Retryable failures
publish a confirmed replacement with a bounded incremented attempt before
ACK. Exhausted, permanent, and malformed deliveries publish a confirmed
terminal replacement with stable classification and source metadata before
ACK. If either replacement is not confirmed, the source remains recoverable.

Replacement publication and source acknowledgement are separate effects. A
crash after the replacement confirm and before source ACK can duplicate work;
applications must use stable identity and remain idempotent. Exactly-once
processing is not claimed.

The producer opens eagerly and never creates a consumer. The consumer opens
lazily on the first `Request`. Each owns an independent native connection and
bounded recovery lifecycle. Fanout, headers, and automatic acknowledgement
are rejected because their legacy job-migration semantics are not defined;
direct and topic exchanges are supported.

CI broker evidence uses RabbitMQ 4.3.5 over TLS with `go-rabbitmq-queues` and
`amqp091-go` 1.14.0. See the
[module guide](../../rabbitmq/README.md) for migration, rollback, limitations,
security, and API details.
