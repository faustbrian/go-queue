# Delivery semantics

No backend in this module promises exactly-once processing. Handlers must be
idempotent whenever a transport can redeliver.

| Backend | Delivery | Ack point | Failure behavior | Important limitation |
| --- | --- | --- | --- | --- |
| Ring | In-memory at-most-once after process loss | None | Job is gone on process loss | Not durable |
| Redis Pub/Sub | At-most-once | None | Disconnected subscribers miss work | No persistence or replay |
| Redis Streams | At-least-once | After handler success | Failed attempts are recorded, pending work is reclaimed, and terminal work is appended to a DLQ | Record append and source ack are non-atomic; duplicate records are possible |
| Valkey Streams | At-least-once | After handler success | Final failure remains pending, idle work is reclaimed, terminal work is appended to DLQ | Ack and DLQ source settlement can be ambiguous; handlers and DLQ consumers must be idempotent |
| Core NATS | At-most-once | None | Disconnect can lose work | This is not JetStream |
| NSQ | At-least-once | FIN after success | REQ for recoverable failures; exhausted, permanent, and malformed work is published to the terminal topic before FIN | Publish/FIN crash windows may duplicate; ordering is not guaranteed |
| RabbitMQ adapter | At-least-once with required manual settlement | Ack after success | Confirmed retry republish; exhausted, permanent, and malformed work is confirmed to the configured terminal exchange before source ack | Confirm/ack crash windows may duplicate; runtime recovery is bounded and can become terminal |

Retryable failures may retry inside a delivery attempt. Permanent, malformed,
canceled, and infrastructure failures proceed to backend settlement after the
first handler execution. The backend ack is not sent between retryable handler
attempts. A process crash can redeliver work even after application side effects
completed but before the ack reached the broker.

Workers may implement `core.DeliveryValidator` for deterministic checks that
must observe the original decoded execution metadata. The root queue invokes it
once before handler timeout or retry execution. A validation failure skips the
handler and proceeds through the delivery's classified backend settlement.

Backends may implement the additive `core.FailureAcknowledger` contract to
receive the classified final handler error. `job.Message` exposes
`SetFailureAcknowledgement` for this path and falls back to the legacy `Nack`
callback when no error-aware callback is attached.

RabbitMQ adapter enqueue uses mandatory persistent delivery mode and reconciles
returns before reporting a positive
publisher confirmation. This confirms broker acceptance, not completion of the
handler. All network deliveries are rejected above one mebibyte of encoded JSON.

Restart evidence distinguishes transport behavior. Core NATS and Redis Pub/Sub
remain lossy despite reconnecting because disconnected subscribers have no
replay. NSQ reconnects and resumes its durable topic/channel. Redis Streams
retains queued backlog. RabbitMQ producer and consumer resources independently
run bounded native recovery and expose a terminal state when that policy is
exhausted.

Valkey Streams uses package-managed bounded `XAUTOCLAIM` recovery. A worker
crash, process termination, or failed settlement leaves an entry pending unless
Valkey already applied an acknowledgement whose response was lost. The
terminal DLQ operation appends before acknowledging the source; an ack failure
after append can duplicate the DLQ entry. These are normal at-least-once
boundaries, not exactly-once transactions.

Valkey dead-letters permanent and malformed handler failures immediately.
Retryable failures reach the dead-letter stream only at the configured backend
delivery-attempt limit. Canceled and infrastructure failures remain pending
even at that limit so shutdown, lease, broker, and settlement uncertainty do
not discard recoverable work.
