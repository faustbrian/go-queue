# Delivery semantics

No backend in this module promises exactly-once processing. Handlers must be
idempotent whenever a transport can redeliver.

| Backend | Delivery | Ack point | Failure behavior | Important limitation |
| --- | --- | --- | --- | --- |
| Ring | In-memory at-most-once after process loss | None | Job is gone on process loss | Not durable |
| Redis Pub/Sub | At-most-once | None | Disconnected subscribers miss work | No persistence or replay |
| Redis Streams | At-least-once | After handler success | Final failure and malformed work remain pending | Pending recovery/claim policy is operator-managed in v1 |
| Core NATS | At-most-once | None | Disconnect can lose work | This is not JetStream |
| NSQ | At-least-once | FIN after success | REQ after final failure/panic; malformed work is finished | Ordering is not guaranteed |
| RabbitMQ | At-least-once when `autoAck=false` | Ack after success | Nack/requeue after handler failure; malformed work is rejected | `autoAck=true` weakens this to broker auto-ack; connection loss requires a replacement worker |

Retries occur inside a delivery attempt. The backend ack is not sent between
handler retries. A process crash can redeliver work even after application side
effects completed but before the ack reached the broker.

RabbitMQ enqueue uses persistent delivery mode and waits for a positive
publisher confirmation. This confirms broker acceptance, not completion of the
handler. All network deliveries are rejected above one mebibyte of encoded JSON.

Restart evidence distinguishes transport behavior. Core NATS and Redis Pub/Sub
remain lossy despite reconnecting because disconnected subscribers have no
replay. NSQ reconnects and resumes its durable topic/channel. Redis Streams
retains queued backlog. The current RabbitMQ worker does not rebuild a closed
AMQP connection or channel; supervision must replace it.
