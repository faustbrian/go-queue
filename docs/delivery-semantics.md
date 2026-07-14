# Delivery semantics

No backend in this module promises exactly-once processing. Handlers must be
idempotent whenever a transport can redeliver.

| Backend | Delivery | Ack point | Failure behavior | Important limitation |
| --- | --- | --- | --- | --- |
| Ring | In-memory at-most-once after process loss | None | Job is gone on process loss | Not durable |
| Redis Pub/Sub | At-most-once | None | Disconnected subscribers miss work | No persistence or replay |
| Redis Streams | At-least-once intent | After handler success | Final failure remains pending | Pending recovery/claim policy is operator-managed in v1 |
| Core NATS | At-most-once | None | Disconnect can lose work | This is not JetStream |
| NSQ | At-least-once | FIN after success | REQ after final failure/panic | Ordering is not guaranteed |
| RabbitMQ | At-least-once when `autoAck=false` | Ack after success | Nack with requeue after failure | `autoAck=true` weakens this to broker auto-ack |

Retries occur inside a delivery attempt. The backend ack is not sent between
handler retries. A process crash can redeliver work even after application side
effects completed but before the ack reached the broker.
