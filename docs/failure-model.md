# Threat and failure model

The trust boundary begins at every enqueue caller and broker delivery. Broker
availability, network continuity, handler correctness, and observer code are
not assumed.

| Threat or failure | Library behavior | Deployment responsibility |
| --- | --- | --- |
| Malformed or oversized JSON | Rejects before execution; encoded limit is 1 MiB | Alert on repeated poison deliveries |
| Hostile retry metadata | Rejects negative, non-finite, or over-100 retry state | Choose job deadlines below service shutdown budget |
| Slow or blocked handler | Cancels its context at deadline/shutdown | Handler must cooperate and bound downstream calls |
| Observer/logger/metric panic | Recovers and preserves worker accounting | Repair the integration; callback observations may be lost |
| Ack/nack failure or panic | Returns a failed settlement event | Reconcile duplicates or pending work |
| Redis Pub/Sub/Core NATS disconnect | Work can be silently lost by protocol design | Use only for transient work |
| Redis Streams process loss | Unacked entry remains in the PEL | Monitor, claim, and dead-letter pending entries |
| NSQ process loss | Unfinished work is eligible for redelivery | Make handlers idempotent |
| RabbitMQ process loss | Manual-ack work is requeued; publish waits for confirm | Configure quorum/classic durability and DLX policy |
| Broker unavailable at startup | Error-returning constructor fails and cleans partial state | Supervise restart with bounded backoff |
| Runtime connection loss | NATS, NSQ, and Redis reconnect; RabbitMQ worker is terminal | Accept lossy gaps or replace the worker as documented |
| Credential-bearing broker URI | Debug output and constructor error text are redacted | Do not log raw options or separately unwrapped client errors |
| TLS disabled or verification skipped | Redis TLS is opt-in; skip-verify is explicit | Require verified TLS and broker authentication in policy |
| Process termination | In-memory and lossy transports lose work | Use a durable backend and idempotent side effects |

There is no compression layer, so decompression bombs do not apply. JSON base64
decoding of `Body` remains within the encoded-message limit. No backend or core
API promises exactly-once execution.

Protocol claims are grounded in the official documentation for
[Redis Pub/Sub](https://redis.io/docs/latest/develop/pubsub/),
[Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/),
[Core NATS](https://docs.nats.io/nats-concepts/what-is-nats),
[NSQ](https://nsq.io/overview/design.html), and
[RabbitMQ acknowledgements and confirms](https://www.rabbitmq.com/docs/confirms).
