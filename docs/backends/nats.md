# NATS setup

The `nats` package uses Core NATS queue subscriptions. Configure one or more
addresses, a subject, and a queue group. Core NATS is at-most-once and has no
durable ack/redelivery; this backend is not JetStream.

`WithConnectTimeout` bounds startup and `WithRequestTimeout` bounds an idle
request. Core NATS deliveries are deliberately not passed to `Msg.Ack`, because
that method requires a reply subject and is not a Core NATS settlement API.

Use it for low-latency work where loss during disconnect is acceptable. Select
a durable backend when processing must survive consumer or broker interruption.
