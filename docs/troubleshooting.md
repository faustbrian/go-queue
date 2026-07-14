# Troubleshooting

## Startup returns a connection error

Check the backend address format, credentials, TLS mode, and broker listener.
Use `NewWorkerE`; do not recover a compatibility-constructor panic and continue.

## Jobs repeat

This is expected for at-least-once transports when processing succeeds but ack
delivery fails or the process exits. Verify idempotency and inspect `ack_failed`.

## Redis Streams messages remain pending

Final handler failures are intentionally not acknowledged. Inspect the consumer
group pending entries and operate an explicit claim/dead-letter policy.
`Worker.Stats(ctx)` reports pending count, lag, depth, and oldest-job age.

## Shutdown takes too long

Confirm handlers honor context cancellation and that task timeouts fit the
service shutdown window. Inspect shutdown events and busy-worker metrics.
Tune backend request/connect timeouts so broker outages fit that window.

## No NATS redelivery

The consolidated backend uses Core NATS, not JetStream. Core NATS does not
provide durable acknowledgement or replay.
