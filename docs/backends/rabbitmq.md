# RabbitMQ setup

Configure AMQP URI, durable exchange name/type, routing key, queue, consumer tag,
and `autoAck`. Keep `autoAck=false` for at-least-once processing: ack occurs after
handler success and nack/requeue after final failure.

Published jobs use persistent delivery mode and synchronous publisher confirms.
`WithPublishTimeout` bounds publish plus confirmation wait and defaults to five
seconds. A negative confirmation or closed confirmation channel fails enqueue.

`WithReconnectConfig` controls initial startup attempts. Runtime reconnection is
not hidden by v1; a closed connection/channel is terminal, so shut down that
queue and construct a replacement worker. The broker-restart integration test
proves both the old-worker failure and replacement-worker recovery. Native
DLX, TTL, priorities, and publisher confirms remain explicit RabbitMQ concerns.
Credential-bearing AMQP URL failures return sanitized constructor text.
`WithRequestTimeout` bounds an idle delivery request. Malformed messages are
rejected without requeue when manual acknowledgement is enabled. Integration
uses RabbitMQ 3.13.7 with `amqp091-go` 1.11.0.
