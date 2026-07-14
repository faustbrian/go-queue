# RabbitMQ setup

Configure AMQP URI, durable exchange name/type, routing key, queue, consumer tag,
and `autoAck`. Keep `autoAck=false` for at-least-once processing: ack occurs after
handler success and nack/requeue after final failure.

`WithReconnectConfig` controls initial startup attempts. Runtime reconnection is
not hidden by v1; supervise worker/service restart and observe failures. Native
DLX, TTL, priorities, and publisher confirms remain explicit RabbitMQ concerns.
