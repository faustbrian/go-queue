# Performance and capacity

Benchmarks are explicit performance qualification, not ordinary local CI.
`make -f verification/package.mk benchmark BENCH_TIME=100ms` runs the root
campaign with bounded measurement time; Redis shutdown uses its separate `1x`
budget. `make check-full` retains this campaign and the nested module benchmarks.
The direct benchmark target does not provision brokers; provide the documented
NATS, NSQ, Redis and Valkey fixtures before running broker-backed workloads.

The suite measures root enqueue/consume/shutdown paths, job encoding, Redis
Pub/Sub enqueue and consume, Redis Streams queue/stat operations, and Valkey
Streams enqueue, consume, reclaim, ack, retry-settlement, and shutdown. Results
depend on CPU, Docker networking, broker persistence, and host load and must not
be compared across machines without recording that environment.

Production capacity must account for:

- the default 10,000-job in-memory admission cap;
- one-mebibyte encoded broker messages;
- worker count and NSQ `MaxInFlight` alignment;
- Redis Streams source admission, pending-entry growth, and claim policy;
- Valkey hard source admission capacity, read/reclaim batch bounds, blocking
  pool, pending age, and DLQ growth;
- RabbitMQ adapter overhead separately from native synchronous publisher-
  confirm latency;
- handler deadline and retry delay within the shutdown budget; and
- broker depth, lag, oldest age, redelivery, and settlement errors.

The library exposes meaningful in-process counters plus Redis and Valkey
Streams group statistics. Valkey counters are process-local monotonic values;
derive rates by sampling them and combine them with server metrics. Redis
Pub/Sub and Core NATS cannot report durable depth because the
protocols do not retain a work queue. RabbitMQ depth belongs to the management
API and NSQ exposes consumer statistics through its client.

Before a deployment scale change, run the explicit benchmark target, a
representative broker load test, the affected module's race tests, and a bounded
shutdown test with the real handler and persistence policy. Select integration
tags and real fixtures where those assertions depend on a broker.
