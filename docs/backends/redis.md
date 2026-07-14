# Redis Pub/Sub setup

Use package `redisdb`. Configure `WithAddr`, `WithUsername`, `WithPassword`,
`WithDB`, `WithChannel`, and optional TLS. Cluster and Sentinel modes are
explicit options. `WithConnectTimeout` bounds startup validation and
`WithRequestTimeout` bounds an idle request.

Pub/Sub is non-durable: messages published while a consumer is disconnected are
lost, there is no ack, and depth/job age are unavailable. Use it only when those
semantics are acceptable. For durable work use [Redis Streams](redis-streams.md).

Compatibility is tested against actively supported Redis 7 releases; Redis 6 is
accepted during the pre-v1 period where covered by integration CI.
