# Redis Streams setup

Import `github.com/faustbrian/go-queue/redisstream` (the Go package identifier is
currently `redisdb` for upstream compatibility). Configure a stream, group, and
unique consumer name.

Use `WithConnectTimeout`, `WithRequestTimeout`, and `WithBlockTime` to bound
startup, queue polling, and Redis blocking reads. `Worker.Stats(ctx)` reports
consumer-group pending work, lag, total outstanding depth, and oldest-job age;
depth is `-1` when Redis cannot determine lag.

Messages are read with `XREADGROUP` and acknowledged only after handler success.
Final failures remain pending. Operators must monitor pending entries and define
claim/dead-letter policy; v1 does not silently choose one. Ordering is stream
ordered but concurrent consumers and retries change processing completion order.
