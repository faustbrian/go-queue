# Redis Streams setup

Import `github.com/faustbrian/go-queue/redisstream` (the Go package identifier is
currently `redisdb` for upstream compatibility). Configure a stream, group, and
unique consumer name.

Messages are read with `XREADGROUP` and acknowledged only after handler success.
Final failures remain pending. Operators must monitor pending entries and define
claim/dead-letter policy; v1 does not silently choose one. Ordering is stream
ordered but concurrent consumers and retries change processing completion order.
