// Package redisstream owns Redis Streams intake, stale pending-entry reclaim
// and explicit settlement. Reclaim idle time is not exclusive execution fencing.
// Failed/terminal records append before source acknowledgement; failures and
// crashes can duplicate records. Direct admission uses an atomic capacity
// check, while administrative mutations use separate commands with partial or
// unknown outcomes. Record retention is opt-in exact count trimming.
package redisstream
