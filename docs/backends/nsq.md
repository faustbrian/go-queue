# NSQ setup

Configure the nsqd address, topic, channel, maximum in-flight messages, and log
level. Each received message disables NSQ automatic response; the queue sends
FIN after successful handling or REQ after final failure/panic.

`WithConnectTimeout` bounds broker connection attempts, `WithRequestTimeout`
bounds idle requests, and `WithTouchInterval` controls in-flight heartbeats.

NSQ does not promise global ordering. Monitor consumer stats and redelivery
attempts, and size `MaxInFlight` with the queue worker count.

Malformed or oversized messages are finished to prevent an endless poison
loop, while handler failures are requeued. Integration uses nsqd 1.3.0 with
`go-nsq` 1.1.0 and proves the consumer reconnects after a same-endpoint broker
restart.
