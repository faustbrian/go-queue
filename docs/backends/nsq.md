# NSQ setup

Configure the nsqd address, topic, channel, maximum in-flight messages, and log
level. Each received message disables NSQ automatic response; the queue sends
FIN after successful handling or REQ after final failure/panic.

NSQ does not promise global ordering. Monitor consumer stats and redelivery
attempts, and size `MaxInFlight` with the queue worker count.
