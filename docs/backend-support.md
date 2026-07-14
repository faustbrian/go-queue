# Backend support

| Capability | Ring | Redis Pub/Sub | Redis Streams | Core NATS | NSQ | RabbitMQ |
| --- | --- | --- | --- | --- | --- | --- |
| Persistent broker storage | No | No | Yes | No | Yes | Yes |
| Explicit ack after handler | N/A | No | Yes | No | Yes | Yes |
| Consumer groups | Process only | No | Yes | Queue group | Channel | Queue |
| Strict global ordering | No | No | Per stream, affected by groups | No | No | Per queue, affected by consumers |
| Native delayed delivery | No | No | No | No | Requeue delay | TTL/DLX, not wrapped |
| Depth available | In process | No | Redis commands | Server monitoring | Stats | Management API |

“Supported” means the implementation is owned in this module. It does not mean
identical guarantees. Redis Streams is the primary durable production path;
Redis Pub/Sub is supported for transient delivery only.
