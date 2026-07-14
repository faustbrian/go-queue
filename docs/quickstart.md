# Quickstart

## Install

```sh
go get github.com/faustbrian/go-queue
```

## Choose A Backend

Redis is the primary adoption path. Select Redis lists or Redis Streams only
after reading their distinct acknowledgement and redelivery guarantees in
[backend support](backend-support.md). NATS, NSQ, and RabbitMQ remain optional
backend packages in the same module.

## Build A Worker

Create the backend with explicit connection options, register a task handler,
start it with a cancellable context, and treat shutdown errors as operational
failures. The runnable [Redis example](../examples/redis) shows the complete
lifecycle.

## Production Checklist

- define idempotency for every task;
- set bounded retries and backoff;
- record attempt, latency, failure, and dead-letter metrics;
- test worker termination during processing;
- confirm backend persistence and eviction policy;
- run the backend-specific integration suite before rollout.

Continue with the [adoption guide](adoption.md) and
[delivery semantics](delivery-semantics.md).
