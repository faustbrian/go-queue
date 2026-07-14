# Scenario cookbook

## Fixed-delay retries

Set `RetryCount` and `RetryDelay` on `job.AllowOption`. The total timeout covers
handler execution and retry waits.

## Exponential backoff

Leave `RetryDelay` at zero and set `RetryMin`, `RetryMax`, `RetryFactor`, and
optionally `Jitter`. Jitter is recommended for shared downstream outages.

## Graceful service shutdown

Stop accepting new application requests, call `Queue.Release`, and bound the
outer service shutdown deadline above the longest job timeout. A handler must
honor its context for prompt cancellation.

## Poison messages

Set a finite retry count and observe `handler_failed` plus `rejected`. v1 does
not hide backend dead-letter differences: configure RabbitMQ DLX, NSQ policy,
or Redis Streams pending inspection explicitly.

## Duplicate protection

Include an application job identifier in the payload and enforce idempotency at
the side-effect boundary. Broker acknowledgement is not a transaction with your
database or external API.
