# Roadmap

## v1 gates

- Make backend integration fixtures hermetic and repeatable.
- Reach and enforce meaningful 100% production-code coverage.
- Add Redis enqueue/consume/ack/retry/shutdown benchmarks.
- Complete backend depth and age observation where protocols expose it.
- Validate all examples and documentation links in CI.
- Complete dependency, vulnerability, and tagged release automation.

## After v1

- Optional middleware/interceptor chain.
- Explicit dead-letter and Redis pending-claim helpers.
- Deduplication and delayed-delivery helpers.
- Maintained metrics exporters outside the core contract.
