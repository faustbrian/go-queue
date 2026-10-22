# Compatibility policy

[Documentation index](README.md)

The project uses semantic versioning.

- Breaking API changes require a new major release, and release notes identify every
  delivery, ack, retry, or shutdown semantic change.
- Exported APIs and documented semantics are stable within each major
  version.
- Backend client and server version constraints are recorded in setup guides.
- Security fixes may require dependency minimum bumps in patch releases.
- Silent changes to acknowledgement or retry behavior are prohibited.
- Redis Streams remains source-compatible and backed by `go-redis/v9`.
  Valkey Streams is additive, backed by `valkey-go`, and does not alias or
  replace Redis.
- Security bounds are contracts: encoded messages default to 1 MiB, retry count
  is at most 100, and the in-memory queue defaults to 10,000 pending jobs.
- `CloseAdmission`, `ReleaseContext`, and the independently versioned
  `adapters/service` module are stable APIs.
  Existing `Shutdown` and `Release` retain their force-compatible
  signatures and cancellation behavior.
- `job.Metadata.TraceContext`, `Message.TraceContextMetadata`, and
  `adapters/service` caller-owned trace propagators are stable APIs.
  `Metadata.TraceID` remains an operational field and is not interpreted as
  W3C propagation state.

Deprecated APIs remain for at least one minor release before removal. The
compatibility `NewWorker` constructors are retained in v2; production code
should use `NewWorkerE`.

`WithQueueSize(0)` retains the upstream unlimited ring behavior as an explicit
escape. It is not recommended for untrusted or bursty producers.

The tested Valkey server line is 9.x, currently 9.1.0. Only standalone
operation is supported. Valkey cluster or managed failover support requires a
separate SemVer-reviewed API and native integration evidence. Public Valkey
signatures are compile-frozen without native client types.
