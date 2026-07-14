# Compatibility policy

The project uses semantic versioning.

- Before v1, minor releases may change APIs, but release notes identify every
  delivery, ack, retry, or shutdown semantic change.
- At v1, exported APIs and documented semantics are stable within the major
  version.
- Backend client and server version constraints are recorded in setup guides.
- Security fixes may require dependency minimum bumps in patch releases.
- Silent changes to acknowledgement or retry behavior are prohibited.

Deprecated APIs remain for at least one minor release before removal. The
compatibility `NewWorker` constructors are retained through v1; production code
should use `NewWorkerE`.
