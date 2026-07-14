# Security policy

Report vulnerabilities privately through GitHub Security Advisories. Do not
open a public issue before a fix is available. Include affected versions,
backend, reproduction, and operational impact.

Pre-v1 receives fixes on the latest minor line. After v1, the latest major and
previous major receive critical fixes for a documented support window.

## Production security baseline

- Use verified TLS and broker authentication. Redis TLS is opt-in and
  `WithSkipTLSVerify` is unsafe outside isolated tests.
- Keep the one-mebibyte encoded-message limit, 100-retry ceiling, finite worker
  count, and default 10,000-job ring capacity unless a measured deployment has
  stricter upstream admission controls.
- Treat payloads and broker metadata as untrusted. Decoder and state-machine
  fuzz targets run through `scripts/check-fuzz.sh`.
- Do not log connection URIs, credentials, payloads, or raw option dumps.
- Use idempotent handlers for every durable backend and operate poison/dead
  letter policy explicitly.

See the [threat and failure model](docs/failure-model.md) and
[hardening report](docs/hardening-report.md).
