# Contributing

## Before Editing

1. Read [`AGENTS.md`](AGENTS.md) and the affected module's docs.
2. Run the focused Go tests for the affected package.
3. Identify owned dependencies and reverse dependants in `modules.json`.
4. Preserve unrelated work and generated/corpus provenance.

## Changes

Keep commits focused and conventional. Update every affected changelog with
the behavior and migration impact. Public API changes require compatibility
evidence and documentation. Specification behavior requires a decision record,
fixture coverage, and interoperability evidence.

New direct dependencies and dependency updates must follow the
[dependency governance policy](AGENTS.md#dependencies-and-supply-chain). Package-local
update bots are forbidden; the root policy owns every module and action update.

Specification-backed changes must follow the
[specification governance contract](AGENTS.md#design), update
the affected stable decision entries, and complete the Specification Decisions
section of the pull request template. An unresolved interpretation or stale
source pin is release-blocking; peer behavior cannot silently select policy.

Explicit full mutation qualification must finish with zero surviving viable
mutants; it is not a prerequisite for every ordinary edit.

Do not add package-local workflows, permanent replacements, machine-specific
paths, bypass flags, broad mutation exclusions, or aggregate quality metrics
that hide a failing package.

## Verification

Use Go 1.27.2 for verification; the modules' minimum remains Go 1.27.0.
From the root, ordinary standalone tests need neither sibling repositories nor
building the verifier release from source:

```bash
GOWORK=off go test ./... -count=1 -timeout=3m
```

Run the same command from an affected nested module to test that module. This
untagged route does not run the configured live-backend integration tests.
`make check` and `make ci` use the checksum-pinned public CLI and bounded
`--local --all` route, which runs deterministic tests, fuzz seeds and the named
local risk check, without active fuzz exploration. Committed-range CI supplies
`--base-sha` to select active smoke for affected fuzz targets. The manifest still requires
NATS, NSQ, Redis and Valkey with the `integration` tag for the root; the service
adapter additionally requires RabbitMQ. Have Docker and these fixtures available
for the manifest-driven route. The other three nested modules declare no services
or test tags. No sibling checkout or goal ledger is a contributor prerequisite.

The selected root race checks atomic ring admission and release admission/drain
ownership, once within two minutes:

```bash
GOWORK=off go test -race . -run '^Test(RingCapacityIsAtomicUnderConcurrentEnqueue|ReleaseContextStopsAdmissionAndDrainsHandlersBeforeShutdown)$' -count=1 -timeout=2m
```

Normal tests execute saved fuzz seeds, not active exploration. For a changed
codec, select one target and package for active exploration:

```bash
GOWORK=off go test ./job -run '^$' -fuzz '^FuzzDecodeE$' -fuzztime=2s -timeout=2m
golib mutation --module . --package ./job
```

The second command is optional focused local mutation, not full qualification.
Agent-run Go commands must use a task-owned disposable `GOCACHE` and remove it
after the bounded run.

Shared PR, main and scheduled CI use the bounded route. `make check-full` and
the explicit `release_dry_run` workflow input retain full qualification: all 17
native fuzz targets at 10,000 iterations/20-minute limits, full race, coverage,
mutation, API compatibility, security, benchmarks, and selected RabbitMQ
integration operations. `make specification-check` is offline;
`make specification-check-online` retains explicit online source authority.
Full RabbitMQ fixtures require Docker and jq; service pod-integration exercises
its separate Kind fixture, not ordinary standalone Go tests. Nested module
Make `check` targets retain their existing full contracts. Use the
[release guide](docs/releasing.md) for deliberate release qualification and
publication. Report unavailable or failing commands without claiming release
readiness from the bounded gate.

## Adding A Module

Follow [repository structure policy](AGENTS.md#repository-structure). New modules
require an explicit purpose, ownership boundary, dependency review, package
catalog entry, full quality gates, documentation, changelog, license, security
policy, compatibility plan, and release dry-run.

## Source Documentation

Install the checksum-pinned public verifier declared in `.golib.yaml`.
`make source-docs-check` validates objective Go comment, marker, link and
generated-producer structure; `make check`, `make check-full` and `make ci`
require it before their existing checks. Shared CI runs the same strict
contract and includes its result in Required. This is structural enforcement,
not a substitute for source-truth review or behavioral verification.
