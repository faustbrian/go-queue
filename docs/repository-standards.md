# Repository Standards

This repository follows the shared maintenance baseline used by the
`faustbrian/go-*` OSS packages.

## Mandatory Root Files

Every repository contains `.gitattributes`, `.gitignore`,
`.golangci.yml`, `AGENTS.md`, `CHANGELOG.md`, `CLAUDE.md`,
`CODE_OF_CONDUCT.md`, `CONTRIBUTING.md`, `LICENSE`, `Makefile`, `NOTICE`,
`README.md`, `ROADMAP.md`, `SECURITY.md`, and `THIRD_PARTY_NOTICES.md`.

Completed implementation plans and verification snapshots belong in issue
tracking or Git history rather than the released source tree.

`NOTICE` identifies project and inherited ownership. `THIRD_PARTY_NOTICES.md`
separately records detailed fork provenance and third-party attribution. Both
remain present even when no additional third-party source requires
attribution. A package may retain a different approved OSS license when
provenance requires it.

## Mandatory Documentation

The shared taxonomy is lowercase kebab-case and includes a documentation
index, quickstart, usage guidance, API reference, architecture, examples,
cookbook, FAQ, troubleshooting, migration, compatibility, performance,
security, Go safety and concurrency, and releasing guide.
Package-specific documents extend this taxonomy without renaming shared
concepts.

## Mandatory Automation

This repository's `ci.yml` consumes immutable shared CI and checksum-qualified
public Tools. PR, main and scheduled runs use bounded local verification with
fail-closed Required and security checks; explicit `release_dry_run` selects
full release rehearsal. Verification uses Go 1.27.2 while all module minima
remain Go 1.27.0. Reachable dependency scanning uses `govulncheck` in the
applicable security/full route.

The root Make interface provides `config`, `inventory`, `cohesion`,
`repository-check`, `specification-check`, `workflows`, `check`, `ci`,
`specification-check-online`, and `check-full`. The first specification route
is offline; full qualification preserves online authority and all configured
API, mutation, fuzz, integration and benchmark operations. Nested module Make
targets retain their distinct full contracts. Publication is a separate,
explicitly authorized operation, not a root semantic-release Make target.

The package family shares the `GO-SAFETY-1` baseline. It forbids `unsafe`,
cgo, and `go:linkname` in production code and standardizes ownership,
goroutine lifecycle, race, fuzz, resource-bound, leak, and benchmark evidence.

## Approved Package-Specific Differences

- `jsonapi` carries JSON:API feature, conformance, extension/profile,
  recommendation, and threat-model documentation.
- `jsonrpc` carries protocol conformance and middleware documentation.
- `queue` carries backend, delivery, lifecycle, failure, and integration
  documentation plus explicit live-backend integration operations. Its fork provenance
  requires detailed third-party notices.
- `wire` carries format, dependency, and audit-evidence documentation.
- `tabular` carries format and ingest-limit documentation. It uses
  Apache-2.0 and retains XLS provenance notices.

Code, dependencies, fuzz targets, benchmark inputs, and domain-specific
security guidance are expected to differ. Shared policy wording and automation
structure must not drift without updating this contract across all affected
repositories.
