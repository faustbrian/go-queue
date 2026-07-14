# Package Maintenance Rules

These rules apply to all package work in this repository. RFC 2119 keywords
(MUST, MUST NOT, SHOULD, SHOULD NOT, MAY) are used intentionally.

## Release And Documentation Hygiene

- You MUST update `CHANGELOG.md` for every implementation task that creates,
  modifies, or deletes files before claiming completion.
- You MUST update `README.md`, examples, and package documentation when public
  behavior, configuration, installation, or usage changes.
- You MUST document breaking changes, removals, deprecations, and migration
  steps in `CHANGELOG.md`.
- You SHOULD provide a documented deprecation path before removing or renaming
  public APIs.

## Public API And Compatibility

- You MUST treat exported Go APIs, configuration, wire formats, error
  contracts, and command behavior as SemVer-governed surface area.
- You MUST NOT introduce backward-incompatible behavior without an explicit,
  documented reason and release-note coverage.
- You MUST keep changes focused. Unrelated refactors MUST be split into
  separate work.

## Testing And Verification

- You MUST add or update automated tests for every bug fix and user-visible
  behavior change.
- You MUST prefer regression coverage before changing existing behavior.
- You MUST maintain meaningful 100% coverage for production package code.
  Line-hitting without behavior proof does not satisfy this requirement.
- Before pushing, you MUST run `make check` and any package-specific
  integration checks required by `CONTRIBUTING.md`.
- You MUST report the exact verification commands and results when handing
  work off for review.

## Dependency Discipline

- You MUST keep dependencies minimal and MUST NOT add, upgrade, or remove one
  without a clear maintenance benefit.
- You MUST verify dependency constraints against the supported Go and platform
  matrix.
- You SHOULD prefer the standard library and existing project utilities for
  small conveniences.
- You MUST update `THIRD_PARTY_NOTICES.md` when copied, forked, generated, or
  vendored code changes attribution obligations.

## Runtime Safety

- You MUST treat concurrency, cancellation, shutdown, resource ownership, and
  error paths as first-class runtime concerns.
- You MUST NOT introduce shared mutable state without clear ownership and
  synchronization.
- You SHOULD prefer explicit context propagation, deterministic cleanup, and
  bounded resource lifetimes over globals or hidden background work.

## Repository-Specific Rules

- Retry, acknowledgement, redelivery, shutdown, and cancellation semantics
  MUST be explicit and verified for every backend.
- Backend-specific guarantees MUST be documented honestly; a lowest-common-
  denominator abstraction MUST NOT imply guarantees a backend cannot provide.
