# Package Maintenance Rules

These rules apply to all package work in this repository. RFC 2119
keywords (MUST, MUST NOT, SHOULD, SHOULD NOT, MAY) are used
intentionally.

## Release And Documentation Hygiene

- You MUST update `CHANGELOG.md` for every implementation task that
  creates, modifies, or deletes files before claiming completion.
- You MUST update `README.md`, examples, and other package
  documentation when public behavior, configuration, installation, or
  usage changes.
- You MUST document breaking changes, removals, and migration steps in
  `CHANGELOG.md` before the work is considered complete.
- You SHOULD prefer a documented deprecation path before removing or
  renaming public APIs.

## Public API And Compatibility

- You MUST treat public Go APIs, exported types, exported functions,
  interfaces, configuration keys, environment variables, wire formats,
  error contracts, and command-line behavior as SemVer-governed surface
  area.
- You MUST NOT introduce backward-incompatible behavior without a clear,
  documented architectural reason and explicit release-note coverage.
- You MUST NOT alternate between conflicting style patterns without a
  documented architectural reason.
- You MUST keep commits and feature changes focused. Unrelated refactors
  MUST be split into separate work.

## Testing And Verification

- You MUST add or update automated tests for every bug fix and every
  user-visible behavior change.
- You MUST prefer regression coverage before changing existing behavior.
- You MUST maintain meaningful `100%` coverage for production package
  code; line-hitting without behavior proof does not satisfy this rule.
- You MUST run the repository's formatting checks before pushing.
- You MUST run the repository's lint and static-analysis checks before
  pushing.
- You MUST run the repository's automated tests before pushing.
- You MUST report the exact verification commands you ran when handing
  work off for review.

## Dependency Discipline

- You MUST keep the dependency surface minimal and MUST NOT add,
  upgrade, or remove dependencies without a clear maintenance benefit.
- You MUST verify new dependency constraints against the package's
  supported Go and platform matrix before merging.
- You SHOULD prefer existing project utilities and well-understood
  backend dependencies over adding new libraries for small conveniences.

## Runtime Safety

- You MUST treat concurrency, cancellation, shutdown, retries,
  acknowledgements, and redelivery behavior as first-class runtime
  concerns.
- You MUST NOT introduce shared mutable state without a clear ownership
  and synchronization model.
- You SHOULD prefer explicit context propagation, deterministic cleanup,
  and bounded resource lifetimes over implicit globals or hidden
  background work.
