# Contributing

## Before Opening A Change

Use an issue for changes affecting worker lifecycle, retries, settlement, backends, observability, and compatibility. Explain the user problem,
compatibility impact, and why the behavior belongs in this generic package.

## Development Setup

Requirements:

- Go 1.25 or later
- Git
- `golangci-lint` v2

```sh
go mod download
make check
```

## Change Requirements

- Add regression coverage before fixing a defect.
- Maintain meaningful 100% production-code coverage.
- Update public examples and documentation with behavior changes.
- Update `GOAL.md` or `GOAL_HARDEN.md` when scope or acceptance criteria
  change.
- Add an `Unreleased` entry to `CHANGELOG.md`.
- Explain every dependency addition, upgrade, or removal.
- Update `NOTICE` and `THIRD_PARTY_NOTICES.md` when attribution changes.

## Package-Specific Review

Document delivery, durability, acknowledgement, redelivery, and shutdown impact for every affected backend. Backend changes MUST pass `make integration` with the documented services.

## Local Verification

Run the complete local gate:

```sh
make check
```

Backend changes also require:

```sh
make integration
```

## Commits And Pull Requests

Use focused conventional commits with a body explaining why the change is
needed. Pull requests must describe compatibility impact, tests, verification
commands and results, documentation updates, and changelog updates.

## Reporting Security Issues

Do not open a public issue for a suspected vulnerability. Follow
[SECURITY.md](SECURITY.md).
