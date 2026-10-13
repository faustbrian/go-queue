# Versioning and release guide

[Documentation index](README.md)

Releases follow semantic versioning and are created from annotated `vX.Y.Z`
tags on a clean `main` commit synchronized with `origin/main`.

## Release commands

After moving the relevant changelog entries to a dated release section and
adding its release link, choose the module and intended compatibility level.
From the repository root, qualify the current source with the public verifier:

```sh
make check-full
golib release check --module .
golib release dry-run --module .
```

Replace `.` with the selected nested module when releasing that module, and
retain its module-specific tag prefix. Metadata validation and dry-run do not
publish. After explicit release authorization, create and publish the intended
annotated tag from qualified, clean main and publish the corresponding release
assets deliberately. This repository has no semantic-release Make targets or
publication workflow; ordinary shared CI is bounded, not full release proof.

## Release checklist

1. Ensure CI, independent Redis Streams and Valkey 9 integration, coverage,
   mutation, lint, security, docs, and example gates pass.
2. Add user-visible changes and semantic notes to `CHANGELOG.md`.
3. Verify `go mod tidy` produces no diff.
4. Run full qualification and the selected metadata check and release dry-run.
5. Publish the authorized tag, notes and checksum-bound assets for that source.
6. Verify the public module through the Go proxy before announcing adoption.

Any ack, retry, redelivery, ordering, or shutdown behavior change must be called
out explicitly and treated as breaking when existing correctness can change.
Do not publish if the relevant native Streams integration qualification fails.

If verification fails, fix forward with a normal commit. Never bypass a check,
move a published tag, or force-push release history.
