# Versioning and release guide

Releases follow semantic versioning and are created from signed `vX.Y.Z` tags.

1. Ensure CI, integration, coverage, lint, security, docs, and example gates pass.
2. Add user-visible changes and semantic notes to `CHANGELOG.md`.
3. Verify `go mod tidy` produces no diff.
4. Create and push an annotated version tag.
5. The release workflow verifies the tag and publishes release notes/checksums.
6. Verify the public module through the Go proxy before announcing adoption.

Any ack, retry, redelivery, ordering, or shutdown behavior change must be called
out explicitly and treated as breaking when existing correctness can change.
