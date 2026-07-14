# Contributing

Use Go 1.25 or newer. Keep changes focused, add a failing behavior test before
production code, and preserve backend-specific semantics.

Before submitting:

```sh
gofmt -w .
go test ./...
go test -race ./...
go vet ./...
```

Integration tests require Docker and run with `go test -tags=integration`.
Changes to delivery semantics require backend integration coverage and updates
to the semantics matrix, migration notes, and changelog.

All contributions are accepted under the MIT License. Preserve provenance for
code derived from upstream projects.
