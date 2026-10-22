# Generated core-interface doubles

The current three files are generated from `core/worker.go` by explicitly
selected `go.uber.org/mock/mockgen` **v0.6.0**, not an ambient executable.
Run from the repository root with Go 1.27.3 and normal public proxy/SumDB:

```sh
GOTOOLCHAIN=go1.27.3 GOWORK=off go generate ./mocks
git diff -- mocks/mock_worker.go mocks/mock_queued_message.go mocks/mock_task_message.go
GOTOOLCHAIN=go1.27.3 GOWORK=off go test . ./core ./job ./mocks
```

The directives execute in `mocks/` with relative destination names and the
v2 `github.com/faustbrian/go-queue/v2/core` import path. Regenerate all
three files after a reviewed interface or producer change; do not manually
edit generated bodies. The root module's runtime dependency pin alone does
not identify a generator.

## Authenticated migration inputs

The selected producer is Apache-2.0 licensed. Public module authentication
resolved `https://github.com/uber/mock` tag `v0.6.0` to commit
`2d1c58167e30f380cf78e44a43b100a14767e817`, with module sums:

- Module: `h1:hyF9dfmbgIX5EfOdasqLsWD6xqpNZlXblLB/Dbnwv3Y=`
- go.mod: `h1:KiVJ4BqZJaMj4svdfmHM0AUx4NJYO8ZNpPnZn1Z+BBU=`

At migration, the owning interface input SHA-256 was
`c0abbf226a1796b703b392123660390fcc162ef0fbdf3f0c8ffb60c1b88d76bc`.
Two independent output directories produced identical bytes with these inputs.
Generated file headers truthfully record MockGen and its relative commands;
the pinned directives and this record identify the selected producer version.

The inherited generator version is still unknown. The
[historical source audit](../docs/source-documentation-audit.md) retains the
original failed reproduction and provenance uncertainty; this is a future
producer migration, not historical attribution. The selected producer adds
private `isgomock` markers and retains Worker parameter names. GoMock may now
format mock arguments by type in unexpected-call diagnostics. Interface methods,
recording and successful return behavior must remain compatible; generated
diagnostic bytes are not a delivery/runtime guarantee.
