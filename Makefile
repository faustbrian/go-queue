GO ?= go
GOLANGCI_LINT ?= golangci-lint
FUZZ_TIME ?= 2s
BENCH_TIME ?= 100ms

.PHONY: benchmark check coverage docs format format-check fuzz lint \
	release-major release-minor release-patch test test-race vet vuln integration

format:
	gofmt -w .

format-check:
	test -z "$$(gofmt -l .)"

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

integration:
	$(GO) test -tags=integration -timeout=15m ./...

coverage:
	./scripts/check-coverage.sh

vet:
	$(GO) vet ./...

lint:
	$(GOLANGCI_LINT) run --timeout=5m

fuzz:
	./scripts/check-fuzz.sh "$(FUZZ_TIME)"

benchmark:
	$(GO) test ./... -run '^$$' -bench . -benchmem -benchtime="$(BENCH_TIME)"

docs:
	./scripts/check-docs.sh

vuln:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...

check: format-check vet lint test-race coverage fuzz benchmark docs vuln

release-patch:
	@scripts/release.sh patch

release-minor:
	@scripts/release.sh minor

release-major:
	@scripts/release.sh major
