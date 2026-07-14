BENCH_TIME ?= 100x

.PHONY: benchmark coverage docs fmt fuzz integration lint release-major \
	release-minor release-patch test test-race vet

test:
	go test ./...

test-race:
	go test -race ./...

integration:
	go test -tags=integration -timeout=15m ./...

coverage:
	./scripts/check-coverage.sh

benchmark:
	go test -run='^$$' -bench=. -benchmem -benchtime=$(BENCH_TIME) ./...

fmt:
	./scripts/check-format.sh

fuzz:
	./scripts/check-fuzz.sh

vet:
	go vet ./...

lint:
	golangci-lint run

docs:
	./scripts/check-docs.sh

release-patch:
	@scripts/release.sh patch

release-minor:
	@scripts/release.sh minor

release-major:
	@scripts/release.sh major
