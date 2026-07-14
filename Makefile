.PHONY: benchmark coverage docs fmt integration lint test test-race vet

test:
	go test ./...

test-race:
	go test -race ./...

integration:
	go test -tags=integration -timeout=15m ./...

coverage:
	./scripts/check-coverage.sh

benchmark:
	go test -run='^$$' -bench=. -benchmem ./...

fmt:
	./scripts/check-format.sh

vet:
	go vet ./...

lint:
	golangci-lint run

docs:
	./scripts/check-docs.sh
