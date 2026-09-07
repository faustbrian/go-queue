GO ?= go
GOLIB ?= golib
FUZZ_TIME ?= 10000x
BENCH_TIME ?= 100ms

.PHONY: benchmark check coverage docs format-check fuzz mutation race test vet

format-check:
	test -z "$$(gofmt -l .)"

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

coverage:
	coverage_dir="$$(mktemp -d)"; \
	trap 'find "$$coverage_dir" -depth -delete' EXIT; \
	$(GO) test -count=1 -covermode=atomic -coverpkg=./... \
		-coverprofile="$$coverage_dir/coverage.out" ./...; \
	test "$$($(GO) tool cover -func="$$coverage_dir/coverage.out" | \
		awk '/^total:/ { print $$3 }')" = "100.0%"

mutation:
	cd .. && $(GOLIB) mutation --module rabbitmq

fuzz:
	$(GO) test . -run '^$$' -fuzz '^FuzzLegacyOptionValuesReachSuccessor$$' \
		-fuzztime="$(FUZZ_TIME)"

benchmark:
	$(GO) test ./... -run '^$$' -bench . -benchmem \
		-benchtime="$(BENCH_TIME)"

vet:
	$(GO) vet ./...

docs:
	test -s README.md
	test -s CHANGELOG.md
	test -s LICENSE
	test -s example_test.go
	$(GO) doc .
	$(GO) test ./... -run '^Example' -count=1

check: format-check vet test race coverage mutation fuzz benchmark docs
