GO ?= go

.PHONY: check docs test race vet

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

docs:
	test -s README.md
	test -s CHANGELOG.md
	test -s LICENSE
	$(GO) doc .

check: vet test race docs
