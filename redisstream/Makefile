GO ?= go

.PHONY: check docs test vet

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

docs:
	test -s README.md
	test -s CHANGELOG.md
	test -s LICENSE
	$(GO) doc .

check: vet test docs
