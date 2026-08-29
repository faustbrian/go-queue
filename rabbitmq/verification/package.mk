SHELL := /usr/bin/env bash

.PHONY: integration

integration:
	@set -euo pipefail; \
	state="$$(mktemp "$${RUNNER_TEMP}/go-queue-rabbitmq-state.XXXXXX")"; \
	cleanup() { \
		status="$$?"; \
		if [[ -s "$$state" ]]; then \
			set -a; source "$$state"; set +a; \
			../.verification/fixtures/rabbitmq-adapter.sh stop; \
		fi; \
		rm -f "$$state"; \
		exit "$$status"; \
	}; \
	trap cleanup EXIT HUP INT TERM; \
	GITHUB_ENV="$$state" ../.verification/fixtures/rabbitmq-adapter.sh start; \
	set -a; source "$$state"; set +a; \
	go test -race -tags=integration -count=1 -timeout=15m ./...
