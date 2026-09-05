SHELL := /usr/bin/env bash

.PHONY: integration

integration:
	@set -euo pipefail; \
	state_root="$${RUNNER_TEMP:-$${TMPDIR:-/tmp}}"; \
	state="$$(mktemp "$${state_root}/go-queue-rabbitmq-state.XXXXXX")"; \
	cleanup() { \
		status="$$1"; \
		trap - EXIT HUP INT TERM; \
		if task_root="$$(jq -er '.task_root' "$$state" 2>/dev/null)"; then \
			if ! RABBITMQ_ADAPTER_TASK_ROOT="$$task_root" ../../.verification/fixtures/rabbitmq-adapter.sh stop; then \
				status=1; \
			fi; \
		fi; \
		find "$$state" -type f -delete; \
		exit "$$status"; \
	}; \
	trap 'cleanup $$?' EXIT; \
	trap 'cleanup 129' HUP; \
	trap 'cleanup 130' INT; \
	trap 'cleanup 143' TERM; \
	RABBITMQ_ADAPTER_STATE_FILE="$$state" ../../.verification/fixtures/rabbitmq-adapter.sh start; \
	task_root="$$(jq -er '.task_root' "$$state")"; \
	live_config="$$(jq -er '.live_config' "$$state")"; \
	RABBITMQ_ADAPTER_TASK_ROOT="$$task_root" \
	RABBITMQ_ADAPTER_LIVE_CONFIG="$$live_config" \
		go test -race -tags=integration -count=1 -timeout=15m ./...
