GO ?= go
BENCH_TIME ?= 100ms

.PHONY: benchmark

benchmark:
	$(GO) test $$( $(GO) list ./... | grep -v '/redisdb$$' ) \
		-run '^$$' -bench . -benchmem -benchtime="$(BENCH_TIME)"
	$(GO) test ./redisdb -run '^$$' \
		-bench '^BenchmarkRedis(Enqueue|Consume|Retry)$$' \
		-benchmem -benchtime="$(BENCH_TIME)"
	$(GO) test ./redisdb -run '^$$' -bench '^BenchmarkRedisShutdown$$' \
		-benchmem -benchtime=1x
