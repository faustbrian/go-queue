#!/usr/bin/env bash
set -euo pipefail

fuzz_time=${FUZZ_TIME:-2s}
targets=(
	"job:FuzzDecodeE"
	"job:FuzzMessageValidation"
	"nats:FuzzRequestDelivery"
	"nsq:FuzzRequestDelivery"
	"rabbitmq:FuzzRequestDelivery"
	"redisdb:FuzzRequestDelivery"
	"redisstream:FuzzRequestDelivery"
)

for target in "${targets[@]}"; do
	package=${target%%:*}
	name=${target#*:}
	go test "./$package" -run '^$' -fuzz "^${name}$" -fuzztime "$fuzz_time"
done
