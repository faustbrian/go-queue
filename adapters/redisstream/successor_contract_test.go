package redisstream_test

import (
	"context"
	"testing"

	"github.com/faustbrian/go-queue/adapters/redisstream"
	"github.com/faustbrian/go-queue/core"
)

func TestSuccessorExposesUnambiguousRedisStreamWorkerAPI(t *testing.T) {
	options := []redisstream.Option{
		redisstream.WithAddr("127.0.0.1:6379"),
		redisstream.WithStreamName("jobs"),
		redisstream.WithGroup("workers"),
		redisstream.WithConsumer("worker-1"),
		redisstream.WithRunFunc(func(context.Context, core.TaskMessage) error { return nil }),
	}
	if len(options) != 5 {
		t.Fatalf("option count = %d, want 5", len(options))
	}
}
