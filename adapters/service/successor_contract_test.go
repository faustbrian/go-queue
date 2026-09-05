package queueservice_test

import (
	"context"
	"testing"

	queueservice "github.com/faustbrian/go-queue/adapters/service"
	"github.com/faustbrian/go-queue/core"
)

func TestSuccessorExposesQueueServiceLifecycleAPI(t *testing.T) {
	var handler queueservice.Handler = func(context.Context, core.TaskMessage) error { return nil }
	var options queueservice.ProducerOptions[int]
	if handler == nil || options.Name != "" {
		t.Fatal("successor public lifecycle types are unusable")
	}
}
