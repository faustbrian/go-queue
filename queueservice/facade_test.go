package queueservice_test

import (
	"context"
	"errors"
	"testing"

	queue "github.com/faustbrian/go-queue"
	successor "github.com/faustbrian/go-queue/adapters/service"
	"github.com/faustbrian/go-queue/core"
	legacy "github.com/faustbrian/go-queue/queueservice"
)

type panickingWorker struct{}

func (*panickingWorker) Run(context.Context, core.TaskMessage) error { return nil }
func (*panickingWorker) Queue(core.TaskMessage) error                { return nil }
func (*panickingWorker) Request() (core.TaskMessage, error)          { return nil, nil }
func (*panickingWorker) Shutdown() error                             { panic("sensitive") }

func TestLegacySentinelsPreserveSuccessorIdentity(t *testing.T) {
	if !errors.Is(legacy.ErrInvalidOptions, successor.ErrInvalidOptions) ||
		!errors.Is(legacy.ErrUnavailable, successor.ErrUnavailable) ||
		!errors.Is(legacy.ErrCallbackPanic, successor.ErrCallbackPanic) {
		t.Fatal("legacy sentinels do not preserve successor identity")
	}
}

func TestLegacyConstructorsPreserveSuccessorValidation(t *testing.T) {
	if handler, err := legacy.NewHandler(legacy.HandlerOptions{}); handler != nil || err == nil {
		t.Fatalf("NewHandler() = (%v, %v), want nil handler and validation error", handler, err)
	} else {
		var optionsError *legacy.OptionsError
		if !errors.As(err, &optionsError) {
			t.Fatalf("NewHandler() error %T does not preserve legacy OptionsError", err)
		}
	}
	if worker, err := legacy.NewWorker(legacy.WorkerOptions{}); worker != nil || err == nil {
		t.Fatalf("NewWorker() = (%v, %v), want nil worker and validation error", worker, err)
	}
	if producer, err := legacy.NewProducer(legacy.ProducerOptions[int]{}); producer != nil || err == nil {
		t.Fatalf("NewProducer() = (%v, %v), want nil producer and validation error", producer, err)
	}
	if worker, err := legacy.NewLifecycleWorker(legacy.LifecycleWorkerOptions[int]{}); worker != nil || err == nil {
		t.Fatalf("NewLifecycleWorker() = (%v, %v), want nil worker and validation error", worker, err)
	}
}

func TestLegacyWorkerStopPreservesJoinedShutdownClassifications(t *testing.T) {
	coordinator, err := queue.NewQueue(
		queue.WithWorker(new(panickingWorker)),
		queue.WithWorkerCount(0),
	)
	if err != nil {
		t.Fatalf("queue.NewQueue() error = %v", err)
	}
	worker, err := legacy.NewWorker(legacy.WorkerOptions{Name: "worker", Queue: coordinator})
	if err != nil {
		t.Fatalf("NewWorker() error = %v", err)
	}
	if worker.Queue() != coordinator {
		t.Fatal("Queue() did not preserve coordinator")
	}
	err = worker.Component().Stop(t.Context())
	if !errors.Is(err, queue.ErrWorkerShutdownPanic) {
		t.Fatal("Stop() lost ErrWorkerShutdownPanic")
	}
	var callback *legacy.CallbackError
	if !errors.As(err, &callback) || callback.Operation != legacy.CallbackShutdown {
		t.Fatalf("Stop() callback = %#v", callback)
	}
	var panicError *legacy.CallbackPanicError
	if !errors.As(err, &panicError) || panicError.Operation != legacy.CallbackShutdown {
		t.Fatalf("Stop() panic = %#v", panicError)
	}
}
