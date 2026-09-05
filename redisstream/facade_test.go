package redisdb_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redisstream "github.com/faustbrian/go-queue/adapters/redisstream"
	"github.com/faustbrian/go-queue/core"
	"github.com/faustbrian/go-queue/job"
	"github.com/faustbrian/go-queue/management"
	legacy "github.com/faustbrian/go-queue/redisstream"
	"github.com/stretchr/testify/require"
)

func TestLegacyFacadePreservesContracts(t *testing.T) {
	var _ core.Worker = (*legacy.Worker)(nil)
	var _ management.Controller = (*legacy.Worker)(nil)
	var _ management.RecordReader = (*legacy.Worker)(nil)
	var _ management.StatusProvider = (*legacy.Worker)(nil)

	require.Same(t, redisstream.ErrManagementControlDisabled, legacy.ErrManagementControlDisabled)
	require.Same(t, redisstream.ErrManagementStatusDisabled, legacy.ErrManagementStatusDisabled)
	require.Same(t, redisstream.ErrInvalidManagementStatus, legacy.ErrInvalidManagementStatus)
}

func TestLegacyOptionsDelegateToSuccessor(t *testing.T) {
	options := []legacy.Option{
		legacy.WithAddr("localhost:6379"),
		legacy.WithBlockTime(time.Second),
		legacy.WithCluster(),
		legacy.WithCommandTimeout(time.Second),
		legacy.WithConnectTimeout(time.Second),
		legacy.WithConnectionString("redis://localhost:6379"),
		legacy.WithConsumer("consumer"),
		legacy.WithDB(1),
		legacy.WithDeadLetter("dead", 2),
		legacy.WithFailureStream("failed"),
		legacy.WithGroup("group"),
		legacy.WithLogger(nil),
		legacy.WithManagementStatus(management.StatusMetadata{}),
		legacy.WithMaxLength(1),
		legacy.WithPassword("password"),
		legacy.WithReclaim(time.Second, time.Second, 1),
		legacy.WithRecordRetention(1),
		legacy.WithReplayDestinations("replay"),
		legacy.WithRequestTimeout(time.Second),
		legacy.WithRunFunc(func(context.Context, core.TaskMessage) error { return nil }),
		legacy.WithSkipTLSVerify(),
		legacy.WithStreamName("stream"),
		legacy.WithTLS(),
		legacy.WithUsername("username"),
	}
	for index, option := range options {
		if option == nil {
			t.Fatalf("legacy option %d is nil", index)
		}
	}
	if worker, err := legacy.NewWorkerE(options...); worker != nil || err == nil {
		t.Fatalf("NewWorkerE(all options) = (%v, %v), want rejected configuration", worker, err)
	}
}

func TestLegacyConstructorsPreserveValidation(t *testing.T) {
	if worker, err := legacy.NewWorkerE(); worker != nil || err == nil {
		t.Fatalf("NewWorkerE() = (%v, %v), want nil worker and validation error", worker, err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("NewWorker() did not preserve successor panic contract")
		}
	}()
	legacy.NewWorker()
}

func TestLegacyWorkerDelegatesRuntimeAndManagementContracts(t *testing.T) {
	server := miniredis.RunT(t)
	runCalled := false
	worker := legacy.NewWorker(
		legacy.WithAddr(server.Addr()),
		legacy.WithRequestTimeout(10*time.Millisecond),
		legacy.WithRunFunc(func(context.Context, core.TaskMessage) error {
			runCalled = true
			return nil
		}),
	)
	task := job.NewTask(func(context.Context) error { return nil })

	if worker.BackendName() != "redis-streams" || worker.QueueName() == "" {
		t.Fatalf("unexpected backend identity: %q %q", worker.BackendName(), worker.QueueName())
	}
	if err := worker.Queue(&task); err != nil {
		t.Fatalf("Queue() error = %v", err)
	}
	delivery, err := worker.Request()
	if err != nil {
		t.Fatalf("Request() error = %v", err)
	}
	if err := worker.Run(t.Context(), delivery); err != nil || !runCalled {
		t.Fatalf("Run() = %v, called = %v", err, runCalled)
	}
	if _, err := worker.Stats(t.Context()); err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	if _, err := worker.Execute(t.Context(), management.Command{}); err == nil {
		t.Fatal("Execute() unexpectedly enabled management")
	}
	if _, err := worker.ListFailures(t.Context(), management.PageRequest{}); err == nil {
		t.Fatal("ListFailures() accepted invalid request")
	}
	if _, err := worker.ListDeadLetters(t.Context(), management.PageRequest{}); err == nil {
		t.Fatal("ListDeadLetters() accepted invalid request")
	}
	if _, err := worker.Inspect(t.Context(), management.InspectRequest{}); err == nil {
		t.Fatal("Inspect() accepted invalid request")
	}
	if _, err := worker.ObserveWorker(t.Context()); err == nil {
		t.Fatal("ObserveWorker() unexpectedly enabled management")
	}
	if _, err := worker.ObserveQueue(t.Context()); err == nil {
		t.Fatal("ObserveQueue() unexpectedly enabled management")
	}
	if err := worker.Shutdown(); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}
