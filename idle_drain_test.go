package queue

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/faustbrian/go-queue/core"
	"github.com/faustbrian/go-queue/job"
	"github.com/faustbrian/go-queue/management"
)

func TestIdleDrainDoesNotWaitForRetryInterval(t *testing.T) {
	// Check bounded external pacing first: a broken scheduler must fail
	// before the real Ring variant can prevent synctest quiescence.
	for _, kind := range []string{"external", "managed-external", "ring"} {
		if !t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var q *Queue
				var worker *idleDrainWorker
				if kind == "ring" {
					q = NewPool(1, WithRetryInterval(time.Hour))
				} else {
					worker = &idleDrainWorker{shutdown: make(chan struct{})}
					var err error
					opts := []Option{WithWorker(worker), WithWorkerCount(1), WithRetryInterval(time.Hour)}
					if kind == "managed-external" {
						opts = append(opts, WithWorkerLifecycle(managedQueueLifecycle(t)))
					}
					q, err = NewQueue(opts...)
					if err != nil {
						t.Fatal(err)
					}
					q.Start()
				}
				defer func() { q.Shutdown(); q.Wait() }()

				// Park the scheduler's empty request in its retry wait without
				// advancing the fake clock or adding a production synchronization seam.
				synctest.Wait()
				var requests int64
				if worker != nil {
					requests = worker.requests.Load()
					if requests != 1 {
						t.Fatalf("idle requests = %d, want 1", requests)
					}
				}
				start := time.Now()
				if err := q.CloseAdmission(); err != nil {
					t.Fatal(err)
				}
				if err := q.QueueTask(func(context.Context) error { return nil }); !errors.Is(err, ErrQueueShutdown) {
					t.Fatalf("late submission error = %v", err)
				}
				if worker != nil && worker.closed {
					t.Fatal("admission closure released worker")
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := q.ReleaseContext(ctx); err != nil {
					t.Fatalf("ReleaseContext() error = %v after %v; idle drain must not wait for its retry interval", err, time.Since(start))
				}
				if elapsed := time.Since(start); elapsed != 0 {
					t.Fatalf("idle release advanced time by %v", elapsed)
				}
				if q.BusyWorkers() != 0 {
					t.Fatal("idle release retained busy workers")
				}
				if worker != nil && !worker.closed {
					t.Fatal("release left worker open")
				}
				if worker != nil && worker.requests.Load() != requests {
					t.Fatal("drain requested new external work after admission closed")
				}
			})
		}) {
			return
		}
	}
}

// The fake supplies a returned empty Request, not a blocked backend request.
type idleDrainWorker struct {
	controlledWorker
	closed   bool
	shutdown chan struct{}
}

func (w *idleDrainWorker) Request() (core.TaskMessage, error) {
	if w.requests.Add(1) == 1 {
		return nil, ErrNoTaskInQueue
	}
	// An unexpected repeated request parks instead of starving the test;
	// the caller can assert its count and still join the scheduler.
	<-w.shutdown
	return nil, errors.New("idle drain fixture closed")
}

func (w *idleDrainWorker) Shutdown() error {
	if !w.closed {
		w.closed = true
		close(w.shutdown)
	}
	return nil
}

func TestDrainRetainsTaskAlreadyWaitingAfterRequestError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		handled := 0
		message := job.NewTask(func(ctx context.Context) error {
			if ctx.Err() != nil {
				t.Errorf("drain canceled accepted task: %v", ctx.Err())
			}
			handled++
			return nil
		})
		worker := newTaskErrorContractWorker(&message)
		q, err := NewQueue(WithWorker(worker), WithWorkerCount(1), WithRetryInterval(time.Hour), WithLogger(NewEmptyLogger()))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { q.Shutdown(); q.Wait() }()
		q.Start()
		synctest.Wait()
		if worker.delivered.Load() != 1 || handled != 0 {
			t.Fatal("request did not retain the unhandled task before drain")
		}
		if err := q.CloseAdmission(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-worker.shutdown:
			t.Fatal("admission closure released accepted task")
		default:
		}
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := q.ReleaseContext(ctx); err != nil {
			t.Fatalf("ReleaseContext() error = %v", err)
		}
		if handled != 1 {
			t.Fatalf("accepted task executions = %d, want 1", handled)
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Fatalf("accepted task drain waited %v", elapsed)
		}
		select {
		case <-worker.shutdown:
		default:
			t.Fatal("worker remained open after accepted task completed")
		}
	})
}

func (*idleDrainWorker) ObserveWorker(ctx context.Context) (management.WorkerStatus, error) {
	return (&managedQueueWorker{}).ObserveWorker(ctx)
}

func (*idleDrainWorker) ObserveQueue(ctx context.Context) (management.QueueStatus, error) {
	return (&managedQueueWorker{}).ObserveQueue(ctx)
}
