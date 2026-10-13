// Package queue schedules in-process tasks and backend deliveries with bounded
// worker concurrency and per-delivery retries. Backend acceptance, settlement and
// durability are distinct from handler completion; none promises exactly-once
// application effects. Handlers must cooperate with their contexts.
//
// Construct queues with NewQueue or NewPool. CloseAdmission stops new submissions
// without closing the worker; ReleaseContext drains tracked scheduling/work
// wrappers before worker shutdown. A timed-out handler can outlive those wrappers
// when it ignores cancellation. Message payload bytes are not copied on admission.
package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/faustbrian/go-queue/core"
	"github.com/faustbrian/go-queue/job"
	"github.com/faustbrian/go-queue/management"

	"github.com/jpillora/backoff"
)

/*
ErrQueueShutdown is returned when an operation is attempted on a queue
that has already been closed and released.
*/
var ErrQueueShutdown = errors.New("queue has been closed and released")

type (
	// Queue represents a message queue with worker management, job scheduling,
	// retry logic, and graceful shutdown capabilities.
	Queue struct {
		sync.Mutex                   // Mutex to protect concurrent access to queue state
		metric         Metric        // Metrics collector for tracking queue and worker stats
		logger         Logger        // Logger for queue events and errors
		workerCount    int64         // Number of worker goroutines to process jobs
		routineGroup   *routineGroup // Group to manage and wait for goroutines
		quit           chan struct{} // Channel to signal shutdown to all goroutines
		drain          chan struct{} // Channel to signal graceful intake withdrawal
		ready          chan struct{} // Channel to signal worker readiness
		notify         chan struct{} // Channel to notify workers of new jobs
		worker         core.Worker   // The worker implementation that processes jobs
		startOnce      sync.Once     // Ensures the scheduler starts only once
		stopOnce       sync.Once     // Ensures shutdown is only performed once
		drainOnce      sync.Once     // Ensures graceful intake stops only once
		admissionMu    sync.Mutex    // Serializes admission state and counters
		admissions     int           // Active accepted enqueue operations
		admissionsDone chan struct{} // Closed after graceful admission drains
		activeWorkers  int64         // Authoritative internal worker count
		started        int32         // Atomic flag indicating the scheduler has started
		stopFlag       int32         // Atomic flag indicating if shutdown has started
		drainFlag      int32         // Atomic flag indicating graceful drain
		shutdownErr    error         // Concrete worker shutdown failure
		afterFn        func()        // Optional callback after each job execution
		observer       Observer      // Queue lifecycle observer
		retryInterval  time.Duration // Interval for retrying job requests
		lifecycle      *management.WorkerLifecycle
	}
)

/*
ErrMissingWorker is returned when a queue is created without a worker implementation.
*/
var ErrMissingWorker = errors.New("missing worker module")

// NewQueue validates scheduler options and retains the supplied worker, logger,
// metric, observer and lifecycle. It does not start the scheduler or assume a
// worker has no constructor-owned background work. Use Start and release the
// queue when finished. A typed-nil Worker is not detected by the interface check.
func NewQueue(opts ...Option) (*Queue, error) {
	o := NewOptions(opts...)
	q := &Queue{
		routineGroup:  newRoutineGroup(),      // Tracks the scheduler and work wrappers, not the inner handler goroutine
		quit:          make(chan struct{}),    // Signals shutdown to all goroutines
		drain:         make(chan struct{}),    // Signals graceful intake withdrawal
		ready:         make(chan struct{}, 1), // Signals when a worker is ready to process a job
		notify:        make(chan struct{}, 1), // Notifies workers of new jobs
		workerCount:   o.workerCount,          // Number of worker goroutines
		logger:        o.logger,               // Logger for queue events
		worker:        o.worker,               // Worker implementation
		metric:        o.metric,               // Metrics collector
		afterFn:       o.afterFn,              // Optional post-job callback
		retryInterval: o.retryInterval,        // Interval for retrying job requests
		observer:      o.observer,             // Lifecycle event observer
		lifecycle:     o.lifecycle,
	}

	if q.worker == nil {
		return nil, ErrMissingWorker
	}
	if o.lifecycleConfigured && (q.lifecycle == nil || !workerProvidesStatus(q.worker)) {
		return nil, ErrInvalidManagementLifecycle
	}
	if o.retryInterval <= 0 {
		return nil, fmt.Errorf("%w: retry interval must be positive", ErrInvalidConfiguration)
	}
	if o.queueSize < 0 {
		return nil, fmt.Errorf("%w: queue size cannot be negative", ErrInvalidConfiguration)
	}

	return q, nil
}

// Start launches the intake scheduler once. Work goroutines are created as
// capacity becomes available. It does nothing after shutdown or when the current
// worker count is zero; constructor options normally select runtime.NumCPU.
func (q *Queue) Start() {
	if atomic.LoadInt32(&q.stopFlag) == 1 {
		return
	}
	q.Lock()
	count := q.workerCount
	q.Unlock()
	if count == 0 {
		return
	}
	q.startOnce.Do(func() {
		atomic.StoreInt32(&q.started, 1)
		q.routineGroup.Run(func() {
			q.start()
		})
	})
}

// Shutdown rejects submissions, calls the concrete worker's synchronous
// Shutdown, then signals tracked work to cancel. It is idempotent but does not
// join work or bound Worker.Shutdown. Ring shutdown waits for buffer dequeue.
// Use ReleaseContext for the separate drain-before-close path.
func (q *Queue) Shutdown() {
	q.stopOnce.Do(func() {
		q.admissionMu.Lock()
		atomic.StoreInt32(&q.stopFlag, 1)
		q.admissionMu.Unlock()
		q.observe(Event{Kind: EventShutdownStarted})
		if busy := q.BusyWorkers(); busy > 0 {
			q.safeLogInfof("shutdown all tasks: %d workers", busy)
		}

		if _, inMemory := q.worker.(*Ring); !inMemory ||
			atomic.LoadInt32(&q.drainFlag) == 0 {
			q.shutdownErr = q.shutdownWorker()
		}
		if q.shutdownErr != nil {
			q.safeLogError("worker shutdown failed")
		}
		close(q.quit)
		q.observe(Event{Kind: EventShutdownCompleted})
	})
}

func (q *Queue) shutdownWorker() (err error) {
	defer func() {
		if recover() != nil {
			err = ErrWorkerShutdownPanic
		}
	}()

	return q.worker.Shutdown()
}

// Release starts an unstarted Ring-backed queue, calls Shutdown and waits for
// the scheduler/work wrappers without a caller deadline. Timeout-ignoring handler
// goroutines are not joined; this is not a graceful drain-before-close guarantee.
func (q *Queue) Release() {
	if _, isRing := q.worker.(*Ring); isRing &&
		atomic.LoadInt32(&q.started) == 0 {
		q.Start()
	}
	q.Shutdown()
	q.Wait()
}

// CloseAdmission synchronously and idempotently rejects new queue submissions
// and withdraws new backend intake. An already accepted submission/request or
// work wrapper can remain active. It does not cancel handlers, close transports
// or join a handler that ignores its own timeout.
func (q *Queue) CloseAdmission() error {
	q.drainOnce.Do(func() {
		q.admissionMu.Lock()
		atomic.StoreInt32(&q.drainFlag, 1)
		q.admissionsDone = make(chan struct{})
		if q.admissions == 0 {
			close(q.admissionsDone)
		}
		q.admissionMu.Unlock()
		if ring, inMemory := q.worker.(*Ring); inMemory {
			_ = ring.stopIntake()
			// Wake an idle request while retaining queued Ring work.
			select {
			case q.notify <- struct{}{}:
			default:
			}
		} else {
			close(q.drain)
			q.releaseReadyAdmission()
		}
	})

	return nil
}

// ReleaseContext closes admission, waits for accepted publishes and tracked
// scheduler/work wrappers, then shuts down the worker. The context bounds drain
// waits, not synchronous Worker.Shutdown or noncooperative callbacks. Expiration
// before shutdown leaves the worker open for a later release attempt. A handler
// that ignores its own timeout can continue after its tracked wrapper completes.
// A nil context returns context.Canceled; shutdown errors remain cached.
func (q *Queue) ReleaseContext(ctx context.Context) error {
	if ctx == nil {
		return context.Canceled
	}
	if err := q.drainContext(ctx); err != nil {
		return err
	}
	q.Shutdown()

	return errors.Join(q.shutdownErr, q.WaitContext(ctx))
}

// BusyWorkers returns the internal active work-wrapper count, independent of
// custom metrics. Timed-out handlers still running are not counted.
func (q *Queue) BusyWorkers() int64 {
	return atomic.LoadInt64(&q.activeWorkers)
}

// SuccessTasks returns the number of successfully completed tasks.
func (q *Queue) SuccessTasks() uint64 {
	return q.safeMetricValue("read successful task count", q.metric.SuccessTasks)
}

// FailureTasks returns the number of failed tasks.
func (q *Queue) FailureTasks() uint64 {
	return q.safeMetricValue("read failed task count", q.metric.FailureTasks)
}

// SubmittedTasks returns the number of tasks submitted to the queue.
func (q *Queue) SubmittedTasks() uint64 {
	return q.safeMetricValue("read submitted task count", q.metric.SubmittedTasks)
}

// CompletedTasks returns the total number of completed tasks (success + failure).
func (q *Queue) CompletedTasks() uint64 {
	return q.safeMetricValue("read completed task count", q.metric.CompletedTasks)
}

// Wait joins the current scheduler/work-wrapper group; it does not close intake,
// join canceled inner handler goroutines or prevent later work from being added.
func (q *Queue) Wait() {
	q.routineGroup.Wait()
}

// WaitContext bounds the current scheduler/work-wrapper wait. It does not cancel
// work or join timeout-ignoring handlers; a nil context returns context.Canceled.
func (q *Queue) WaitContext(ctx context.Context) error {
	return q.routineGroup.WaitContext(ctx)
}

// Queue borrows message.Bytes(), applies only the first job option, validates
// execution metadata and synchronously invokes Worker.Queue. Success is backend
// admission, not completed execution. There is no caller context; a blocked
// backend publish also holds an admission until it returns.
func (q *Queue) Queue(message core.QueuedMessage, opts ...job.AllowOption) error {
	data := job.NewMessage(message, opts...)

	return q.queue(&data)
}

// QueueTask admits an in-process callback using only the first job option.
// Task functions are not serialized, so use a Ring for actual local execution.
// The callback must honor the message's total timeout; admission does not copy
// application-owned state captured by it.
func (q *Queue) QueueTask(task job.TaskFunc, opts ...job.AllowOption) error {
	data := job.NewTask(task, opts...)
	return q.queue(&data)
}

// queue is an internal helper to enqueue a job.Message into the worker.
// It increments the submitted task metric and notifies workers if possible.
func (q *Queue) queue(m *job.Message) error {
	if !q.beginAdmission() {
		return ErrQueueShutdown
	}
	defer q.endAdmission()
	if err := m.Validate(); err != nil {
		return err
	}

	if err := q.worker.Queue(m); err != nil {
		return err
	}

	q.safeMetricUpdate("increment submitted task count", q.metric.IncSubmittedTask)
	q.observe(Event{Kind: EventEnqueued})
	// Notify a worker that a new job is available.
	// If the notify channel is full, the worker is busy and we avoid blocking.
	select {
	case q.notify <- struct{}{}:
	default:
	}

	return nil
}

// work executes a single task, handling panics and updating metrics accordingly.
// After execution, it schedules the next worker if needed.
func (q *Queue) work(task core.TaskMessage) {
	var err error
	startedAt := time.Now()
	q.observe(Event{Kind: EventHandlerStarted})
	// Defer block to handle panics, update metrics, and run afterFn callback.
	defer func() {
		atomic.AddInt64(&q.activeWorkers, -1)
		q.safeMetricUpdate("decrement busy worker count", q.metric.DecBusyWorker)
		e := recover()
		if e != nil {
			panicFailure := management.NewFailure(
				management.ClassificationPermanent,
				"handler_panic",
				ErrHandlerPanic,
			)
			err = q.settle(task, panicFailure)
			q.safeLogError(ErrHandlerPanic)
		}
		q.schedule()

		// Update success or failure metrics based on execution result.
		if err == nil && e == nil {
			q.safeMetricUpdate("increment successful task count", q.metric.IncSuccessTask)
			q.observe(Event{Kind: EventHandlerSucceeded, Duration: time.Since(startedAt)})
		} else {
			q.safeMetricUpdate("increment failed task count", q.metric.IncFailureTask)
			q.observe(failureEvent(Event{
				Kind: EventHandlerFailed, Duration: time.Since(startedAt), Err: err,
			}, err))
		}
		if q.afterFn != nil {
			if panicValue := invokeSafely(q.afterFn); panicValue != nil {
				q.safeLogError("after callback panic")
			}
		}
	}()

	if err = q.run(task); err != nil {
		err = normalizeHandlerFailure(err)
		q.safeLogError(err)
	}
	err = q.settle(task, err)
}

func (q *Queue) workManaged(task core.TaskMessage, tracked bool) {
	if tracked {
		defer func() {
			_ = q.lifecycle.EndJob()
		}()
	}
	q.work(task)
}

func (q *Queue) settle(task core.TaskMessage, handlerErr error) error {
	delivery, ok := task.(core.Acknowledger)
	if !ok || !delivery.AcknowledgementRequired() {
		return handlerErr
	}
	if handlerErr == nil {
		if err := invokeSettlement("acknowledge delivery", delivery.Ack); err != nil {
			failure := management.NewFailure(
				management.ClassificationInfrastructure,
				"acknowledgement_failed",
				err,
			)
			q.observe(failureEvent(Event{Kind: EventAckFailed, Err: failure}, failure))
			return failure
		}
		q.observe(Event{Kind: EventAcknowledged})
		return nil
	}
	reject := delivery.Nack
	if failureDelivery, supported := task.(core.FailureAcknowledger); supported {
		reject = func() error { return failureDelivery.NackFailure(handlerErr) }
	}
	if err := invokeSettlement("reject delivery", reject); err != nil {
		failure := management.NewFailure(
			management.ClassificationInfrastructure,
			"failure_settlement_failed",
			err,
		)
		q.observe(failureEvent(Event{Kind: EventRejectFailed, Err: failure}, failure))
		return errors.Join(handlerErr, failure)
	}
	q.observe(failureEvent(Event{Kind: EventRejected, Err: handlerErr}, handlerErr))
	return handlerErr
}

func invokeSettlement(operation string, settle func() error) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("%s: %w", operation, ErrSettlementPanic)
		}
	}()
	return settle()
}

// run dispatches the task to the appropriate handler based on its type.
// Returns an error if the task type is invalid.
func (q *Queue) run(task core.TaskMessage) error {
	switch t := task.(type) {
	case *job.Message:
		if validator, ok := q.worker.(core.DeliveryValidator); ok {
			if err := validator.ValidateDelivery(t); err != nil {
				return err
			}
		}
		return q.handle(t)
	default:
		return errors.New("invalid task type")
	}
}

// handle isolates one handler/retry loop behind buffered completion channels.
// One context budget covers every attempt and delay. Timeout returns without
// joining the inner goroutine, so accounting/settlement can finish while caller
// code still runs; cooperative cancellation is required, not enforced.
func (q *Queue) handle(m *job.Message) error {
	// done: receives the result of the job execution
	// panicChan: receives any panic that occurs in the job goroutine
	done := make(chan error, 1)
	panicChan := make(chan any, 1)
	startTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), m.Timeout)
	defer func() {
		cancel()
	}()

	// Run the job in a separate goroutine to support timeout and panic recovery.
	go func() {
		// Defer block to catch panics and send to panicChan
		defer func() {
			if p := recover(); p != nil {
				panicChan <- p
			}
		}()

		var err error

		// Set up backoff for retry logic
		b := &backoff.Backoff{
			Min:    m.RetryMin,
			Max:    m.RetryMax,
			Factor: m.RetryFactor,
			Jitter: m.Jitter,
		}
		delay := m.RetryDelay
	loop:
		for {
			// If a custom Task function is provided, use it; otherwise, use the worker's Run method.
			if m.Task != nil {
				err = m.Task(ctx)
			} else {
				err = q.worker.Run(ctx, m)
			}

			// Retry only recoverable handler failures. Permanent, malformed,
			// canceled, and infrastructure outcomes must reach backend
			// settlement without another handler execution.
			if err == nil || m.RetryCount == 0 ||
				management.ClassifyFailure(err) !=
					management.ClassificationRetryable {
				break
			}
			m.RetryCount--

			// If no fixed retry delay, use backoff.
			if m.RetryDelay == 0 {
				delay = b.Duration()
			}
			retryFailure := normalizeHandlerFailure(err)
			q.observe(failureEvent(Event{
				Kind:           EventRetryScheduled,
				RetryRemaining: m.RetryCount,
				RetryDelay:     delay,
				Err:            retryFailure,
			}, retryFailure))

			select {
			case <-time.After(delay): // Wait before retrying
				q.safeLogInfof("retry remaining times: %d, delay time: %s", m.RetryCount, delay)
			case <-ctx.Done(): // Timeout reached
				err = ctx.Err()
				break loop
			}
		}

		done <- err
	}()

	select {
	case p := <-panicChan:
		panic(p)
	case <-ctx.Done(): // Timeout reached
		return ctx.Err()
	case <-q.quit: // Queue is shutting down
		// Cancel job and wait for remaining time or job completion
		cancel()
		leftTime := time.Until(startTime.Add(m.Timeout))
		select {
		case <-time.After(leftTime):
			return context.DeadlineExceeded
		case err := <-done: // Job finished
			return err
		case p := <-panicChan:
			panic(p)
		}
	case err := <-done: // Job finished
		return err
	}
}

func normalizeHandlerFailure(err error) error {
	if err == nil {
		return nil
	}
	resolution := management.ResolveFailure(err)
	code := resolution.Code
	if code == "" {
		code = "handler_failed"
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		code = "handler_deadline_exceeded"
	case errors.Is(err, context.Canceled):
		code = "handler_canceled"
	}

	return management.NewFailure(resolution.Classification, code, err)
}

func failureEvent(event Event, err error) Event {
	resolution := management.ResolveFailure(err)
	event.Classification = resolution.Classification
	event.FailureCode = resolution.Code

	return event
}

func (q *Queue) observe(event Event) {
	panicValue := invokeSafely(func() {
		if metadata, ok := q.worker.(core.WorkerMetadata); ok {
			if event.Backend == "" {
				event.Backend = metadata.BackendName()
			}
			if event.Queue == "" {
				event.Queue = metadata.QueueName()
			}
		}
		if event.OccurredAt.IsZero() {
			event.OccurredAt = time.Now()
		}
		q.observer.Observe(event)
	})
	if panicValue != nil {
		q.safeLogError("observer panic")
	}
}

func invokeSafely(fn func()) (panicValue any) {
	defer func() { panicValue = recover() }()
	fn()
	return nil
}

func valueSafely[T any](fn func() T) (value T, panicValue any) {
	defer func() { panicValue = recover() }()
	return fn(), nil
}

func (q *Queue) safeMetricUpdate(operation string, update func()) {
	if panicValue := invokeSafely(update); panicValue != nil {
		q.safeLogErrorf("metric panic during %s", operation)
	}
}

func (q *Queue) safeMetricValue(operation string, read func() uint64) uint64 {
	value, panicValue := valueSafely(read)
	if panicValue != nil {
		q.safeLogErrorf("metric panic during %s", operation)
	}
	return value
}

func (q *Queue) safeLogInfof(format string, args ...any) {
	_ = invokeSafely(func() { q.logger.Infof(format, args...) })
}

func (q *Queue) safeLogErrorf(format string, args ...any) {
	_ = invokeSafely(func() { q.logger.Errorf(format, args...) })
}

func (q *Queue) safeLogError(args ...any) {
	_ = invokeSafely(func() { q.logger.Error(args...) })
}

// UpdateWorkerCount changes the admission concurrency threshold and schedules
// work. It does not stop existing handlers. Unlike WithWorkerCount, zero/negative
// values are stored as-is and prevent new work from being scheduled.
func (q *Queue) UpdateWorkerCount(num int64) {
	q.Lock()
	q.workerCount = num
	q.Unlock()
	q.schedule()
}

// schedule checks if more workers can be started based on the current busy count.
// If so, it signals readiness to start a new worker.
func (q *Queue) schedule() {
	q.Lock()
	defer q.Unlock()
	if atomic.LoadInt32(&q.drainFlag) == 1 {
		if _, inMemory := q.worker.(*Ring); !inMemory {
			return
		}
	}
	if q.BusyWorkers() >= q.workerCount {
		return
	}
	reserved := false
	if q.lifecycle != nil {
		if !q.lifecycle.BeginAdmission() {
			return
		}
		reserved = true
	}

	select {
	case q.ready <- struct{}{}:
	default:
		if reserved {
			_ = q.lifecycle.EndAdmission()
		}
	}
}

func (q *Queue) hasWorkerCapacity() bool {
	q.Lock()
	defer q.Unlock()

	return q.BusyWorkers() < q.workerCount
}

/*
start launches the main worker loop, which manages job scheduling and execution.

- It uses a ticker to periodically retry job requests if the queue is empty.
- For each available worker slot, it requests a new task from the worker.
- If a task is available, it is sent to the tasks channel and processed by a new goroutine.
- The loop exits when the quit channel is closed.
*/
func (q *Queue) start() {
	type admittedTask struct {
		task    core.TaskMessage
		tracked bool
	}
	tasks := make(chan admittedTask, 1)
	ticker := time.NewTicker(q.retryInterval)
	defer ticker.Stop()

	for {
		// Ensure the number of busy workers does not exceed the configured worker count.
		q.schedule()

		select {
		case <-q.ready: // Wait for a worker slot to become available
			if atomic.LoadInt32(&q.drainFlag) == 1 {
				if _, inMemory := q.worker.(*Ring); !inMemory {
					if q.lifecycle != nil {
						_ = q.lifecycle.EndAdmission()
					}
					return
				}
			}
			if !q.hasWorkerCapacity() {
				if q.lifecycle != nil {
					_ = q.lifecycle.EndAdmission()
				}
				continue
			}
		case <-q.quit: // Shutdown signal received
			return
		case <-q.drain:
			return
		}

		// Request a task from the worker in a background goroutine.
		q.routineGroup.Run(func() {
			for {
				t, err := q.worker.Request()
				tracked := false
				if q.lifecycle != nil {
					if t == nil {
						_ = q.lifecycle.EndAdmission()
					} else {
						_ = q.lifecycle.PromoteAdmissionToJob()
						tracked = true
					}
				}
				if t != nil {
					if err != nil && atomic.LoadInt32(&q.drainFlag) == 0 {
						select {
						case <-q.quit:
						case <-ticker.C:
						case <-q.notify:
						case <-q.drain:
						}
					}
					tasks <- admittedTask{task: t, tracked: tracked}
					return
				}
				if atomic.LoadInt32(&q.drainFlag) == 1 {
					tasks <- admittedTask{}
					return
				}
				if err != nil {
					select {
					case <-q.quit:
					case <-ticker.C:
					case <-q.notify:
					case <-q.drain:
					}
				}
				// A notification may race with closure; external intake still
				// stops before requesting another task. Ring backlog is owned.
				select {
				case <-q.drain:
					tasks <- admittedTask{}
					return
				default:
				}
				if q.lifecycle != nil {
					if !q.lifecycle.BeginAdmission() {
						tasks <- admittedTask{}
						return
					}
				}

				select {
				case <-q.quit:
					if !errors.Is(err, ErrNoTaskInQueue) {
						close(tasks)
						return
					}
				default:
				}
			}
		})

		admitted, ok := <-tasks
		if !ok {
			return
		}
		if admitted.task == nil {
			if atomic.LoadInt32(&q.drainFlag) == 1 {
				return
			}
		} else {
			// Start processing the new task in a separate goroutine.
			atomic.AddInt64(&q.activeWorkers, 1)
			q.safeMetricUpdate("increment busy worker count", q.metric.IncBusyWorker)
			q.routineGroup.Run(func() {
				q.workManaged(admitted.task, admitted.tracked)
			})
		}
	}
}

func (q *Queue) drainContext(ctx context.Context) error {
	ring, inMemory := q.worker.(*Ring)
	if inMemory &&
		atomic.LoadInt32(&q.started) == 0 {
		q.Start()
	}
	_ = q.CloseAdmission()
	q.admissionMu.Lock()
	admissionsDone := q.admissionsDone
	q.admissionMu.Unlock()
	select {
	case <-admissionsDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	if inMemory {
		select {
		case <-ring.exit:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return q.WaitContext(ctx)
}

func (q *Queue) releaseReadyAdmission() {
	select {
	case <-q.ready:
		if q.lifecycle != nil {
			_ = q.lifecycle.EndAdmission()
		}
	default:
	}
}

func (q *Queue) beginAdmission() bool {
	q.admissionMu.Lock()
	defer q.admissionMu.Unlock()
	if atomic.LoadInt32(&q.stopFlag) == 1 ||
		atomic.LoadInt32(&q.drainFlag) == 1 {
		return false
	}
	q.admissions++

	return true
}

func (q *Queue) endAdmission() {
	q.admissionMu.Lock()
	defer q.admissionMu.Unlock()
	q.admissions--
	if q.admissions == 0 && q.admissionsDone != nil {
		close(q.admissionsDone)
	}
}
