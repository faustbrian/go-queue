package queue

import "errors"

var (
	// ErrNoTaskInQueue is returned by Worker.Request() when the queue is currently empty.
	// This is a temporary condition - new tasks may be added later.
	// The queue scheduler uses this error to determine when to retry or wait for notifications.
	ErrNoTaskInQueue = errors.New("golang-queue: no task in queue")

	// ErrQueueHasBeenClosed is returned by Worker.Request() during/after shutdown when no tasks remain.
	// It describes intake closure, not proof that handlers or side effects finished.
	ErrQueueHasBeenClosed = errors.New("golang-queue: queue has been closed")

	// ErrMaxCapacity reports rejected admission to a full Ring or bounded stream.
	// The default Ring is bounded; backend stream capacity is configured separately.
	ErrMaxCapacity = errors.New("golang-queue: maximum size limit reached")

	// ErrInvalidConfiguration reports unsafe queue scheduler configuration.
	ErrInvalidConfiguration = errors.New("golang-queue: invalid configuration")

	// ErrHandlerPanic reports a recovered handler panic without exposing the
	// arbitrary panic value across logs, observations, or durable settlement.
	ErrHandlerPanic = errors.New("golang-queue: handler panic")

	// ErrSettlementPanic reports a recovered backend settlement callback panic
	// without exposing its arbitrary panic value.
	ErrSettlementPanic = errors.New("golang-queue: settlement panic")

	// ErrWorkerShutdownPanic reports a recovered concrete worker shutdown panic
	// without exposing or retaining its arbitrary panic value.
	ErrWorkerShutdownPanic = errors.New("golang-queue: worker shutdown panic")
)
