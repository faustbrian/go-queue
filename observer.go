package queue

import (
	"time"

	"github.com/faustbrian/go-queue/management"
)

// EventKind identifies an observable queue lifecycle transition.
type EventKind string

const (
	// EventEnqueued reports successful submission, not handler completion.
	EventEnqueued EventKind = "enqueued"
	// EventHandlerStarted reports the start of a tracked work wrapper.
	EventHandlerStarted EventKind = "handler_started"
	// EventRetryScheduled reports a local handler retry delay.
	EventRetryScheduled EventKind = "retry_scheduled"
	// EventHandlerSucceeded reports success after handler and required settlement.
	EventHandlerSucceeded EventKind = "handler_succeeded"
	// EventHandlerFailed reports final work/settlement failure.
	EventHandlerFailed EventKind = "handler_failed"
	// EventAcknowledged reports successful explicit success settlement.
	EventAcknowledged EventKind = "acknowledged"
	// EventAckFailed reports failed success settlement with possible ambiguous effects.
	EventAckFailed EventKind = "ack_failed"
	// EventRejected reports successful explicit failure settlement.
	EventRejected EventKind = "rejected"
	// EventRejectFailed reports failed failure settlement.
	EventRejectFailed EventKind = "reject_failed"
	// EventShutdownStarted reports entry into shutdown.
	EventShutdownStarted EventKind = "shutdown_started"
	// EventShutdownCompleted reports worker shutdown return, before Wait necessarily completes.
	EventShutdownCompleted EventKind = "shutdown_completed"
)

// Event describes a queue or backend lifecycle transition.
type Event struct {
	// Kind selects the observed transition.
	Kind EventKind
	// Backend is a best-effort backend label, not authenticated identity.
	Backend string
	// Queue is a best-effort logical queue label; Ring leaves it empty.
	Queue string
	// OccurredAt is local event time, not a distributed ordering clock.
	OccurredAt time.Time
	// Duration is measured elapsed work when supplied; zero can mean unavailable.
	Duration time.Duration
	// RetryRemaining counts remaining additional handler attempts, not broker deliveries.
	RetryRemaining int64
	// RetryDelay reports the scheduled local retry delay.
	RetryDelay time.Duration
	// Depth reports Ring depth for enqueue; other events may leave zero.
	Depth int64
	// JobAge is best-effort age when supplied, or zero when unavailable.
	JobAge time.Duration
	// Err retains the classifiable error, which may include sensitive caller text.
	Err error
	// Classification is the selected backend-neutral disposition.
	Classification management.Classification
	// FailureCode is an operational label, not arbitrary error text.
	FailureCode string
}

// Observer receives synchronous, potentially concurrent queue events. Queue
// contains observer panics, but cannot preempt a blocking observer or redact
// caller errors. Observers must return promptly and control their own logging.
type Observer interface {
	// Observe receives synchronous events, potentially concurrently from multiple workers.
	Observe(Event)
}

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(Event)

// Observe calls the wrapped observer function.
func (f ObserverFunc) Observe(event Event) {
	f(event)
}

type emptyObserver struct{}

func (emptyObserver) Observe(Event) {}
