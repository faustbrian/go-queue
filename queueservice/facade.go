// Package queueservice preserves the released queue service import path.
//
// Deprecated: use github.com/faustbrian/go-queue/adapters/service.
package queueservice

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/faustbrian/go-correlation"
	queuecorrelation "github.com/faustbrian/go-correlation/queue"
	queue "github.com/faustbrian/go-queue"
	successor "github.com/faustbrian/go-queue/adapters/service"
	"github.com/faustbrian/go-queue/core"
	"github.com/faustbrian/go-queue/job"
	"github.com/faustbrian/go-service"
	"go.opentelemetry.io/otel/propagation"
)

// MaxNameBytes bounds component, task, and readiness identifiers.
const MaxNameBytes = successor.MaxNameBytes

// CallbackOperation identifies an application callback boundary.
type CallbackOperation uint8

const (
	// CallbackStartup identifies resource validation during service start.
	CallbackStartup CallbackOperation = 1
	// CallbackReadiness identifies an opt-in dependency readiness check.
	CallbackReadiness CallbackOperation = 2
	// CallbackPublish identifies concrete producer publication.
	CallbackPublish CallbackOperation = 3
	// CallbackHandler identifies application task handling.
	CallbackHandler CallbackOperation = 4
	// CallbackRun identifies supervised worker intake.
	CallbackRun CallbackOperation = 5
	// CallbackShutdown identifies transferred resource cleanup.
	CallbackShutdown CallbackOperation = 6
	// CallbackAdmission identifies synchronous worker intake closure.
	CallbackAdmission CallbackOperation = 7
)

// PublishAcceptance reports whether a failed publish reached the backend.
type PublishAcceptance uint8

const (
	// PublishNotAccepted means the backend definitively did not accept the task.
	PublishNotAccepted PublishAcceptance = 1
	// PublishAccepted means the backend definitively accepted the task.
	PublishAccepted PublishAcceptance = 2
	// PublishUnknown means the backend may have accepted the task.
	PublishUnknown PublishAcceptance = 3
)

var (
	// ErrInvalidOptions identifies invalid adapter construction.
	ErrInvalidOptions = successor.ErrInvalidOptions
	// ErrUnavailable reports an inactive or draining adapter.
	ErrUnavailable = successor.ErrUnavailable
	// ErrMissingCorrelation reports a publish without an explicit parent workflow.
	ErrMissingCorrelation = successor.ErrMissingCorrelation
	// ErrCallbackPanic reports a recovered application callback panic.
	ErrCallbackPanic = successor.ErrCallbackPanic
	// ErrPublishOutcomeUnknown reports a publish that may have reached the backend.
	ErrPublishOutcomeUnknown = successor.ErrPublishOutcomeUnknown
	// ErrInvalidPublishAcceptance reports a result outside the acceptance contract.
	ErrInvalidPublishAcceptance = successor.ErrInvalidPublishAcceptance
	// ErrWorkerExited reports a worker that returned before cancellation.
	ErrWorkerExited = successor.ErrWorkerExited
)

func compatibilityError(err error) error {
	if err == nil {
		return nil
	}
	for _, sentinel := range []struct {
		successor error
		legacy    error
	}{
		{successor: successor.ErrInvalidOptions, legacy: ErrInvalidOptions},
		{successor: successor.ErrUnavailable, legacy: ErrUnavailable},
		{successor: successor.ErrMissingCorrelation, legacy: ErrMissingCorrelation},
		{successor: successor.ErrCallbackPanic, legacy: ErrCallbackPanic},
		{successor: successor.ErrPublishOutcomeUnknown, legacy: ErrPublishOutcomeUnknown},
		{successor: successor.ErrInvalidPublishAcceptance, legacy: ErrInvalidPublishAcceptance},
		{successor: successor.ErrWorkerExited, legacy: ErrWorkerExited},
	} {
		if sameErrorIdentity(err, sentinel.successor) {
			return sentinel.legacy
		}
	}
	switch typed := err.(type) { //nolint:errorlint // Exact outer ownership must be preserved; errors.As would promote nested caller causes.
	case *successor.OptionsError:
		return &OptionsError{Field: typed.Field, Reason: typed.Reason}
	case *successor.StartupError:
		return &StartupError{
			Validation: compatibilityOwnedError(typed.Validation),
			Cleanup:    compatibilityOwnedError(typed.Cleanup),
		}
	case *successor.PublishError:
		return &PublishError{
			Acceptance: PublishAcceptance(typed.Acceptance),
			Err:        compatibilityOwnedError(typed.Err),
		}
	case *successor.CallbackPanicError:
		return &CallbackPanicError{Operation: CallbackOperation(typed.Operation)}
	case *successor.CallbackError:
		return &CallbackError{
			Operation: CallbackOperation(typed.Operation),
			Err:       typed.Err,
		}
	}
	return err
}

func compatibilityOwnedError(err error) error {
	switch err.(type) { //nolint:errorlint // Exact successor ownership precedes generic lifecycle joins.
	case *successor.OptionsError, *successor.StartupError, *successor.PublishError,
		*successor.CallbackPanicError, *successor.CallbackError:
		return compatibilityError(err)
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		converted := make([]error, len(causes))
		for index, cause := range causes {
			converted[index] = compatibilityOwnedError(cause)
		}
		return errors.Join(converted...)
	}
	return compatibilityError(err)
}

func compatibilityPublishError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if !sameErrorIdentity(err, successor.ErrUnavailable) {
		if ctx != nil {
			current := context.Cause(ctx)
			if sameErrorIdentity(err, current) {
				return current
			}
		}
	}
	return compatibilityError(err)
}

func sameErrorIdentity(left, right error) bool {
	if left == nil {
		return false
	}
	if right == nil {
		return false
	}
	leftType := reflect.TypeOf(left)
	if leftType != reflect.TypeOf(right) {
		return false
	}
	if !leftType.Comparable() {
		return false
	}
	return left == right //nolint:errorlint // Exact comparable identity is the behavior under test.
}

func compatibilityComponent(component service.Component) service.Component {
	if component.CloseAdmission != nil {
		original := component.CloseAdmission
		component.CloseAdmission = func() error { return compatibilityOwnedError(original()) }
	}
	if component.Start != nil {
		original := component.Start
		component.Start = func(ctx context.Context) error { return compatibilityOwnedError(original(ctx)) }
	}
	if component.Stop != nil {
		original := component.Stop
		component.Stop = func(ctx context.Context) error { return compatibilityOwnedError(original(ctx)) }
	}
	return component
}

// OptionsError identifies one rejected option without exposing sensitive values.
type OptionsError struct {
	// Field identifies the rejected option.
	Field string
	// Reason describes the safe failure category.
	Reason string
}

// Error returns a secret-safe construction diagnostic.
func (err *OptionsError) Error() string {
	return fmt.Sprintf("%s: %s: %v", err.Field, err.Reason, ErrInvalidOptions)
}

// Unwrap exposes the stable option classification.
func (*OptionsError) Unwrap() error { return ErrInvalidOptions }

// CallbackPanicError identifies a recovered callback without retaining its value.
type CallbackPanicError struct{ Operation CallbackOperation }

// Error returns a secret-safe callback failure.
func (err *CallbackPanicError) Error() string {
	return (&successor.CallbackPanicError{Operation: successor.CallbackOperation(err.Operation)}).Error()
}

// Unwrap exposes the stable panic classification.
func (*CallbackPanicError) Unwrap() error { return ErrCallbackPanic }

// CallbackError preserves a callback cause without formatting its text.
type CallbackError struct {
	// Operation identifies the callback boundary.
	Operation CallbackOperation
	// Err is the original callback failure.
	Err error
}

// Error returns a secret-safe callback failure.
func (err *CallbackError) Error() string {
	return (&successor.CallbackError{Operation: successor.CallbackOperation(err.Operation), Err: err.Err}).Error()
}

// Unwrap preserves the callback cause for errors.Is and errors.As.
func (err *CallbackError) Unwrap() error { return err.Err }

// StartupError preserves validation and partial-cleanup failures.
type StartupError struct {
	// Validation is the startup-check failure.
	Validation error
	// Cleanup is an optional transferred-resource shutdown failure.
	Cleanup error
}

// Error returns a secret-safe startup diagnostic.
func (err *StartupError) Error() string {
	return (&successor.StartupError{Validation: err.Validation, Cleanup: err.Cleanup}).Error()
}

// Unwrap preserves both causes for errors.Is and errors.As.
func (err *StartupError) Unwrap() []error {
	return (&successor.StartupError{Validation: err.Validation, Cleanup: err.Cleanup}).Unwrap()
}

// PublishError preserves the backend cause and acceptance classification.
type PublishError struct {
	// Acceptance describes whether the task reached the backend.
	Acceptance PublishAcceptance
	// Err is the original classifiable failure.
	Err error
}

// Error returns a secret-safe publish diagnostic.
func (err *PublishError) Error() string {
	return (&successor.PublishError{Acceptance: successor.PublishAcceptance(err.Acceptance), Err: err.Err}).Error()
}

// Unwrap preserves the backend and stable acceptance causes.
func (err *PublishError) Unwrap() error { return err.Err }

// Startup validates an explicitly constructed resource before admission.
type Startup[R any] func(context.Context, R) error

// Check evaluates whether a resource can accept new work.
type Check[R any] func(context.Context, R) error

// CloseAdmission synchronously and idempotently stops new worker intake.
type CloseAdmission[R any] func(R) error

// Publish appends one correlation-aware message through a concrete resource.
type Publish[R any] func(context.Context, R, core.QueuedMessage, ...job.AllowOption) error

// PublishWithAcceptance appends one task and classifies backend acceptance.
type PublishWithAcceptance[R any] func(context.Context, R, core.QueuedMessage, ...job.AllowOption) (PublishAcceptance, error)

// Shutdown releases an explicitly transferred producer resource.
type Shutdown[R any] func(context.Context, R) error

// Handler processes one queue delivery within the supplied context.
type Handler func(context.Context, core.TaskMessage) error

// Run owns one blocking worker intake loop until cancellation or failure.
type Run[R any] func(context.Context, R, Handler) error

// ProducerOptions configure one producer lifecycle adapter.
type ProducerOptions[R any] struct {
	// Name is the secret-safe component name.
	Name string
	// Resource is the caller-constructed concrete producer.
	Resource R
	// Correlation creates message-hop identifiers.
	Correlation *correlation.Factory
	// CorrelationOptions configure queue propagation.
	CorrelationOptions queuecorrelation.Options
	// TracePropagator injects caller-owned telemetry context when non-nil.
	TracePropagator propagation.TextMapPropagator
	// Startup optionally validates Resource before admission begins.
	Startup Startup[R]
	// Readiness optionally checks Resource after successful startup.
	Readiness Check[R]
	// Publish performs one concrete caller-bounded append.
	Publish Publish[R]
	// PublishWithAcceptance performs one append with explicit acceptance.
	PublishWithAcceptance PublishWithAcceptance[R]
	// Shutdown transfers transport close ownership when non-nil.
	Shutdown Shutdown[R]
}

// Producer retains a concrete producer and coordinates its in-flight calls.
type Producer[R any] struct {
	successor *successor.Producer[R]
	zero      sync.Once
}

func (producer *Producer[R]) delegate() *successor.Producer[R] {
	producer.zero.Do(func() {
		if producer.successor == nil {
			producer.successor = new(successor.Producer[R])
		}
	})
	return producer.successor
}

// NewProducer validates and constructs a lifecycle-aware producer.
func NewProducer[R any](options ProducerOptions[R]) (*Producer[R], error) {
	configured := successor.ProducerOptions[R]{
		Name: options.Name, Resource: options.Resource, Correlation: options.Correlation,
		CorrelationOptions: options.CorrelationOptions, TracePropagator: options.TracePropagator,
		Startup: successor.Startup[R](options.Startup), Readiness: successor.Check[R](options.Readiness),
		Publish: successor.Publish[R](options.Publish), Shutdown: successor.Shutdown[R](options.Shutdown),
	}
	if options.PublishWithAcceptance != nil {
		configured.PublishWithAcceptance = func(ctx context.Context, resource R, message core.QueuedMessage, values ...job.AllowOption) (successor.PublishAcceptance, error) {
			acceptance, err := options.PublishWithAcceptance(ctx, resource, message, values...)
			return successor.PublishAcceptance(acceptance), err
		}
	}
	producer, err := successor.NewProducer(configured)
	if err != nil {
		return nil, compatibilityError(err)
	}
	return &Producer[R]{successor: producer}, nil
}

// Resource returns the exact caller-provided producer resource.
func (producer *Producer[R]) Resource() R { return producer.delegate().Resource() }

// Component returns the service lifecycle component for this producer.
func (producer *Producer[R]) Component() service.Component {
	return compatibilityComponent(producer.delegate().Component())
}

// Readiness returns the optional dependency readiness check.
func (producer *Producer[R]) Readiness() (service.ReadinessCheck, bool) {
	check, ok := producer.delegate().Readiness()
	if ok {
		original := check.Run
		check.Run = func(ctx context.Context) error { return compatibilityError(original(ctx)) }
	}
	return check, ok
}

// Publish appends one correlation-aware task and returns its propagated values.
func (producer *Producer[R]) Publish(ctx context.Context, message core.QueuedMessage, options ...job.AllowOption) (correlation.Values, error) {
	values, err := producer.delegate().Publish(ctx, message, options...)
	return values, compatibilityPublishError(ctx, err)
}

// PublishWithAcceptance appends one task and reports its backend acceptance.
func (producer *Producer[R]) PublishWithAcceptance(ctx context.Context, message core.QueuedMessage, options ...job.AllowOption) (correlation.Values, PublishAcceptance, error) {
	values, acceptance, err := producer.delegate().PublishWithAcceptance(ctx, message, options...)
	return values, PublishAcceptance(acceptance), compatibilityPublishError(ctx, err)
}

// HandlerOptions configure correlation and tracing around one task handler.
type HandlerOptions struct {
	// Correlation creates per-delivery request identifiers.
	Correlation *correlation.Factory
	// CorrelationOptions configure queue metadata propagation.
	CorrelationOptions queuecorrelation.Options
	// TrustedMetadata permits authenticated incoming correlation metadata.
	TrustedMetadata bool
	// TracePropagator extracts caller-approved telemetry context when non-nil.
	TracePropagator propagation.TextMapPropagator
	// Handler processes the decoded task.
	Handler Handler
}

// NewHandler validates and wraps one correlation-aware delivery handler.
func NewHandler(options HandlerOptions) (Handler, error) {
	handler, err := successor.NewHandler(successor.HandlerOptions{
		Correlation: options.Correlation, CorrelationOptions: options.CorrelationOptions,
		TrustedMetadata: options.TrustedMetadata, TracePropagator: options.TracePropagator,
		Handler: successor.Handler(options.Handler),
	})
	if err != nil {
		return nil, compatibilityError(err)
	}
	return func(ctx context.Context, message core.TaskMessage) error {
		return compatibilityError(handler(ctx, message))
	}, nil
}

// LifecycleWorkerOptions configure a typed supervised worker lifecycle.
type LifecycleWorkerOptions[R any] struct {
	// Name is the stable service identity.
	Name string
	// Resource is the caller-constructed worker resource.
	Resource R
	// Correlation creates per-delivery request identifiers.
	Correlation *correlation.Factory
	// CorrelationOptions configure queue metadata propagation.
	CorrelationOptions queuecorrelation.Options
	// TrustedMetadata permits authenticated incoming correlation metadata.
	TrustedMetadata bool
	// TracePropagator extracts caller-approved telemetry context when non-nil.
	TracePropagator propagation.TextMapPropagator
	// Handler processes admitted deliveries.
	Handler Handler
	// Startup optionally validates Resource before intake.
	Startup Startup[R]
	// Readiness optionally checks Resource after startup.
	Readiness Check[R]
	// CloseAdmission synchronously stops new intake during drain.
	CloseAdmission CloseAdmission[R]
	// Run owns the blocking intake loop.
	Run Run[R]
	// Shutdown releases the transferred worker resource.
	Shutdown Shutdown[R]
}

// LifecycleWorker coordinates startup, intake, drain, and shutdown for a resource.
type LifecycleWorker[R any] struct {
	successor *successor.LifecycleWorker[R]
	zero      sync.Once
}

func (worker *LifecycleWorker[R]) delegate() *successor.LifecycleWorker[R] {
	worker.zero.Do(func() {
		if worker.successor == nil {
			worker.successor = new(successor.LifecycleWorker[R])
		}
	})
	return worker.successor
}

// NewLifecycleWorker validates and constructs a supervised typed worker.
func NewLifecycleWorker[R any](options LifecycleWorkerOptions[R]) (*LifecycleWorker[R], error) {
	configured := successor.LifecycleWorkerOptions[R]{
		Name: options.Name, Resource: options.Resource, Correlation: options.Correlation,
		CorrelationOptions: options.CorrelationOptions, TrustedMetadata: options.TrustedMetadata,
		TracePropagator: options.TracePropagator, Handler: successor.Handler(options.Handler),
		Startup: successor.Startup[R](options.Startup), Readiness: successor.Check[R](options.Readiness),
		CloseAdmission: successor.CloseAdmission[R](options.CloseAdmission), Shutdown: successor.Shutdown[R](options.Shutdown),
	}
	if options.Run != nil {
		configured.Run = func(ctx context.Context, resource R, handler successor.Handler) error {
			legacyHandler := func(ctx context.Context, message core.TaskMessage) error {
				return compatibilityError(handler(ctx, message))
			}
			return options.Run(ctx, resource, legacyHandler)
		}
	}
	worker, err := successor.NewLifecycleWorker(configured)
	if err != nil {
		return nil, compatibilityError(err)
	}
	return &LifecycleWorker[R]{successor: worker}, nil
}

// Resource returns the exact caller-provided worker resource.
func (worker *LifecycleWorker[R]) Resource() R { return worker.delegate().Resource() }

// Plan returns the component, task, and optional readiness check under one identity.
func (worker *LifecycleWorker[R]) Plan() service.Plan {
	plan := worker.delegate().Plan()
	for index := range plan.Components {
		plan.Components[index] = compatibilityComponent(plan.Components[index])
	}
	for index := range plan.Tasks {
		original := plan.Tasks[index].Run
		plan.Tasks[index].Run = func(ctx context.Context) error { return compatibilityError(original(ctx)) }
	}
	for index := range plan.Readiness {
		original := plan.Readiness[index].Run
		plan.Readiness[index].Run = func(ctx context.Context) error { return compatibilityError(original(ctx)) }
	}
	return plan
}

// WorkerOptions configure the concrete queue convenience adapter.
type WorkerOptions struct {
	// Name is the stable service identity.
	Name string
	// Queue is the caller-constructed queue resource.
	Queue *queue.Queue
}

// Worker adapts a concrete queue to the service lifecycle.
type Worker struct {
	successor *successor.Worker
	zero      sync.Once
}

func (worker *Worker) delegate() *successor.Worker {
	worker.zero.Do(func() {
		if worker.successor == nil {
			worker.successor = new(successor.Worker)
		}
	})
	return worker.successor
}

// NewWorker validates and constructs the concrete queue lifecycle adapter.
func NewWorker(options WorkerOptions) (*Worker, error) {
	worker, err := successor.NewWorker(successor.WorkerOptions{Name: options.Name, Queue: options.Queue})
	if err != nil {
		return nil, compatibilityError(err)
	}
	return &Worker{successor: worker}, nil
}

// Queue returns the exact caller-provided queue.
func (worker *Worker) Queue() *queue.Queue { return worker.delegate().Queue() }

// Component starts, drains, and releases the queue through service lifecycle hooks.
func (worker *Worker) Component() service.Component {
	return compatibilityComponent(worker.delegate().Component())
}
