package queueservice

import (
	"context"
	"errors"
	"fmt"
	"testing"

	queue "github.com/faustbrian/go-queue"
	successor "github.com/faustbrian/go-queue/adapters/service"
	"github.com/faustbrian/go-queue/core"
	"github.com/faustbrian/go-queue/job"
	"github.com/faustbrian/go-service"
)

func TestCompatibilityErrorPreservesUnknownWrapper(t *testing.T) {
	original := fmt.Errorf("wrapped successor: %w", &successor.OptionsError{
		Field: "Name", Reason: "invalid",
	})
	converted := compatibilityError(original)
	if !errors.Is(converted, original) {
		t.Fatalf("compatibilityError() = %T, want original wrapper", converted)
	}
}

func TestOptionsErrorFormatsTheCurrentLegacySentinel(t *testing.T) {
	original := ErrInvalidOptions
	custom := errors.New("custom invalid queue service options")
	ErrInvalidOptions = custom
	t.Cleanup(func() { ErrInvalidOptions = original })

	err := &OptionsError{Field: "Name", Reason: "must not be empty"}
	if got, want := err.Error(), "Name: must not be empty: "+custom.Error(); got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	if !errors.Is(err, custom) {
		t.Fatal("OptionsError does not unwrap to the current legacy sentinel")
	}
}

type typedWrapper struct{ cause error }

func (*typedWrapper) Error() string     { return "typed wrapper" }
func (err *typedWrapper) Unwrap() error { return err.cause }

type typedMultiError struct{ causes []error }

func (*typedMultiError) Error() string       { return "typed multi-error" }
func (err *typedMultiError) Unwrap() []error { return err.causes }

type noncomparableValueError []string

func (noncomparableValueError) Error() string { return "noncomparable value error" }

type causeOnSecondErrContext struct {
	context.Context
	calls  int
	cancel func()
}

func (ctx *causeOnSecondErrContext) Err() error {
	ctx.calls++
	if ctx.calls == 2 {
		ctx.cancel()
	}
	return ctx.Context.Err()
}

func TestCompatibilityErrorRestoresReleasedTypes(t *testing.T) {
	cause := errors.New("cause")
	tests := []struct {
		name string
		err  error
		want any
	}{
		{name: "options", err: &successor.OptionsError{Field: "Name", Reason: "invalid"}, want: new(*OptionsError)},
		{name: "panic", err: &successor.CallbackPanicError{Operation: successor.CallbackRun}, want: new(*CallbackPanicError)},
		{name: "callback", err: &successor.CallbackError{Operation: successor.CallbackRun, Err: cause}, want: new(*CallbackError)},
		{name: "startup", err: &successor.StartupError{Validation: cause}, want: new(*StartupError)},
		{name: "publish", err: &successor.PublishError{Acceptance: successor.PublishUnknown, Err: cause}, want: new(*PublishError)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			converted := compatibilityError(test.err)
			if !errors.As(converted, test.want) {
				t.Fatalf("compatibilityError(%T) = %T", test.err, converted)
			}
			if converted.Error() != test.err.Error() {
				t.Fatalf("error text changed: %q != %q", converted, test.err)
			}
		})
	}
	if compatibilityError(nil) != nil {
		t.Fatal("nil error changed")
	}
	if converted := compatibilityError(cause); !errors.Is(converted, cause) {
		t.Fatal("unrelated error changed")
	}
}

func TestSameErrorIdentityRequiresExactComparableValue(t *testing.T) {
	first := errors.New("cause")
	if sameErrorIdentity(nil, first) || sameErrorIdentity(first, nil) {
		t.Fatal("nil errors unexpectedly have identity")
	}
	if !sameErrorIdentity(first, first) {
		t.Fatal("same comparable error lost identity")
	}
	if sameErrorIdentity(first, errors.New("cause")) {
		t.Fatal("distinct comparable errors share identity")
	}
	if sameErrorIdentity(first, &successor.CallbackError{Operation: successor.CallbackPublish, Err: first}) {
		t.Fatal("different error types share identity")
	}
	uncomparable := noncomparableValueError{"cause"}
	if sameErrorIdentity(uncomparable, uncomparable) {
		t.Fatal("noncomparable error was treated as identity-safe")
	}
}

func TestCompatibilityErrorTranslatesNestedPublishCause(t *testing.T) {
	cause := errors.New("cause")
	tests := []struct {
		name string
		err  error
		want any
	}{
		{
			name: "callback",
			err: &successor.CallbackError{
				Operation: successor.CallbackPublish,
				Err:       cause,
			},
			want: new(*CallbackError),
		},
		{
			name: "panic",
			err:  &successor.CallbackPanicError{Operation: successor.CallbackPublish},
			want: new(*CallbackPanicError),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			converted := compatibilityError(&successor.PublishError{
				Acceptance: successor.PublishUnknown,
				Err:        test.err,
			})
			var publish *PublishError
			if !errors.As(converted, &publish) {
				t.Fatalf("compatibilityError() = %T, want *PublishError", converted)
			}
			if !errors.As(publish.Err, test.want) {
				t.Fatalf("PublishError.Err = %T, want %T", publish.Err, test.want)
			}
		})
	}
}

func TestCompatibilityErrorPreservesJoinedShutdownCauses(t *testing.T) {
	converted := compatibilityOwnedError(errors.Join(
		&successor.CallbackError{
			Operation: successor.CallbackShutdown,
			Err:       queue.ErrWorkerShutdownPanic,
		},
		&successor.CallbackPanicError{Operation: successor.CallbackShutdown},
	))
	if !errors.Is(converted, queue.ErrWorkerShutdownPanic) {
		t.Fatal("compatibilityError() lost ErrWorkerShutdownPanic")
	}
	var callback *CallbackError
	if !errors.As(converted, &callback) || callback.Operation != CallbackShutdown {
		t.Fatalf("compatibilityError() callback = %#v", callback)
	}
	var panicError *CallbackPanicError
	if !errors.As(converted, &panicError) || panicError.Operation != CallbackShutdown {
		t.Fatalf("compatibilityError() panic = %#v", panicError)
	}
	var leakedCallback *successor.CallbackError
	var leakedPanic *successor.CallbackPanicError
	if errors.As(converted, &leakedCallback) || errors.As(converted, &leakedPanic) {
		t.Fatal("compatibilityError() leaked successor structured error")
	}
}

func TestCanceledPublishPreservesCallerOwnedMultiErrorIdentity(t *testing.T) {
	producer, err := NewProducer(ProducerOptions[int]{
		Name:        "producer",
		Resource:    1,
		Correlation: facadeFactory(t),
		Publish:     func(context.Context, int, core.QueuedMessage, ...job.AllowOption) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}
	if err = producer.Component().Start(t.Context()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	original := &typedMultiError{}
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(original)
	if _, err = producer.Publish(ctx, facadeQueuedPayload("payload")); !errors.Is(err, original) {
		t.Fatalf("Publish() error = %v, want original cancellation cause", err)
	}
	var preserved *typedMultiError
	if !errors.As(err, &preserved) || preserved != original {
		t.Fatalf("Publish() cause = %p, want %p", preserved, original)
	}
	if _, acceptance, publishErr := producer.PublishWithAcceptance(ctx, facadeQueuedPayload("payload")); acceptance != PublishNotAccepted || !errors.Is(publishErr, original) {
		t.Fatalf("PublishWithAcceptance() = (%v, %v)", acceptance, publishErr)
	}
}

func TestCanceledPublishPreservesCallerOwnedSuccessorErrorIdentity(t *testing.T) {
	producer, err := NewProducer(ProducerOptions[int]{
		Name:        "producer",
		Resource:    1,
		Correlation: facadeFactory(t),
		Publish:     func(context.Context, int, core.QueuedMessage, ...job.AllowOption) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}
	if err = producer.Component().Start(t.Context()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	original := &successor.CallbackError{
		Operation: successor.CallbackPublish,
		Err:       errors.New("caller-owned cancellation cause"),
	}
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(original)
	if _, err = producer.Publish(ctx, facadeQueuedPayload("payload")); err != original { //nolint:errorlint // Exact caller-owned identity is required.
		t.Fatalf("Publish() error = %p (%T), want exact caller cause %p", err, err, original)
	}
	if _, acceptance, publishErr := producer.PublishWithAcceptance(ctx, facadeQueuedPayload("payload")); acceptance != PublishNotAccepted || publishErr != original { //nolint:errorlint // Exact caller-owned identity is required.
		t.Fatalf("PublishWithAcceptance() = (%v, %p %T), want exact caller cause %p", acceptance, publishErr, publishErr, original)
	}
	if err = producer.Component().Stop(t.Context()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if _, err = producer.Publish(ctx, facadeQueuedPayload("payload")); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("stopped Publish() error = %v, want ErrUnavailable before cancellation cause", err)
	}
}

func TestPublishDoesNotManufactureErrorWhenCallbackCancelsContext(t *testing.T) {
	cause := errors.New("callback-triggered cancellation")
	t.Run("publish", func(t *testing.T) {
		var cancel context.CancelCauseFunc
		producer, err := NewProducer(ProducerOptions[int]{
			Name:        "producer",
			Resource:    1,
			Correlation: facadeFactory(t),
			Publish: func(context.Context, int, core.QueuedMessage, ...job.AllowOption) error {
				cancel(cause)
				return nil
			},
		})
		if err != nil {
			t.Fatalf("NewProducer() error = %v", err)
		}
		if err = producer.Component().Start(t.Context()); err != nil {
			t.Fatalf("Start() error = %v", err)
		}
		ctx, cancelContext := context.WithCancelCause(facadeProducerContext())
		cancel = cancelContext
		if _, err = producer.Publish(ctx, facadeQueuedPayload("payload")); err != nil {
			t.Fatalf("Publish() error = %v, want callback success", err)
		}
	})

	t.Run("publish with acceptance", func(t *testing.T) {
		var cancel context.CancelCauseFunc
		producer, err := NewProducer(ProducerOptions[int]{
			Name:        "producer",
			Resource:    1,
			Correlation: facadeFactory(t),
			PublishWithAcceptance: func(context.Context, int, core.QueuedMessage, ...job.AllowOption) (PublishAcceptance, error) {
				cancel(cause)
				return PublishAccepted, nil
			},
		})
		if err != nil {
			t.Fatalf("NewProducer() error = %v", err)
		}
		if err = producer.Component().Start(t.Context()); err != nil {
			t.Fatalf("Start() error = %v", err)
		}
		ctx, cancelContext := context.WithCancelCause(facadeProducerContext())
		cancel = cancelContext
		_, acceptance, publishErr := producer.PublishWithAcceptance(ctx, facadeQueuedPayload("payload"))
		if publishErr != nil || acceptance != PublishAccepted {
			t.Fatalf("PublishWithAcceptance() = (%v, %v), want accepted callback success", acceptance, publishErr)
		}
	})

	t.Run("package error is not masked", func(t *testing.T) {
		callbackFailure := errors.New("callback failure")
		var cancel context.CancelCauseFunc
		producer, err := NewProducer(ProducerOptions[int]{
			Name:        "producer",
			Resource:    1,
			Correlation: facadeFactory(t),
			Publish: func(context.Context, int, core.QueuedMessage, ...job.AllowOption) error {
				cancel(cause)
				return callbackFailure
			},
		})
		if err != nil {
			t.Fatalf("NewProducer() error = %v", err)
		}
		if err = producer.Component().Start(t.Context()); err != nil {
			t.Fatalf("Start() error = %v", err)
		}
		ctx, cancelContext := context.WithCancelCause(facadeProducerContext())
		cancel = cancelContext
		_, err = producer.Publish(ctx, facadeQueuedPayload("payload"))
		var callback *CallbackError
		if !errors.As(err, &callback) || callback.Operation != CallbackPublish || !errors.Is(err, callbackFailure) {
			t.Fatalf("Publish() error = %v, want legacy callback failure", err)
		}
		if err == cause { //nolint:errorlint // The package error must not be replaced by this exact cause.
			t.Fatal("Publish() package error was replaced by callback-triggered cancellation")
		}
	})
}

func TestPublishDoesNotAddCancellationObservations(t *testing.T) {
	publishCalls := 0
	producer, err := NewProducer(ProducerOptions[int]{
		Name:        "producer",
		Resource:    1,
		Correlation: facadeFactory(t),
		Publish: func(context.Context, int, core.QueuedMessage, ...job.AllowOption) error {
			publishCalls++
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}
	if err = producer.Component().Start(t.Context()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	original := &successor.CallbackError{
		Operation: successor.CallbackPublish,
		Err:       errors.New("caller-owned racing cancellation cause"),
	}
	base, cancel := context.WithCancelCause(facadeProducerContext())
	ctx := &causeOnSecondErrContext{Context: base, cancel: func() { cancel(original) }}
	if _, err = producer.Publish(ctx, facadeQueuedPayload("payload")); err != nil {
		t.Fatalf("Publish() error = %v, want released successful result", err)
	}
	if publishCalls != 1 || ctx.calls != 1 {
		t.Fatalf("Publish() calls = (%d callback, %d context), want (1, 1)", publishCalls, ctx.calls)
	}
	base, cancel = context.WithCancelCause(facadeProducerContext())
	ctx = &causeOnSecondErrContext{Context: base, cancel: func() { cancel(original) }}
	if _, acceptance, publishErr := producer.PublishWithAcceptance(ctx, facadeQueuedPayload("payload")); acceptance != PublishAccepted || publishErr != nil {
		t.Fatalf("PublishWithAcceptance() = (%v, %v), want released accepted result", acceptance, publishErr)
	}
	if publishCalls != 2 || ctx.calls != 1 {
		t.Fatalf("PublishWithAcceptance() calls = (%d callback, %d context), want (2, 1)", publishCalls, ctx.calls)
	}
}

func TestCompatibilityErrorUsesCurrentLegacySentinels(t *testing.T) {
	tests := []struct {
		name      string
		legacy    *error
		successor error
	}{
		{name: "invalid options", legacy: &ErrInvalidOptions, successor: successor.ErrInvalidOptions},
		{name: "unavailable", legacy: &ErrUnavailable, successor: successor.ErrUnavailable},
		{name: "missing correlation", legacy: &ErrMissingCorrelation, successor: successor.ErrMissingCorrelation},
		{name: "callback panic", legacy: &ErrCallbackPanic, successor: successor.ErrCallbackPanic},
		{name: "publish outcome unknown", legacy: &ErrPublishOutcomeUnknown, successor: successor.ErrPublishOutcomeUnknown},
		{name: "invalid publish acceptance", legacy: &ErrInvalidPublishAcceptance, successor: successor.ErrInvalidPublishAcceptance},
		{name: "worker exited", legacy: &ErrWorkerExited, successor: successor.ErrWorkerExited},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			original := *test.legacy
			replacement := errors.New("replacement " + test.name)
			*test.legacy = replacement
			t.Cleanup(func() { *test.legacy = original })
			if converted := compatibilityError(test.successor); converted != replacement { //nolint:errorlint // Exact mutable sentinel compatibility is required.
				t.Fatalf("compatibilityError() = %v, want reassigned legacy sentinel", converted)
			}
		})
	}
}

func TestCompatibilityErrorPreservesCallerCallbackCauseIdentity(t *testing.T) {
	original := &typedWrapper{cause: errors.New("cause")}
	converted := compatibilityError(&successor.CallbackError{
		Operation: successor.CallbackPublish,
		Err:       original,
	})
	var callback *CallbackError
	if !errors.As(converted, &callback) || callback.Operation != CallbackPublish {
		t.Fatalf("compatibilityError() callback = %#v", callback)
	}
	if !errors.Is(callback.Err, original) {
		t.Fatalf("CallbackError.Err = %p, want exact caller error %p", callback.Err, original)
	}
	var wrapper *typedWrapper
	if !errors.As(converted, &wrapper) || wrapper != original {
		t.Fatalf("compatibilityError() wrapper = %p, want %p", wrapper, original)
	}
	var leaked *successor.CallbackError
	if errors.As(converted, &leaked) {
		t.Fatalf("compatibilityError() leaked successor callback %#v", leaked)
	}
}

func TestPublicHandlerPreservesCallerOwnedMultiErrorIdentity(t *testing.T) {
	original := &typedMultiError{causes: []error{&successor.OptionsError{Field: "Name", Reason: "caller owned"}}}
	handler, err := NewHandler(HandlerOptions{
		Correlation: facadeFactory(t),
		Handler:     func(context.Context, core.TaskMessage) error { return original },
	})
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	err = handler(t.Context(), facadeQueuedPayload("payload"))
	var callback *CallbackError
	if !errors.As(err, &callback) || callback.Operation != CallbackHandler {
		t.Fatalf("handler callback = %#v", callback)
	}
	var preserved *typedMultiError
	if !errors.As(callback.Err, &preserved) || preserved != original || !errors.Is(err, original) {
		t.Fatalf("handler cause = %T, want exact caller multi-error", callback.Err)
	}
}

func TestCompatibilityComponentTranslatesCallbackErrors(t *testing.T) {
	callback := func() error {
		return &successor.CallbackError{Operation: successor.CallbackAdmission, Err: errors.New("cause")}
	}
	component := compatibilityComponent(service.Component{
		CloseAdmission: callback,
		Start:          func(context.Context) error { return callback() },
		Stop:           func(context.Context) error { return callback() },
	})
	for name, invoke := range map[string]func() error{
		"admission": component.CloseAdmission,
		"start":     func() error { return component.Start(t.Context()) },
		"stop":      func() error { return component.Stop(t.Context()) },
	} {
		var converted *CallbackError
		if err := invoke(); !errors.As(err, &converted) {
			t.Fatalf("%s error = %T", name, err)
		}
	}
}

func TestLifecycleRunReceivesAndReturnsLegacyHandlerErrors(t *testing.T) {
	handlerCause := errors.New("handler cause")
	legacySeenInsideRun := false
	worker, err := NewLifecycleWorker(LifecycleWorkerOptions[int]{
		Name:        "worker",
		Resource:    1,
		Correlation: facadeFactory(t),
		Handler: func(context.Context, core.TaskMessage) error {
			return handlerCause
		},
		Run: func(ctx context.Context, _ int, handler Handler) error {
			handlerErr := handler(ctx, facadeQueuedPayload("payload"))
			var callback *CallbackError
			legacySeenInsideRun = errors.As(handlerErr, &callback) && callback.Operation == CallbackHandler
			return handlerErr
		},
		Shutdown: func(context.Context, int) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewLifecycleWorker() error = %v", err)
	}
	plan := worker.Plan()
	if err = plan.Components[0].Start(t.Context()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	err = plan.Tasks[0].Run(t.Context())
	if !legacySeenInsideRun {
		t.Fatal("Run handler did not expose legacy CallbackError")
	}
	if !errors.Is(err, handlerCause) {
		t.Fatalf("Run() error = %v, want handler cause", err)
	}
	var leaked *successor.CallbackError
	if errors.As(err, &leaked) {
		t.Fatalf("Run() leaked successor callback %#v", leaked)
	}
}

func TestLegacyFacadeZeroValuesPreserveReleasedShapes(t *testing.T) {
	producer := new(Producer[int])
	if producer.Resource() != 0 {
		t.Fatalf("zero Producer.Resource() = %d", producer.Resource())
	}
	if _, ok := producer.Readiness(); ok {
		t.Fatal("zero Producer.Readiness() unexpectedly enabled")
	}
	if _, err := producer.Publish(t.Context(), facadeQueuedPayload("payload")); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("zero Producer.Publish() error = %v", err)
	}
	if _, acceptance, err := producer.PublishWithAcceptance(t.Context(), facadeQueuedPayload("payload")); acceptance != PublishNotAccepted || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("zero Producer.PublishWithAcceptance() = (%v, %v)", acceptance, err)
	}
	producerComponent := producer.Component()
	if producerComponent.Name != "" || producerComponent.Start == nil ||
		producerComponent.Stop == nil || producerComponent.CloseAdmission == nil {
		t.Fatalf("zero Producer.Component() = %#v", producerComponent)
	}
	if err := producerComponent.Start(t.Context()); err != nil {
		t.Fatalf("zero Producer Start() error = %v", err)
	}
	if _, err := producer.Publish(t.Context(), facadeQueuedPayload("payload")); !errors.Is(err, ErrMissingCorrelation) {
		t.Fatalf("started zero Producer.Publish() error = %v", err)
	}
	if err := producerComponent.CloseAdmission(); err != nil {
		t.Fatalf("zero Producer CloseAdmission() error = %v", err)
	}
	if err := producerComponent.Stop(t.Context()); err != nil {
		t.Fatalf("zero Producer Stop() error = %v", err)
	}

	lifecycle := new(LifecycleWorker[int])
	if lifecycle.Resource() != 0 {
		t.Fatalf("zero LifecycleWorker.Resource() = %d", lifecycle.Resource())
	}
	plan := lifecycle.Plan()
	if len(plan.Components) != 1 || len(plan.Tasks) != 1 || len(plan.Readiness) != 0 {
		t.Fatalf("zero LifecycleWorker.Plan() shape = (%d, %d, %d)", len(plan.Components), len(plan.Tasks), len(plan.Readiness))
	}
	if err := plan.Components[0].Start(t.Context()); err != nil {
		t.Fatalf("zero LifecycleWorker Start() error = %v", err)
	}
	var runPanic *CallbackPanicError
	if err := plan.Tasks[0].Run(t.Context()); !errors.As(err, &runPanic) || runPanic.Operation != CallbackRun {
		t.Fatalf("zero LifecycleWorker Run() panic = %#v", runPanic)
	}
	if err := plan.Components[0].CloseAdmission(); err != nil {
		t.Fatalf("zero LifecycleWorker CloseAdmission() error = %v", err)
	}
	var shutdownPanic *CallbackPanicError
	if err := plan.Components[0].Stop(t.Context()); !errors.As(err, &shutdownPanic) || shutdownPanic.Operation != CallbackShutdown {
		t.Fatalf("zero LifecycleWorker Stop() panic = %#v", shutdownPanic)
	}

	worker := new(Worker)
	if worker.Queue() != nil {
		t.Fatal("zero Worker.Queue() is non-nil")
	}
	component := worker.Component()
	if component.Name != "" || component.Start == nil || component.Stop == nil || component.CloseAdmission == nil {
		t.Fatalf("zero Worker.Component() = %#v", component)
	}
	for name, invoke := range map[string]func() error{
		"admission": component.CloseAdmission,
		"start":     func() error { return component.Start(t.Context()) },
		"stop":      func() error { return component.Stop(t.Context()) },
	} {
		var panicError *CallbackPanicError
		if err := invoke(); !errors.As(err, &panicError) {
			t.Fatalf("zero Worker %s error = %v", name, err)
		}
	}
}

func assertLegacyCallback(t *testing.T, err, cause error, operation CallbackOperation) {
	t.Helper()
	if !errors.Is(err, cause) {
		t.Fatalf("error = %v, want cause", err)
	}
	var callback *CallbackError
	if !errors.As(err, &callback) || callback.Operation != operation {
		t.Fatalf("error %T legacy callback = %#v, want operation %d", err, callback, operation)
	}
	var leaked *successor.CallbackError
	if errors.As(err, &leaked) {
		t.Fatalf("error leaked successor callback %#v", leaked)
	}
}

func TestPublicProducerAndHandlerBoundariesTranslateErrors(t *testing.T) {
	cause := errors.New("callback cause")
	handler, err := NewHandler(HandlerOptions{
		Correlation: facadeFactory(t),
		Handler:     func(context.Context, core.TaskMessage) error { return cause },
	})
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	assertLegacyCallback(t, handler(t.Context(), facadeQueuedPayload("payload")), cause, CallbackHandler)

	producer, err := NewProducer(ProducerOptions[int]{
		Name:        "producer",
		Resource:    1,
		Correlation: facadeFactory(t),
		Readiness:   func(context.Context, int) error { return cause },
		Publish: func(context.Context, int, core.QueuedMessage, ...job.AllowOption) error {
			return cause
		},
		Shutdown: func(context.Context, int) error { return cause },
	})
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}
	component := producer.Component()
	if err = component.Start(t.Context()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	readiness, ok := producer.Readiness()
	if !ok {
		t.Fatal("Readiness() missing")
	}
	assertLegacyCallback(t, readiness.Run(t.Context()), cause, CallbackReadiness)
	_, err = producer.Publish(facadeProducerContext(), facadeQueuedPayload("payload"))
	assertLegacyCallback(t, err, cause, CallbackPublish)
	var publish *PublishError
	if !errors.As(err, &publish) || publish.Acceptance != PublishUnknown {
		t.Fatalf("legacy publish = %#v", publish)
	}
	assertLegacyCallback(t, component.Stop(t.Context()), cause, CallbackShutdown)
}

func TestPublicStartupAndAdmissionBoundariesTranslateErrors(t *testing.T) {
	cause := errors.New("callback cause")
	producer, err := NewProducer(ProducerOptions[int]{
		Name:        "producer",
		Resource:    1,
		Correlation: facadeFactory(t),
		Startup:     func(context.Context, int) error { return cause },
		Publish:     func(context.Context, int, core.QueuedMessage, ...job.AllowOption) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}
	err = producer.Component().Start(t.Context())
	assertLegacyCallback(t, err, cause, CallbackStartup)
	var startup *StartupError
	if !errors.As(err, &startup) {
		t.Fatalf("Start() error = %T, want *StartupError", err)
	}

	worker, err := NewLifecycleWorker(LifecycleWorkerOptions[int]{
		Name:           "worker",
		Resource:       1,
		Correlation:    facadeFactory(t),
		Handler:        func(context.Context, core.TaskMessage) error { return nil },
		CloseAdmission: func(int) error { return cause },
		Run:            func(context.Context, int, Handler) error { return nil },
		Shutdown:       func(context.Context, int) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewLifecycleWorker() error = %v", err)
	}
	plan := worker.Plan()
	assertLegacyCallback(t, plan.Components[0].CloseAdmission(), cause, CallbackAdmission)
}
