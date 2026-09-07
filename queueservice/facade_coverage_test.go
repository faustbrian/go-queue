package queueservice

import (
	"context"
	"errors"
	"testing"

	"github.com/faustbrian/go-correlation"
	successor "github.com/faustbrian/go-queue/adapters/service"
	"github.com/faustbrian/go-queue/core"
	"github.com/faustbrian/go-queue/job"
)

type facadeQueuedPayload string

func (payload facadeQueuedPayload) Bytes() []byte   { return []byte(payload) }
func (payload facadeQueuedPayload) Payload() []byte { return []byte(payload) }

func facadeFactory(t *testing.T) *correlation.Factory {
	t.Helper()
	factory, err := correlation.NewFactory(correlation.FactoryOptions{})
	if err != nil {
		t.Fatalf("NewFactory() error = %v", err)
	}
	return factory
}

func facadeProducerContext() context.Context {
	return correlation.WithValues(context.Background(), correlation.Values{
		CorrelationID: correlation.MustCorrelationID("workflow", correlation.Policy{}),
		RequestID:     correlation.MustRequestID("request", correlation.Policy{}),
	})
}

func TestLegacyErrorMethodsPreserveReleasedClassification(t *testing.T) {
	cause := errors.New("cause")
	errorsToCheck := []struct {
		err  error
		want error
	}{
		{err: &OptionsError{Field: "Name", Reason: "invalid"}, want: ErrInvalidOptions},
		{err: &CallbackPanicError{Operation: CallbackRun}, want: ErrCallbackPanic},
		{err: &CallbackError{Operation: CallbackRun, Err: cause}, want: cause},
		{err: &StartupError{Validation: cause}, want: cause},
		{err: &PublishError{Acceptance: PublishUnknown, Err: cause}, want: cause},
	}
	for _, test := range errorsToCheck {
		if test.err.Error() == "" || !errors.Is(test.err, test.want) {
			t.Fatalf("error %T lost text or classification", test.err)
		}
	}
}

func TestLegacyCallbackErrorsPreserveEveryOperation(t *testing.T) {
	cause := errors.New("cause")
	operations := []struct {
		name      string
		legacy    CallbackOperation
		successor successor.CallbackOperation
	}{
		{name: "startup", legacy: CallbackStartup, successor: successor.CallbackStartup},
		{name: "readiness", legacy: CallbackReadiness, successor: successor.CallbackReadiness},
		{name: "publish", legacy: CallbackPublish, successor: successor.CallbackPublish},
		{name: "handler", legacy: CallbackHandler, successor: successor.CallbackHandler},
		{name: "run", legacy: CallbackRun, successor: successor.CallbackRun},
		{name: "shutdown", legacy: CallbackShutdown, successor: successor.CallbackShutdown},
		{name: "admission", legacy: CallbackAdmission, successor: successor.CallbackAdmission},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			legacyPanic := &CallbackPanicError{Operation: operation.legacy}
			successorPanic := &successor.CallbackPanicError{Operation: operation.successor}
			if legacyPanic.Error() != successorPanic.Error() || !errors.Is(legacyPanic, ErrCallbackPanic) {
				t.Fatalf("panic error = %q, want %q with stable classification", legacyPanic, successorPanic)
			}
			convertedPanic, ok := compatibilityError(successorPanic).(*CallbackPanicError) //nolint:errorlint // The top-level facade type is the contract.
			if !ok || convertedPanic.Operation != operation.legacy {
				t.Fatalf("converted panic = %#v", convertedPanic)
			}

			legacyCallback := &CallbackError{Operation: operation.legacy, Err: cause}
			successorCallback := &successor.CallbackError{Operation: operation.successor, Err: cause}
			if legacyCallback.Error() != successorCallback.Error() || !errors.Is(legacyCallback, cause) {
				t.Fatalf("callback error = %q, want %q with original cause", legacyCallback, successorCallback)
			}
			convertedCallback, ok := compatibilityError(successorCallback).(*CallbackError)               //nolint:errorlint // The top-level facade type is the contract.
			if !ok || convertedCallback.Operation != operation.legacy || convertedCallback.Err != cause { //nolint:errorlint // Exact cause identity is required.
				t.Fatalf("converted callback = %#v", convertedCallback)
			}
		})
	}
}

func TestLegacyPublishErrorPreservesEveryAcceptance(t *testing.T) {
	cause := errors.New("cause")
	acceptances := []struct {
		name      string
		legacy    PublishAcceptance
		successor successor.PublishAcceptance
	}{
		{name: "not accepted", legacy: PublishNotAccepted, successor: successor.PublishNotAccepted},
		{name: "accepted", legacy: PublishAccepted, successor: successor.PublishAccepted},
		{name: "unknown", legacy: PublishUnknown, successor: successor.PublishUnknown},
	}
	for _, acceptance := range acceptances {
		t.Run(acceptance.name, func(t *testing.T) {
			legacyError := &PublishError{Acceptance: acceptance.legacy, Err: cause}
			successorError := &successor.PublishError{Acceptance: acceptance.successor, Err: cause}
			if legacyError.Error() != successorError.Error() || !errors.Is(legacyError, cause) {
				t.Fatalf("publish error = %q, want %q with original cause", legacyError, successorError)
			}
			converted, ok := compatibilityError(successorError).(*PublishError) //nolint:errorlint // The top-level facade type is the contract.
			if !ok || converted.Acceptance != acceptance.legacy || !errors.Is(converted.Err, cause) {
				t.Fatalf("converted publish error = %#v", converted)
			}
		})
	}
}

func TestLegacyProducerDelegatesLifecycleAndPublish(t *testing.T) {
	factory := facadeFactory(t)
	producer, err := NewProducer(ProducerOptions[int]{
		Name:        "producer",
		Resource:    7,
		Correlation: factory,
		Startup:     func(context.Context, int) error { return nil },
		Readiness:   func(context.Context, int) error { return nil },
		Publish: func(context.Context, int, core.QueuedMessage, ...job.AllowOption) error {
			return nil
		},
		Shutdown: func(context.Context, int) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}
	if producer.Resource() != 7 {
		t.Fatalf("Resource() = %d", producer.Resource())
	}
	component := producer.Component()
	if err = component.Start(t.Context()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	readiness, ok := producer.Readiness()
	if !ok {
		t.Fatal("Readiness() missing")
	}
	if err = readiness.Run(t.Context()); err != nil {
		t.Fatalf("Readiness() error = %v", err)
	}
	if _, err = producer.Publish(facadeProducerContext(), facadeQueuedPayload("payload")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if err = component.Stop(t.Context()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	withoutReadiness, err := NewProducer(ProducerOptions[int]{
		Name:        "without-readiness",
		Resource:    1,
		Correlation: factory,
		Publish: func(context.Context, int, core.QueuedMessage, ...job.AllowOption) error {
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewProducer() without readiness error = %v", err)
	}
	if _, ok = withoutReadiness.Readiness(); ok {
		t.Fatal("Readiness() unexpectedly present")
	}
}

func TestLegacyProducerDelegatesAcceptancePublish(t *testing.T) {
	producer, err := NewProducer(ProducerOptions[int]{
		Name:        "producer",
		Resource:    1,
		Correlation: facadeFactory(t),
		PublishWithAcceptance: func(
			context.Context,
			int,
			core.QueuedMessage,
			...job.AllowOption,
		) (PublishAcceptance, error) {
			return PublishAccepted, nil
		},
	})
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}
	if err = producer.Component().Start(t.Context()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	_, acceptance, err := producer.PublishWithAcceptance(
		facadeProducerContext(),
		facadeQueuedPayload("payload"),
	)
	if err != nil || acceptance != PublishAccepted {
		t.Fatalf("PublishWithAcceptance() = (%v, %v)", acceptance, err)
	}
}

func TestLegacyHandlerAndLifecycleWorkerDelegate(t *testing.T) {
	handler, err := NewHandler(HandlerOptions{
		Correlation: facadeFactory(t),
		Handler:     func(context.Context, core.TaskMessage) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	if err = handler(t.Context(), facadeQueuedPayload("payload")); err != nil {
		t.Fatalf("handler error = %v", err)
	}

	worker, err := NewLifecycleWorker(LifecycleWorkerOptions[int]{
		Name:           "worker",
		Resource:       9,
		Correlation:    facadeFactory(t),
		Handler:        func(context.Context, core.TaskMessage) error { return nil },
		Startup:        func(context.Context, int) error { return nil },
		Readiness:      func(context.Context, int) error { return nil },
		CloseAdmission: func(int) error { return nil },
		Run:            func(context.Context, int, Handler) error { return nil },
		Shutdown:       func(context.Context, int) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewLifecycleWorker() error = %v", err)
	}
	if worker.Resource() != 9 {
		t.Fatalf("Resource() = %d", worker.Resource())
	}
	plan := worker.Plan()
	if err = plan.Components[0].Start(t.Context()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err = plan.Readiness[0].Run(t.Context()); err != nil {
		t.Fatalf("Readiness() error = %v", err)
	}
	if err = plan.Tasks[0].Run(t.Context()); !errors.Is(err, ErrWorkerExited) {
		t.Fatalf("Run() error = %v, want ErrWorkerExited", err)
	}
	if err = plan.Components[0].CloseAdmission(); err != nil {
		t.Fatalf("CloseAdmission() error = %v", err)
	}
	if err = plan.Components[0].Stop(t.Context()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}
