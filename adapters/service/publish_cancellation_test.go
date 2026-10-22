package queueservice

import (
	"context"
	"errors"
	"testing"

	"github.com/faustbrian/go-queue/core"
	"github.com/faustbrian/go-queue/job"
)

// A publish the backend already accepted must not be reported as failed
// because the callback's context was cancelled afterwards: the caller would
// retry and duplicate the message.
func TestPublishKeepsCallbackOutcomeWhenCallbackCancelsContext(t *testing.T) {
	cause := errors.New("callback-triggered cancellation")
	t.Run("publish", func(t *testing.T) {
		var cancel context.CancelCauseFunc
		producer, err := NewProducer(ProducerOptions[int]{
			Name:        "producer",
			Resource:    1,
			Correlation: mustFactory(t),
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
		ctx, cancelContext := context.WithCancelCause(producerContext())
		cancel = cancelContext
		if _, err = producer.Publish(ctx, queuedPayload("payload")); err != nil {
			t.Fatalf("Publish() error = %v, want callback success", err)
		}
	})

	t.Run("publish with acceptance", func(t *testing.T) {
		var cancel context.CancelCauseFunc
		producer, err := NewProducer(ProducerOptions[int]{
			Name:        "producer",
			Resource:    1,
			Correlation: mustFactory(t),
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
		ctx, cancelContext := context.WithCancelCause(producerContext())
		cancel = cancelContext
		_, acceptance, publishErr := producer.PublishWithAcceptance(ctx, queuedPayload("payload"))
		if publishErr != nil || acceptance != PublishAccepted {
			t.Fatalf("PublishWithAcceptance() = (%v, %v), want accepted callback success", acceptance, publishErr)
		}
	})

	t.Run("callback failure is not masked", func(t *testing.T) {
		callbackFailure := errors.New("callback failure")
		var cancel context.CancelCauseFunc
		producer, err := NewProducer(ProducerOptions[int]{
			Name:        "producer",
			Resource:    1,
			Correlation: mustFactory(t),
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
		ctx, cancelContext := context.WithCancelCause(producerContext())
		cancel = cancelContext
		_, err = producer.Publish(ctx, queuedPayload("payload"))
		if !errors.Is(err, callbackFailure) || errors.Is(err, cause) {
			t.Fatalf("Publish() error = %v, want the callback failure", err)
		}
	})
}

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

// Publishing observes the caller's cancellation once, before the callback;
// a second observation could turn an accepted publish into a reported failure.
func TestPublishObservesCancellationOnce(t *testing.T) {
	publishCalls := 0
	producer, err := NewProducer(ProducerOptions[int]{
		Name:        "producer",
		Resource:    1,
		Correlation: mustFactory(t),
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
	racing := errors.New("caller-owned racing cancellation cause")
	base, cancel := context.WithCancelCause(producerContext())
	ctx := &causeOnSecondErrContext{Context: base, cancel: func() { cancel(racing) }}
	if _, err = producer.Publish(ctx, queuedPayload("payload")); err != nil {
		t.Fatalf("Publish() error = %v, want successful result", err)
	}
	if publishCalls != 1 || ctx.calls != 1 {
		t.Fatalf("Publish() calls = (%d callback, %d context), want (1, 1)", publishCalls, ctx.calls)
	}
	base, cancel = context.WithCancelCause(producerContext())
	ctx = &causeOnSecondErrContext{Context: base, cancel: func() { cancel(racing) }}
	if _, acceptance, publishErr := producer.PublishWithAcceptance(ctx, queuedPayload("payload")); acceptance != PublishAccepted || publishErr != nil {
		t.Fatalf("PublishWithAcceptance() = (%v, %v), want accepted result", acceptance, publishErr)
	}
	if publishCalls != 2 || ctx.calls != 1 {
		t.Fatalf("PublishWithAcceptance() calls = (%d callback, %d context), want (2, 1)", publishCalls, ctx.calls)
	}
}
