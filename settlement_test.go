package queue

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/faustbrian/go-queue/job"
	"github.com/stretchr/testify/require"
)

func TestSuccessfulHandlerAcknowledgesDelivery(t *testing.T) {
	var acknowledgements atomic.Int64
	var rejections atomic.Int64
	done := make(chan struct{})
	message := job.NewTask(func(context.Context) error { return nil })
	message.SetAcknowledgement(
		func() error { acknowledgements.Add(1); return nil },
		func() error { rejections.Add(1); return nil },
	)
	q, err := NewQueue(WithWorker(NewRing()), WithAfterFn(func() { close(done) }))
	require.NoError(t, err)

	require.NoError(t, q.queue(&message))
	q.Start()
	require.Eventually(t, func() bool { return channelClosed(done) }, time.Second, time.Millisecond)
	q.Release()

	require.EqualValues(t, 1, acknowledgements.Load())
	require.Zero(t, rejections.Load())
}

func TestFailedHandlerRejectsDelivery(t *testing.T) {
	var acknowledgements atomic.Int64
	var rejections atomic.Int64
	done := make(chan struct{})
	message := job.NewTask(func(context.Context) error { return errors.New("handler failed") })
	message.SetAcknowledgement(
		func() error { acknowledgements.Add(1); return nil },
		func() error { rejections.Add(1); return nil },
	)
	q, err := NewQueue(WithWorker(NewRing()), WithAfterFn(func() { close(done) }))
	require.NoError(t, err)

	require.NoError(t, q.queue(&message))
	q.Start()
	require.Eventually(t, func() bool { return channelClosed(done) }, time.Second, time.Millisecond)
	q.Release()

	require.Zero(t, acknowledgements.Load())
	require.EqualValues(t, 1, rejections.Load())
}

func TestAcknowledgementFailureFailsDeliveryAndEmitsEvent(t *testing.T) {
	observer := &recordingObserver{}
	done := make(chan struct{})
	message := job.NewTask(func(context.Context) error { return nil })
	message.SetAcknowledgement(
		func() error { return errors.New("ack failed") },
		func() error { return nil },
	)
	q, err := NewQueue(
		WithWorker(NewRing()),
		WithObserver(observer),
		WithAfterFn(func() { close(done) }),
	)
	require.NoError(t, err)

	require.NoError(t, q.queue(&message))
	q.Start()
	require.Eventually(t, func() bool { return channelClosed(done) }, time.Second, time.Millisecond)
	q.Release()

	require.EqualValues(t, 1, q.FailureTasks())
	require.Contains(t, observer.kinds(), EventAckFailed)
}

func TestPanickingHandlerRejectsDelivery(t *testing.T) {
	var rejections atomic.Int64
	done := make(chan struct{})
	message := job.NewTask(func(context.Context) error { panic("boom") })
	message.SetAcknowledgement(
		func() error { return nil },
		func() error { rejections.Add(1); return nil },
	)
	q, err := NewQueue(
		WithWorker(NewRing()),
		WithLogger(NewEmptyLogger()),
		WithAfterFn(func() { close(done) }),
	)
	require.NoError(t, err)

	require.NoError(t, q.queue(&message))
	q.Start()
	require.Eventually(t, func() bool { return channelClosed(done) }, time.Second, time.Millisecond)
	q.Release()

	require.EqualValues(t, 1, rejections.Load())
}

func channelClosed(channel <-chan struct{}) bool {
	select {
	case <-channel:
		return true
	default:
		return false
	}
}
