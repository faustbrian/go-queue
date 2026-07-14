//go:build integration

package rabbitmq

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"testing"
	"time"

	"github.com/faustbrian/go-queue"
	"github.com/faustbrian/go-queue/core"
	"github.com/faustbrian/go-queue/job"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type mockMessage struct {
	Message string
}

func (m mockMessage) Bytes() []byte {
	return []byte(m.Message)
}

func waitForCompleted(t *testing.T, q *queue.Queue, count uint64) {
	t.Helper()
	require.Eventually(t, func() bool {
		return q.CompletedTasks() == count
	}, 10*time.Second, time.Millisecond)
}

func waitForSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for signal")
	}
}

func setupRabbitMQContainer(ctx context.Context, t *testing.T) (testcontainers.Container, string) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	hostPort := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	require.NoError(t, listener.Close())
	req := testcontainers.ContainerRequest{
		Image: "rabbitmq:3.13.7-management@sha256:e582c0bc7766f3342496d8485efb5a1df782b5ce3886ad017e2eaae442311f69",
		ExposedPorts: []string{
			"4369/tcp", // epmd
			"5672/tcp", // amqp
		},
		HostConfigModifier: func(config *container.HostConfig) {
			config.PortBindings = network.PortMap{
				network.MustParsePort("5672/tcp"): {{
					HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: hostPort,
				}},
			}
		},
		WaitingFor: wait.ForLog("Server startup complete"),
		Env: map[string]string{
			"RABBITMQ_DEFAULT_USER": "guest",
			"RABBITMQ_DEFAULT_PASS": "guest",
		},
	}
	rabbitMQC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	endpoint, err := rabbitMQC.PortEndpoint(ctx, "5672/tcp", "")
	require.NoError(t, err)

	return rabbitMQC, endpoint
}

func newRabbitWorker(t *testing.T, opts ...Option) *Worker {
	t.Helper()
	ctx := context.Background()
	container, endpoint := setupRabbitMQContainer(ctx, t)
	testcontainers.CleanupContainer(t, container)
	return NewWorker(append(
		[]Option{WithAddr(fmt.Sprintf("amqp://guest:guest@%s/", endpoint))},
		opts...,
	)...)
}

func TestShutdownWorkFlow(t *testing.T) {
	w := newRabbitWorker(t,
		WithQueue("test"),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	q.Shutdown()
	// check shutdown once
	q.Shutdown()
	q.Wait()
}

func TestBrokerRestartRequiresReplacementWorker(t *testing.T) {
	ctx := context.Background()
	rabbitMQC, endpoint := setupRabbitMQContainer(ctx, t)
	defer testcontainers.CleanupContainer(t, rabbitMQC)
	address := fmt.Sprintf("amqp://guest:guest@%s/", endpoint)

	worker := NewWorker(WithAddr(address), WithQueue("restart"))
	q, err := queue.NewQueue(queue.WithWorker(worker), queue.WithWorkerCount(1))
	require.NoError(t, err)
	q.Start()
	require.NoError(t, q.Queue(mockMessage{Message: "before-restart"}))
	waitForCompleted(t, q, 1)

	stopTimeout := time.Second
	require.NoError(t, rabbitMQC.Stop(ctx, &stopTimeout))
	require.NoError(t, rabbitMQC.Start(ctx))
	assert.Error(t, q.Queue(mockMessage{Message: "old-worker-after-restart"}))
	q.Release()

	replacement := NewWorker(WithAddr(address), WithQueue("restart"))
	replacementQueue, err := queue.NewQueue(
		queue.WithWorker(replacement),
		queue.WithWorkerCount(1),
	)
	require.NoError(t, err)
	replacementQueue.Start()
	require.NoError(t, replacementQueue.Queue(mockMessage{Message: "replacement-worker"}))
	waitForCompleted(t, replacementQueue, 1)
	replacementQueue.Release()
}

func TestCustomFuncAndWait(t *testing.T) {
	m := &mockMessage{
		Message: "foo",
	}
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	w := newRabbitWorker(t,
		WithQueue("test"),
		WithRunFunc(func(ctx context.Context, m core.TaskMessage) error {
			started <- struct{}{}
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	waitForSignal(t, started)
	waitForSignal(t, started)
	close(release)
	waitForCompleted(t, q, 4)
	q.Release()
}

func TestEnqueueJobAfterShutdown(t *testing.T) {
	m := mockMessage{
		Message: "foo",
	}
	w := newRabbitWorker(t)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	q.Shutdown()
	// can't queue task after shutdown
	err = q.Queue(m)
	assert.Error(t, err)
	assert.Equal(t, queue.ErrQueueShutdown, err)
	q.Wait()
}

func TestJobReachTimeout(t *testing.T) {
	m := mockMessage{
		Message: "foo",
	}
	started := make(chan struct{}, 1)
	deadline := make(chan error, 2)
	w := newRabbitWorker(t,
		WithQueue("JobReachTimeout"),
		WithRunFunc(func(ctx context.Context, m core.TaskMessage) error {
			started <- struct{}{}
			<-ctx.Done()
			deadline <- ctx.Err()
			return ctx.Err()
		}),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	assert.NoError(t, q.Queue(m, job.AllowOption{
		Timeout: job.Time(20 * time.Millisecond),
	}))
	waitForSignal(t, started)
	assert.ErrorIs(t, <-deadline, context.DeadlineExceeded)
	q.Shutdown()
	q.Wait()
	assert.GreaterOrEqual(t, q.CompletedTasks(), uint64(1))
}

func TestCancelJobAfterShutdown(t *testing.T) {
	m := mockMessage{
		Message: "test",
	}
	started := make(chan struct{}, 1)
	canceled := make(chan error, 1)
	w := newRabbitWorker(t,
		WithQueue("CancelJob"),
		WithLogger(queue.NewLogger()),
		WithRunFunc(func(ctx context.Context, m core.TaskMessage) error {
			close(started)
			<-ctx.Done()
			canceled <- ctx.Err()
			return ctx.Err()
		}),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	assert.NoError(t, q.Queue(m, job.AllowOption{
		Timeout: job.Time(time.Minute),
	}))
	waitForSignal(t, started)
	q.Shutdown()
	assert.ErrorIs(t, <-canceled, context.Canceled)
	q.Wait()
}

func TestGoroutineLeak(t *testing.T) {
	m := mockMessage{
		Message: "foo",
	}
	w := newRabbitWorker(t,
		WithQueue("GoroutineLeak"),
		WithLogger(queue.NewEmptyLogger()),
		WithRunFunc(func(ctx context.Context, m core.TaskMessage) error {
			return nil
		}),
	)
	q, err := queue.NewQueue(
		queue.WithLogger(queue.NewEmptyLogger()),
		queue.WithWorker(w),
		queue.WithWorkerCount(10),
	)
	assert.NoError(t, err)
	q.Start()
	for i := 0; i < 500; i++ {
		assert.NoError(t, q.Queue(m))
	}
	waitForCompleted(t, q, 500)
	q.Release()
}

func TestGoroutinePanic(t *testing.T) {
	m := mockMessage{
		Message: "foo",
	}
	panicked := make(chan struct{}, 2)
	w := newRabbitWorker(t,
		WithQueue("GoroutinePanic"),
		WithRoutingKey("GoroutinePanic"),
		WithExchangeName("GoroutinePanic"),
		WithRunFunc(func(ctx context.Context, m core.TaskMessage) error {
			panicked <- struct{}{}
			panic("missing something")
		}),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	waitForSignal(t, panicked)
	waitForSignal(t, panicked)
	q.Shutdown()
	q.Wait()
	assert.GreaterOrEqual(t, q.FailureTasks(), uint64(2))
	assert.Error(t, q.Queue(m))
}
