//go:build integration

package redisdb

import (
	"context"
	"io"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/faustbrian/go-queue"
	"github.com/faustbrian/go-queue/core"
	"github.com/faustbrian/go-queue/job"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func setupRedisClusterContainer(ctx context.Context, t *testing.T) (testcontainers.Container, string) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	hostPort := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	require.NoError(t, listener.Close())
	req := testcontainers.ContainerRequest{
		Image:        "redis:6.2.22@sha256:3b477db2f54035771360d023c9aff4c6255ba833834511b8eedc5ba8c10d0bce",
		ExposedPorts: []string{"6379/tcp"},
		Cmd: []string{
			"redis-server", "--cluster-enabled", "yes",
			"--cluster-config-file", "nodes.conf",
			"--cluster-node-timeout", "1000", "--appendonly", "no",
			"--cluster-announce-ip", "127.0.0.1",
			"--cluster-announce-port", hostPort,
		},
		HostConfigModifier: func(config *container.HostConfig) {
			config.PortBindings = network.PortMap{
				network.MustParsePort("6379/tcp"): {{
					HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: hostPort,
				}},
			}
		},
		WaitingFor: wait.NewExecStrategy(
			[]string{"redis-cli", "-h", "localhost", "-p", "6379", "ping"},
		),
	}
	redisC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)
	for _, command := range [][]string{
		{"sh", "-c", "redis-cli CLUSTER ADDSLOTS $(seq 0 16383)"},
	} {
		exitCode, _, execErr := redisC.Exec(ctx, command)
		require.NoError(t, execErr)
		require.Zero(t, exitCode)
	}
	require.Eventually(t, func() bool {
		exitCode, output, execErr := redisC.Exec(ctx, []string{"redis-cli", "CLUSTER", "INFO"})
		if execErr != nil || exitCode != 0 {
			return false
		}
		body, readErr := io.ReadAll(output)
		return readErr == nil && strings.Contains(string(body), "cluster_state:ok")
	}, 5*time.Second, 100*time.Millisecond)

	endpoint, err := redisC.Endpoint(ctx, "")
	require.NoError(t, err)

	return redisC, endpoint
}

func setupRedisContainer(ctx context.Context, t *testing.T) (testcontainers.Container, string) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	hostPort := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	require.NoError(t, listener.Close())
	req := testcontainers.ContainerRequest{
		Image:        "redis:6.2.22@sha256:3b477db2f54035771360d023c9aff4c6255ba833834511b8eedc5ba8c10d0bce",
		ExposedPorts: []string{"6379/tcp"},
		HostConfigModifier: func(config *container.HostConfig) {
			config.PortBindings = network.PortMap{
				network.MustParsePort("6379/tcp"): {{
					HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: hostPort,
				}},
			}
		},
		WaitingFor: wait.NewExecStrategy(
			[]string{"redis-cli", "-h", "localhost", "-p", "6379", "ping"},
		),
	}
	redisC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	endpoint, err := redisC.Endpoint(ctx, "")
	require.NoError(t, err)

	return redisC, endpoint
}

func TestWithRedis(t *testing.T) {
	ctx := context.Background()
	redisC, _ := setupRedisContainer(ctx, t)
	testcontainers.CleanupContainer(t, redisC)
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
	}, 5*time.Second, time.Millisecond)
}

func waitForSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for signal")
	}
}

func TestRedisDefaultFlow(t *testing.T) {
	ctx := context.Background()
	redisC, endpoint := setupRedisContainer(ctx, t)
	defer testcontainers.CleanupContainer(t, redisC)

	m := &mockMessage{
		Message: "foo",
	}
	w := NewWorker(
		WithAddr(endpoint),
		WithStreamName("test"),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	assert.NoError(t, err)
	assert.NoError(t, q.Queue(m))
	q.Start()
	waitForCompleted(t, q, 1)
	q.Release()
}

func TestRedisStreamBacklogSurvivesBrokerRestart(t *testing.T) {
	ctx := context.Background()
	redisC, endpoint := setupRedisContainer(ctx, t)
	defer testcontainers.CleanupContainer(t, redisC)

	worker := NewWorker(
		WithAddr(endpoint),
		WithStreamName("restart"),
		WithConnectTimeout(250*time.Millisecond),
	)
	q, err := queue.NewQueue(queue.WithWorker(worker), queue.WithWorkerCount(1))
	require.NoError(t, err)
	require.NoError(t, q.Queue(mockMessage{Message: "queued-before-restart"}))

	stopTimeout := time.Second
	require.NoError(t, redisC.Stop(ctx, &stopTimeout))
	require.NoError(t, redisC.Start(ctx))
	q.Start()
	waitForCompleted(t, q, 1)
	q.Release()
}

func TestRedisShutdown(t *testing.T) {
	ctx := context.Background()
	redisC, endpoint := setupRedisContainer(ctx, t)
	defer testcontainers.CleanupContainer(t, redisC)
	w := NewWorker(
		WithAddr(endpoint),
		WithStreamName("test2"),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	q.Shutdown()
	// check shutdown once
	assert.Error(t, w.Shutdown())
	assert.Equal(t, queue.ErrQueueShutdown, w.Shutdown())
	q.Wait()
}

func TestCustomFuncAndWait(t *testing.T) {
	ctx := context.Background()
	redisC, endpoint := setupRedisContainer(ctx, t)
	defer testcontainers.CleanupContainer(t, redisC)
	m := &mockMessage{
		Message: "foo",
	}
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	w := NewWorker(
		WithAddr(endpoint),
		WithStreamName("test3"),
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
	q := queue.NewPool(
		5,
		queue.WithWorker(w),
	)
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	waitForSignal(t, started)
	waitForSignal(t, started)
	waitForSignal(t, started)
	waitForSignal(t, started)
	close(release)
	waitForCompleted(t, q, 4)
	q.Release()
}

func TestRedisCluster(t *testing.T) {
	ctx := context.Background()
	redisC, endpoint := setupRedisClusterContainer(ctx, t)
	defer testcontainers.CleanupContainer(t, redisC)

	m := &mockMessage{
		Message: "foo",
	}
	started := make(chan struct{}, 4)
	release := make(chan struct{})

	w := NewWorker(
		WithAddr(endpoint),
		WithStreamName("testCluster"),
		WithCluster(),
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
	q := queue.NewPool(
		5,
		queue.WithWorker(w),
	)
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	waitForSignal(t, started)
	waitForSignal(t, started)
	waitForSignal(t, started)
	waitForSignal(t, started)
	close(release)
	waitForCompleted(t, q, 4)
	q.Release()
}

func TestEnqueueJobAfterShutdown(t *testing.T) {
	ctx := context.Background()
	redisC, endpoint := setupRedisContainer(ctx, t)
	defer testcontainers.CleanupContainer(t, redisC)
	m := mockMessage{
		Message: "foo",
	}
	w := NewWorker(
		WithAddr(endpoint),
	)
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
	ctx := context.Background()
	redisC, endpoint := setupRedisContainer(ctx, t)
	defer testcontainers.CleanupContainer(t, redisC)
	m := mockMessage{
		Message: "foo",
	}
	started := make(chan struct{}, 1)
	deadline := make(chan error, 2)
	w := NewWorker(
		WithAddr(endpoint),
		WithStreamName("timeout"),
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
	ctx := context.Background()
	redisC, endpoint := setupRedisContainer(ctx, t)
	defer testcontainers.CleanupContainer(t, redisC)
	m := mockMessage{
		Message: "test",
	}
	started := make(chan struct{}, 1)
	canceled := make(chan error, 1)
	w := NewWorker(
		WithAddr(endpoint),
		WithStreamName("cancel"),
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
	ctx := context.Background()
	redisC, endpoint := setupRedisContainer(ctx, t)
	defer testcontainers.CleanupContainer(t, redisC)
	m := mockMessage{
		Message: "foo",
	}
	w := NewWorker(
		WithAddr(endpoint),
		WithStreamName("GoroutineLeak"),
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
	for i := 0; i < 50; i++ {
		assert.NoError(t, q.Queue(m))
	}
	waitForCompleted(t, q, 50)
	q.Release()
}

func TestGoroutinePanic(t *testing.T) {
	ctx := context.Background()
	redisC, endpoint := setupRedisContainer(ctx, t)
	defer testcontainers.CleanupContainer(t, redisC)
	m := mockMessage{
		Message: "foo",
	}
	panicked := make(chan struct{}, 2)
	w := NewWorker(
		WithAddr(endpoint),
		WithStreamName("GoroutinePanic"),
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
