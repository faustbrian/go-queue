package redisdb

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	queue "github.com/faustbrian/go-queue"
	"github.com/faustbrian/go-queue/core"
	"github.com/faustbrian/go-queue/job"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOptionsConfigureRedisStreams(t *testing.T) {
	logger := queue.NewEmptyLogger()
	runErr := errors.New("run")
	opts := newOptions(
		WithAddr("redis:6379"),
		WithDB(2),
		WithCluster(),
		WithTLS(),
		WithSkipTLSVerify(),
		WithMaxLength(42),
		WithBlockTime(25*time.Millisecond),
		WithStreamName("jobs"),
		WithGroup("workers"),
		WithConsumer("worker-1"),
		WithUsername("user"),
		WithPassword("secret"),
		WithConnectionString("redis://redis:6379/2"),
		WithRunFunc(func(context.Context, core.TaskMessage) error { return runErr }),
		WithLogger(logger),
		WithRequestTimeout(30*time.Millisecond),
		WithConnectTimeout(35*time.Millisecond),
	)

	assert.Equal(t, "redis:6379", opts.addr)
	assert.Equal(t, 2, opts.db)
	assert.True(t, opts.cluster)
	assert.Equal(t, int64(42), opts.maxLength)
	assert.Equal(t, 25*time.Millisecond, opts.blockTime)
	assert.Equal(t, "jobs", opts.streamName)
	assert.Equal(t, "workers", opts.group)
	assert.Equal(t, "worker-1", opts.consumer)
	assert.Equal(t, "user", opts.username)
	assert.Equal(t, "secret", opts.password)
	assert.Equal(t, "redis://redis:6379/2", opts.connectionString)
	assert.Equal(t, logger, opts.logger)
	assert.Equal(t, 30*time.Millisecond, opts.requestTimeout)
	assert.Equal(t, 35*time.Millisecond, opts.connectTimeout)
	assert.Equal(t, uint16(tls.VersionTLS12), opts.tls.MinVersion)
	assert.True(t, opts.tls.InsecureSkipVerify)
	assert.ErrorIs(t, opts.runFunc(context.Background(), nil), runErr)
}

func TestSkipTLSVerifyCreatesConfig(t *testing.T) {
	assert.True(t, newOptions(WithSkipTLSVerify()).tls.InsecureSkipVerify)
}

func TestDefaultRunFunctionSucceeds(t *testing.T) {
	assert.NoError(t, newOptions().runFunc(context.Background(), nil))
}

func TestWorkerQueuesRequestsAcknowledgesRunsAndShutsDown(t *testing.T) {
	server := miniredis.RunT(t)
	var handled []byte
	worker, err := NewWorkerE(
		WithAddr(server.Addr()),
		WithStreamName("jobs"),
		WithGroup("workers"),
		WithConsumer("worker-1"),
		WithBlockTime(time.Millisecond),
		WithRequestTimeout(time.Second),
		WithRunFunc(func(_ context.Context, task core.TaskMessage) error {
			handled = append([]byte(nil), task.Payload()...)
			return nil
		}),
	)
	require.NoError(t, err)
	worker.startConsumer()
	message := job.NewMessage(rawMessage("payload"))

	require.NoError(t, worker.Queue(&message))
	received, err := worker.Request()
	require.NoError(t, err)
	assert.Equal(t, []byte("payload"), received.Payload())
	require.NoError(t, worker.Run(context.Background(), received))
	assert.Equal(t, []byte("payload"), handled)
	require.NoError(t, received.(*job.Message).Nack())
	require.NoError(t, received.(*job.Message).Ack())
	require.NoError(t, worker.Shutdown())
	assert.ErrorIs(t, worker.Shutdown(), queue.ErrQueueShutdown)
	assert.ErrorIs(t, worker.Queue(&message), queue.ErrQueueShutdown)
}

func TestWorkerConnectsWithConnectionString(t *testing.T) {
	server := miniredis.RunT(t)
	worker, err := NewWorkerE(WithConnectionString("redis://" + server.Addr() + "/0"))
	require.NoError(t, err)
	require.NoError(t, worker.Shutdown())
}

func TestLegacyConstructorReturnsConnectedWorker(t *testing.T) {
	server := miniredis.RunT(t)
	worker := NewWorker(WithAddr(server.Addr()))

	require.NoError(t, worker.Shutdown())
}

func TestWorkerConstructorsReturnConnectionErrors(t *testing.T) {
	started := time.Now()
	worker, err := NewWorkerE(
		WithAddr("127.0.0.1:1"),
		WithConnectTimeout(20*time.Millisecond),
	)
	assert.Nil(t, worker)
	assert.ErrorContains(t, err, "connect to Redis")
	assert.Less(t, time.Since(started), 250*time.Millisecond)

	assert.Panics(t, func() {
		NewWorker(WithAddr("127.0.0.1:1"), WithConnectTimeout(20*time.Millisecond))
	})
}

func TestClusterConstructorReturnsBoundedConnectionError(t *testing.T) {
	started := time.Now()
	worker, err := NewWorkerE(
		WithAddr("127.0.0.1:1"),
		WithCluster(),
		WithConnectTimeout(20*time.Millisecond),
	)
	assert.Nil(t, worker)
	assert.ErrorContains(t, err, "connect to Redis")
	assert.Less(t, time.Since(started), 250*time.Millisecond)
}

func TestRequestReturnsPayloadAndChannelErrors(t *testing.T) {
	t.Run("malformed body", func(t *testing.T) {
		worker := workerWithTask(redis.XMessage{
			ID: "1-0", Values: map[string]any{"body": "not-json"},
		})

		message, err := worker.Request()
		assert.Nil(t, message)
		assert.Error(t, err)
	})

	t.Run("missing body", func(t *testing.T) {
		worker := workerWithTask(redis.XMessage{ID: "1-0", Values: map[string]any{}})

		message, err := worker.Request()
		assert.Nil(t, message)
		assert.Error(t, err)
	})

	t.Run("closed", func(t *testing.T) {
		tasks := make(chan redis.XMessage)
		close(tasks)
		worker := &Worker{tasks: tasks, opts: newOptions()}
		worker.startOnce.Do(func() {})

		message, err := worker.Request()
		assert.Nil(t, message)
		assert.ErrorIs(t, err, queue.ErrQueueHasBeenClosed)
	})
}

func TestRequestUsesConfiguredTimeout(t *testing.T) {
	worker := &Worker{
		tasks: make(chan redis.XMessage),
		opts:  newOptions(WithRequestTimeout(time.Millisecond)),
	}
	worker.startOnce.Do(func() {})

	started := time.Now()
	message, err := worker.Request()

	assert.Nil(t, message)
	assert.ErrorIs(t, err, queue.ErrNoTaskInQueue)
	assert.Less(t, time.Since(started), 100*time.Millisecond)
}

func TestClusterWorkerShutdownClosesResources(t *testing.T) {
	client := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{"127.0.0.1:1"}})
	worker := &Worker{
		rdb:   client,
		tasks: make(chan redis.XMessage),
		stop:  make(chan struct{}),
		exit:  make(chan struct{}),
		opts:  newOptions(),
	}

	require.NoError(t, worker.Shutdown())
}

func TestStartConsumerLogsExistingGroupError(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	require.NoError(t, client.XGroupCreateMkStream(
		context.Background(), "jobs", "workers", "$",
	).Err())
	worker, err := NewWorkerE(
		WithAddr(server.Addr()),
		WithStreamName("jobs"),
		WithGroup("workers"),
		WithBlockTime(time.Millisecond),
		WithLogger(queue.NewEmptyLogger()),
	)
	require.NoError(t, err)

	worker.startConsumer()
	require.NoError(t, worker.Shutdown())
	require.NoError(t, client.Close())
}

func TestFetchTaskHandlesReadErrors(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "empty stream", err: redis.Nil},
		{name: "backend failure", err: errors.New("read failed")},
	} {
		t.Run(test.name, func(t *testing.T) {
			stop := make(chan struct{})
			worker := &Worker{
				stop: stop,
				opts: newOptions(WithLogger(queue.NewEmptyLogger())),
				readGroup: func(context.Context, *redis.XReadGroupArgs) ([]redis.XStream, error) {
					close(stop)
					return nil, test.err
				},
			}

			worker.fetchTask()
		})
	}
}

func TestFetchTaskRequeuesDeliveryDuringShutdown(t *testing.T) {
	for _, test := range []struct {
		name        string
		closeClient bool
	}{
		{name: "requeue succeeds"},
		{name: "requeue fails", closeClient: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := miniredis.RunT(t)
			client := redis.NewClient(&redis.Options{Addr: server.Addr()})
			if test.closeClient {
				require.NoError(t, client.Close())
			}
			stop := make(chan struct{})
			exit := make(chan struct{})
			worker := &Worker{
				rdb:   client,
				tasks: make(chan redis.XMessage),
				stop:  stop,
				exit:  exit,
				opts: newOptions(
					WithStreamName("jobs"),
					WithLogger(queue.NewEmptyLogger()),
				),
				readGroup: func(context.Context, *redis.XReadGroupArgs) ([]redis.XStream, error) {
					close(stop)
					return []redis.XStream{{Messages: []redis.XMessage{{
						ID: "1-0", Values: map[string]any{"body": "payload"},
					}}}}, nil
				},
			}

			worker.fetchTask()
			select {
			case <-exit:
			default:
				t.Fatal("fetchTask did not signal requeue completion")
			}
			if !test.closeClient {
				require.NoError(t, client.Close())
			}
		})
	}
}

func workerWithTask(task redis.XMessage) *Worker {
	tasks := make(chan redis.XMessage, 1)
	tasks <- task
	worker := &Worker{
		tasks: tasks,
		ack:   func(string) error { return nil },
		opts:  newOptions(),
	}
	worker.startOnce.Do(func() {})
	return worker
}

type rawMessage string

func (m rawMessage) Bytes() []byte { return []byte(m) }
