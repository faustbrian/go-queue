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

func TestOptionsConfigureRedisPubSub(t *testing.T) {
	logger := queue.NewEmptyLogger()
	runErr := errors.New("run")
	opts := newOptions(
		WithAddr("redis:6379"),
		WithDB(2),
		WithCluster(),
		WithSentinel(),
		WithTLS(),
		WithSkipTLSVerify(),
		WithMasterName("primary"),
		WithChannelSize(42),
		WithUsername("user"),
		WithPassword("secret"),
		WithConnectionString("redis://redis:6379/2"),
		WithChannel("jobs"),
		WithRunFunc(func(context.Context, core.TaskMessage) error { return runErr }),
		WithLogger(logger),
		WithDebug(),
		WithConnectTimeout(25*time.Millisecond),
	)

	assert.Equal(t, "redis:6379", opts.addr)
	assert.Equal(t, 2, opts.db)
	assert.True(t, opts.cluster)
	assert.True(t, opts.sentinel)
	assert.Equal(t, "primary", opts.masterName)
	assert.Equal(t, 42, opts.channelSize)
	assert.Equal(t, "user", opts.username)
	assert.Equal(t, "secret", opts.password)
	assert.Equal(t, "redis://redis:6379/2", opts.connectionString)
	assert.Equal(t, "jobs", opts.channelName)
	assert.True(t, opts.debug)
	assert.Equal(t, logger, opts.logger)
	assert.Equal(t, 25*time.Millisecond, opts.connectTimeout)
	assert.Equal(t, uint16(tls.VersionTLS12), opts.tls.MinVersion)
	assert.True(t, opts.tls.InsecureSkipVerify)
	assert.ErrorIs(t, opts.runFunc(context.Background(), nil), runErr)
	worker := &Worker{opts: opts}
	assert.Equal(t, "redis-pubsub", worker.BackendName())
	assert.Equal(t, "jobs", worker.QueueName())
}

func TestDefaultRunFunctionSucceeds(t *testing.T) {
	assert.NoError(t, newOptions().runFunc(context.Background(), nil))
}

func TestSkipTLSVerifyCreatesConfig(t *testing.T) {
	opts := newOptions(WithSkipTLSVerify())

	assert.True(t, opts.tls.InsecureSkipVerify)
}

func TestWorkerPublishesRunsReceivesAndShutsDown(t *testing.T) {
	server := miniredis.RunT(t)
	var handled []byte
	worker, err := NewWorkerE(
		WithAddr(server.Addr()),
		WithChannel("jobs"),
		WithChannelSize(2),
		WithRunFunc(func(_ context.Context, task core.TaskMessage) error {
			handled = append([]byte(nil), task.Payload()...)
			return nil
		}),
	)
	require.NoError(t, err)
	message := job.NewMessage(rawMessage("payload"))

	require.NoError(t, worker.Queue(&message))
	received, err := worker.Request()
	require.NoError(t, err)
	assert.Equal(t, []byte("payload"), received.Payload())
	require.NoError(t, worker.Run(context.Background(), received))
	assert.Equal(t, []byte("payload"), handled)
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

func TestWorkerDebugModeConnects(t *testing.T) {
	server := miniredis.RunT(t)
	worker, err := NewWorkerE(WithAddr(server.Addr()), WithDebug())

	require.NoError(t, err)
	require.NoError(t, worker.Shutdown())
}

func TestWorkerConstructorReturnsModeConnectionErrors(t *testing.T) {
	t.Run("cluster", func(t *testing.T) {
		started := time.Now()
		worker, err := NewWorkerE(
			WithAddr("127.0.0.1:1"),
			WithCluster(),
			WithConnectTimeout(20*time.Millisecond),
		)
		assert.Nil(t, worker)
		assert.ErrorContains(t, err, "connect to Redis")
		assert.Less(t, time.Since(started), 250*time.Millisecond)
	})

	t.Run("sentinel", func(t *testing.T) {
		started := time.Now()
		worker, err := NewWorkerE(
			WithAddr("127.0.0.1:1"),
			WithSentinel(),
			WithMasterName("primary"),
			WithConnectTimeout(20*time.Millisecond),
		)
		assert.Nil(t, worker)
		assert.ErrorContains(t, err, "connect to Redis")
		assert.Less(t, time.Since(started), 250*time.Millisecond)
	})
}

func TestLegacyConstructorPanicsOnConnectionError(t *testing.T) {
	assert.Panics(t, func() {
		NewWorker(WithAddr("127.0.0.1:1"), WithConnectTimeout(20*time.Millisecond))
	})
}

func TestRequestReturnsDecodeAndClosedChannelErrors(t *testing.T) {
	t.Run("decode", func(t *testing.T) {
		messages := make(chan *redis.Message, 1)
		messages <- &redis.Message{Payload: "not-json"}
		worker := &Worker{channel: messages, opts: newOptions()}

		message, err := worker.Request()
		assert.Nil(t, message)
		assert.Error(t, err)
	})

	t.Run("closed", func(t *testing.T) {
		messages := make(chan *redis.Message)
		close(messages)
		worker := &Worker{channel: messages, opts: newOptions()}

		message, err := worker.Request()
		assert.Nil(t, message)
		assert.ErrorIs(t, err, queue.ErrQueueHasBeenClosed)
	})
}

func TestRequestUsesConfiguredTimeout(t *testing.T) {
	worker := &Worker{
		channel: make(chan *redis.Message),
		opts:    newOptions(WithRequestTimeout(time.Millisecond)),
	}

	started := time.Now()
	message, err := worker.Request()

	assert.Nil(t, message)
	assert.ErrorIs(t, err, queue.ErrNoTaskInQueue)
	assert.Less(t, time.Since(started), 100*time.Millisecond)
}

func TestClusterWorkerShutdownClosesResources(t *testing.T) {
	client := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{"127.0.0.1:1"}})
	worker := &Worker{
		rdb:    client,
		pubsub: client.Subscribe(context.Background(), "jobs"),
		stop:   make(chan struct{}),
		opts:   newOptions(),
	}

	require.NoError(t, worker.Shutdown())
}

func TestSubscribeRedisSupportsStandaloneAndClusterClients(t *testing.T) {
	ctx := context.Background()
	standalone := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	cluster := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{"127.0.0.1:1"}})
	ring := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{"local": "127.0.0.1:1"},
	})

	standaloneSubscription := subscribeRedis(ctx, standalone, "jobs")
	clusterSubscription := subscribeRedis(ctx, cluster, "jobs")

	assert.NotNil(t, standaloneSubscription)
	assert.NotNil(t, clusterSubscription)
	assert.Nil(t, subscribeRedis(ctx, ring, "jobs"))
	require.NoError(t, standaloneSubscription.Close())
	require.NoError(t, clusterSubscription.Close())
	require.NoError(t, standalone.Close())
	require.NoError(t, cluster.Close())
	require.NoError(t, ring.Close())
}

func TestWorkerReturnsSubscriptionValidationError(t *testing.T) {
	server := miniredis.RunT(t)
	expected := errors.New("subscription unavailable")
	original := pingRedisSubscription
	pingRedisSubscription = func(context.Context, *redis.PubSub) error {
		return expected
	}
	t.Cleanup(func() { pingRedisSubscription = original })

	worker, err := NewWorkerE(WithAddr(server.Addr()))

	assert.Nil(t, worker)
	assert.ErrorIs(t, err, expected)
}

func TestQueueReturnsPublishError(t *testing.T) {
	server := miniredis.RunT(t)
	worker, err := NewWorkerE(WithAddr(server.Addr()))
	require.NoError(t, err)
	require.NoError(t, worker.rdb.(*redis.Client).Close())
	message := job.NewMessage(rawMessage("payload"))

	assert.Error(t, worker.Queue(&message))
	require.NoError(t, worker.Shutdown())
}

type rawMessage string

func (m rawMessage) Bytes() []byte { return []byte(m) }
