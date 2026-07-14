package redisdb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/faustbrian/go-queue"
	"github.com/faustbrian/go-queue/core"
	"github.com/faustbrian/go-queue/job"

	"github.com/redis/go-redis/v9"
	"github.com/yassinebenaid/godump"
)

var _ core.Worker = (*Worker)(nil)
var _ core.WorkerMetadata = (*Worker)(nil)

var pingRedisSubscription = func(ctx context.Context, subscription *redis.PubSub) error {
	return subscription.Ping(ctx)
}

// BackendName identifies Redis Pub/Sub in lifecycle events.
func (*Worker) BackendName() string { return "redis-pubsub" }

// QueueName returns the configured Redis channel.
func (w *Worker) QueueName() string { return w.opts.channelName }

// Worker for Redis
type Worker struct {
	// redis config
	rdb      redis.Cmdable
	pubsub   *redis.PubSub
	channel  <-chan *redis.Message
	stopFlag int32
	stopOnce sync.Once
	stop     chan struct{}
	opts     options
}

// NewWorker creates a new Worker instance with the provided options.
// It initializes a Redis client based on the options and establishes a connection to the Redis server.
// The Worker is responsible for subscribing to a Redis channel and receiving messages from it.
// It returns the created Worker instance.
func NewWorker(opts ...Option) *Worker {
	w, err := NewWorkerE(opts...)
	if err != nil {
		panic(err)
	}

	return w
}

// NewWorkerE creates a worker and returns connection and configuration errors.
func NewWorkerE(opts ...Option) (*Worker, error) {
	var err error
	w := &Worker{
		opts: newOptions(opts...),
		stop: make(chan struct{}),
	}

	if w.opts.debug {
		_ = godump.Dump(w.opts)
	}

	options := &redis.Options{
		Addr:                  w.opts.addr,
		Username:              w.opts.username,
		Password:              w.opts.password,
		DB:                    w.opts.db,
		TLSConfig:             w.opts.tls,
		DialTimeout:           w.opts.connectTimeout,
		DialerRetries:         -1,
		MaxRetries:            -1,
		ContextTimeoutEnabled: true,
	}

	if w.opts.connectionString != "" {
		options, err = redis.ParseURL(w.opts.connectionString)
		if err != nil {
			return nil, fmt.Errorf("parse Redis connection string: %w", err)
		}
		options.DialTimeout = w.opts.connectTimeout
		options.DialerRetries = -1
		options.MaxRetries = -1
		options.ContextTimeoutEnabled = true
	}

	switch {
	case w.opts.sentinel:
		w.rdb = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:            w.opts.masterName,
			SentinelAddrs:         strings.Split(w.opts.addr, ","),
			Username:              w.opts.username,
			Password:              w.opts.password,
			DB:                    w.opts.db,
			TLSConfig:             w.opts.tls,
			DialTimeout:           w.opts.connectTimeout,
			DialerRetries:         -1,
			MaxRetries:            -1,
			ContextTimeoutEnabled: true,
		})
	case w.opts.cluster:
		w.rdb = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:                 strings.Split(w.opts.addr, ","),
			Username:              w.opts.username,
			Password:              w.opts.password,
			TLSConfig:             w.opts.tls,
			DialTimeout:           w.opts.connectTimeout,
			DialerRetries:         -1,
			MaxRedirects:          -1,
			ContextTimeoutEnabled: true,
		})
	default:
		w.rdb = redis.NewClient(options)
	}

	ctx, cancel := context.WithTimeout(context.Background(), w.opts.connectTimeout)
	defer cancel()
	_, err = w.rdb.Ping(ctx).Result()
	if err != nil {
		closeRedisClient(w.rdb)
		return nil, fmt.Errorf("connect to Redis: %w", err)
	}

	ctx = context.Background()

	w.pubsub = subscribeRedis(ctx, w.rdb, w.opts.channelName)

	var ropts []redis.ChannelOption

	if w.opts.channelSize > 1 {
		ropts = append(ropts, redis.WithChannelSize(w.opts.channelSize))
	}

	w.channel = w.pubsub.Channel(ropts...)
	// make sure the connection is successful
	if err := pingRedisSubscription(ctx, w.pubsub); err != nil {
		_ = w.pubsub.Close()
		closeRedisClient(w.rdb)
		return nil, fmt.Errorf("subscribe to Redis channel: %w", err)
	}

	return w, nil
}

func subscribeRedis(ctx context.Context, client redis.Cmdable, channel string) *redis.PubSub {
	switch value := client.(type) {
	case *redis.Client:
		return value.Subscribe(ctx, channel)
	case *redis.ClusterClient:
		return value.Subscribe(ctx, channel)
	default:
		return nil
	}
}

func closeRedisClient(client redis.Cmdable) {
	switch value := client.(type) {
	case *redis.Client:
		_ = value.Close()
	case *redis.ClusterClient:
		_ = value.Close()
	}
}

// Run to execute new task
func (w *Worker) Run(ctx context.Context, task core.TaskMessage) error {
	return w.opts.runFunc(ctx, task)
}

// Shutdown worker
func (w *Worker) Shutdown() error {
	if !atomic.CompareAndSwapInt32(&w.stopFlag, 0, 1) {
		return queue.ErrQueueShutdown
	}

	w.stopOnce.Do(func() {
		_ = w.pubsub.Close()
		closeRedisClient(w.rdb)
		close(w.stop)
	})
	return nil
}

// Queue send notification to queue
func (w *Worker) Queue(job core.TaskMessage) error {
	if atomic.LoadInt32(&w.stopFlag) == 1 {
		return queue.ErrQueueShutdown
	}

	ctx := context.Background()

	// Publish a message.
	err := w.rdb.Publish(ctx, w.opts.channelName, job.Bytes()).Err()
	if err != nil {
		return err
	}

	return nil
}

// Request a new task
func (w *Worker) Request() (core.TaskMessage, error) {
	timer := time.NewTimer(w.opts.requestTimeout)
	defer timer.Stop()
	select {
	case task, ok := <-w.channel:
		if !ok {
			return nil, queue.ErrQueueHasBeenClosed
		}
		var data job.Message
		err := json.Unmarshal([]byte(task.Payload), &data)
		if err != nil {
			return nil, err
		}
		return &data, nil
	case <-timer.C:
		return nil, queue.ErrNoTaskInQueue
	}
}
