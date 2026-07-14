package redisdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/faustbrian/go-queue"
	"github.com/faustbrian/go-queue/core"
	"github.com/faustbrian/go-queue/job"

	"github.com/appleboy/com/bytesconv"
	"github.com/redis/go-redis/v9"
)

var _ core.Worker = (*Worker)(nil)
var _ core.WorkerMetadata = (*Worker)(nil)

// BackendName identifies Redis Streams in lifecycle events.
func (*Worker) BackendName() string { return "redis-streams" }

// QueueName returns the configured Redis stream.
func (w *Worker) QueueName() string { return w.opts.streamName }

// Stats describes outstanding work for this worker's Redis consumer group.
// Depth is Pending plus Lag and is -1 when Redis cannot determine group lag.
type Stats struct {
	Depth        int64
	Pending      int64
	Lag          int64
	LagKnown     bool
	OldestJobAge time.Duration
}

// Worker for Redis
type Worker struct {
	// redis config
	rdb         redis.Cmdable
	readGroup   func(context.Context, *redis.XReadGroupArgs) ([]redis.XStream, error)
	readGroups  func(context.Context, string) ([]redis.XInfoGroup, error)
	readPending func(context.Context, *redis.XPendingExtArgs) ([]redis.XPendingExt, error)
	readRange   func(context.Context, string, string, string, int64) ([]redis.XMessage, error)
	readContext context.Context
	cancelRead  context.CancelFunc
	tasks       chan redis.XMessage
	ack         func(string) error
	stopFlag    int32
	stopOnce    sync.Once
	startOnce   sync.Once
	stop        chan struct{}
	exit        chan struct{}
	opts        options
}

// NewWorker for struc
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
	readContext, cancelRead := context.WithCancel(context.Background())
	w := &Worker{
		opts:        newOptions(opts...),
		readContext: readContext,
		cancelRead:  cancelRead,
		stop:        make(chan struct{}),
		exit:        make(chan struct{}),
		tasks:       make(chan redis.XMessage),
	}

	if w.opts.connectionString != "" {
		options, err := redis.ParseURL(w.opts.connectionString)
		if err != nil {
			return nil, fmt.Errorf("parse Redis connection string: %w", err)
		}
		configureRedisOptions(options, w.opts.connectTimeout)
		w.rdb = redis.NewClient(options)
	} else if w.opts.addr != "" {
		if w.opts.cluster {
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
		} else {
			options := &redis.Options{
				Addr:      w.opts.addr,
				Username:  w.opts.username,
				Password:  w.opts.password,
				DB:        w.opts.db,
				TLSConfig: w.opts.tls,
			}
			configureRedisOptions(options, w.opts.connectTimeout)
			w.rdb = redis.NewClient(options)
		}
	}
	if w.rdb == nil {
		return nil, errors.New("redis address or connection string is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), w.opts.connectTimeout)
	defer cancel()
	_, err = w.rdb.Ping(ctx).Result()
	if err != nil {
		closeRedisClient(w.rdb)
		return nil, fmt.Errorf("connect to Redis: %w", err)
	}
	w.ack = func(id string) error {
		return w.rdb.XAck(context.Background(), w.opts.streamName, w.opts.group, id).Err()
	}
	w.readGroup = func(ctx context.Context, args *redis.XReadGroupArgs) ([]redis.XStream, error) {
		return w.rdb.XReadGroup(ctx, args).Result()
	}
	w.readGroups = func(ctx context.Context, stream string) ([]redis.XInfoGroup, error) {
		return w.rdb.XInfoGroups(ctx, stream).Result()
	}
	w.readPending = func(ctx context.Context, args *redis.XPendingExtArgs) ([]redis.XPendingExt, error) {
		return w.rdb.XPendingExt(ctx, args).Result()
	}
	w.readRange = func(
		ctx context.Context, stream string, start string, stop string, count int64,
	) ([]redis.XMessage, error) {
		return w.rdb.XRangeN(ctx, stream, start, stop, count).Result()
	}

	return w, nil
}

func configureRedisOptions(options *redis.Options, timeout time.Duration) {
	options.DialTimeout = timeout
	options.DialerRetries = -1
	options.MaxRetries = -1
	options.ContextTimeoutEnabled = true
}

func closeRedisClient(client redis.Cmdable) {
	switch value := client.(type) {
	case *redis.Client:
		_ = value.Close()
	case *redis.ClusterClient:
		_ = value.Close()
	}
}

func (w *Worker) startConsumer() {
	w.startOnce.Do(func() {
		if err := w.rdb.XGroupCreateMkStream(
			context.Background(),
			w.opts.streamName,
			w.opts.group,
			"$",
		).Err(); err != nil {
			w.opts.logger.Error(err)
		}

		go w.fetchTask()
	})
}

func (w *Worker) fetchTask() {
	for {
		select {
		case <-w.stop:
			return
		default:
		}

		ctx := w.readContext
		if ctx == nil {
			ctx = context.Background()
		}
		blockTime := w.opts.blockTime
		if blockTime <= 0 || blockTime > time.Second {
			blockTime = time.Second
		}
		data, err := w.readGroup(ctx, &redis.XReadGroupArgs{
			Group:    w.opts.group,
			Consumer: w.opts.consumer,
			Streams:  []string{w.opts.streamName, ">"},
			// count is number of entries we want to read from redis
			Count: 1,
			// we use the block command to make sure if no entry is found we wait
			// until an entry is found
			Block: blockTime,
		})
		if err != nil {
			if errors.Is(err, redis.Nil) {
				w.opts.logger.Infof("no messages available in Redis stream [%s]", w.opts.streamName)
				continue
			}
			w.opts.logger.Errorf("error while reading from redis %v", err)
			continue
		}
		// we have received the data we should loop it and queue the messages
		// so that our tasks can start processing
		for _, result := range data {
			for _, message := range result.Messages {
				select {
				case w.tasks <- message:
				case <-w.stop:
					// Todo: re-queue the task
					w.opts.logger.Info("re-queue the task: ", message.ID)
					if err := w.queue(message.Values); err != nil {
						w.opts.logger.Error("error to re-queue the task: ", message.ID)
					}
					close(w.exit)
					return
				}
			}
		}
	}
}

// Shutdown worker
func (w *Worker) Shutdown() error {
	if !atomic.CompareAndSwapInt32(&w.stopFlag, 0, 1) {
		return queue.ErrQueueShutdown
	}

	w.stopOnce.Do(func() {
		close(w.stop)
		if w.cancelRead != nil {
			w.cancelRead()
		}

		// wait requeue
		select {
		case <-w.exit:
		case <-time.After(200 * time.Millisecond):
		}

		closeRedisClient(w.rdb)
		close(w.tasks)
	})
	return nil
}

func (w *Worker) queue(data interface{}) error {
	ctx := context.Background()

	// Publish a message.
	err := w.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: w.opts.streamName,
		MaxLen: w.opts.maxLength,
		Values: data,
	}).Err()

	return err
}

// Queue send notification to queue
func (w *Worker) Queue(task core.TaskMessage) error {
	if atomic.LoadInt32(&w.stopFlag) == 1 {
		return queue.ErrQueueShutdown
	}

	return w.queue(map[string]interface{}{"body": bytesconv.BytesToStr(task.Bytes())})
}

// Run start the worker
func (w *Worker) Run(ctx context.Context, task core.TaskMessage) error {
	return w.opts.runFunc(ctx, task)
}

// Request a new task
func (w *Worker) Request() (core.TaskMessage, error) {
	w.startConsumer()
	timer := time.NewTimer(w.opts.requestTimeout)
	defer timer.Stop()
	select {
	case task, ok := <-w.tasks:
		if !ok {
			return nil, queue.ErrQueueHasBeenClosed
		}
		body, ok := task.Values["body"].(string)
		if !ok {
			return nil, errors.New("redis stream message body must be a string")
		}
		var data job.Message
		if err := json.Unmarshal(bytesconv.StrToBytes(body), &data); err != nil {
			return nil, fmt.Errorf("decode Redis stream message: %w", err)
		}
		data.SetAcknowledgement(
			func() error { return w.ack(task.ID) },
			func() error { return nil },
		)
		return &data, nil
	case <-timer.C:
		return nil, queue.ErrNoTaskInQueue
	}
}

// Stats returns consumer-group depth and the age of its oldest outstanding job.
func (w *Worker) Stats(ctx context.Context) (Stats, error) {
	groups, err := w.readGroups(ctx, w.opts.streamName)
	if err != nil {
		return Stats{}, fmt.Errorf("read Redis stream groups: %w", err)
	}
	var group *redis.XInfoGroup
	for index := range groups {
		if groups[index].Name == w.opts.group {
			group = &groups[index]
			break
		}
	}
	if group == nil {
		return Stats{}, fmt.Errorf("redis stream group %q does not exist", w.opts.group)
	}

	stats := Stats{Pending: group.Pending, Lag: group.Lag, LagKnown: group.Lag >= 0}
	if stats.LagKnown {
		stats.Depth = group.Pending + group.Lag
	} else {
		stats.Depth = -1
	}
	if group.Pending == 0 && group.Lag == 0 {
		return stats, nil
	}

	var oldestIDs []string
	if group.Pending > 0 {
		pending, pendingErr := w.readPending(ctx, &redis.XPendingExtArgs{
			Stream: w.opts.streamName,
			Group:  w.opts.group,
			Start:  "-",
			End:    "+",
			Count:  1,
		})
		if pendingErr != nil {
			return Stats{}, fmt.Errorf("read Redis pending jobs: %w", pendingErr)
		}
		if len(pending) > 0 {
			oldestIDs = append(oldestIDs, pending[0].ID)
		}
	}
	if group.Lag > 0 {
		start := "(" + group.LastDeliveredID
		messages, rangeErr := w.readRange(ctx, w.opts.streamName, start, "+", 1)
		if rangeErr != nil {
			return Stats{}, fmt.Errorf("read Redis queued jobs: %w", rangeErr)
		}
		if len(messages) > 0 {
			oldestIDs = append(oldestIDs, messages[0].ID)
		}
	}

	now := time.Now()
	for _, id := range oldestIDs {
		age, ageErr := streamMessageAge(id, now)
		if ageErr != nil {
			return Stats{}, ageErr
		}
		if age > stats.OldestJobAge {
			stats.OldestJobAge = age
		}
	}
	return stats, nil
}

func streamMessageAge(id string, now time.Time) (time.Duration, error) {
	milliseconds, _, ok := strings.Cut(id, "-")
	if !ok {
		return 0, fmt.Errorf("invalid Redis stream message ID %q", id)
	}
	timestamp, err := strconv.ParseInt(milliseconds, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid Redis stream message ID %q: %w", id, err)
	}
	age := now.Sub(time.UnixMilli(timestamp))
	if age < 0 {
		return 0, nil
	}
	return age, nil
}
