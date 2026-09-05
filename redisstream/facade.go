// Package redisdb preserves the released Redis Streams import path.
//
// Deprecated: use github.com/faustbrian/go-queue/adapters/redisstream. This
// compatibility facade delegates runtime behavior and state to that semantic
// owner.
package redisdb

import (
	"context"
	"time"

	queue "github.com/faustbrian/go-queue"
	"github.com/faustbrian/go-queue/adapters/redisstream"
	"github.com/faustbrian/go-queue/core"
	"github.com/faustbrian/go-queue/management"
)

// Option preserves the released configuration function type.
//
// Deprecated: use redisstream.Option.
type Option func(*options)

type options struct {
	successor []redisstream.Option
}

func successorOption(option redisstream.Option) Option {
	return func(options *options) {
		options.successor = append(options.successor, option)
	}
}

// Stats describes outstanding work for this worker's Redis consumer group.
//
// Deprecated: use redisstream.Stats.
type Stats struct {
	Depth        int64
	Pending      int64
	Lag          int64
	LagKnown     bool
	OldestJobAge time.Duration
}

// Worker preserves the released worker type while delegating all runtime
// state and behavior to the successor adapter.
//
// Deprecated: use redisstream.Worker.
type Worker struct {
	successor *redisstream.Worker
}

var (
	// Deprecated: use redisstream.ErrManagementControlDisabled.
	ErrManagementControlDisabled = redisstream.ErrManagementControlDisabled
	// Deprecated: use redisstream.ErrManagementStatusDisabled.
	ErrManagementStatusDisabled = redisstream.ErrManagementStatusDisabled
	// Deprecated: use redisstream.ErrInvalidManagementStatus.
	ErrInvalidManagementStatus = redisstream.ErrInvalidManagementStatus
)

// Deprecated: use redisstream.NewWorker.
func NewWorker(options ...Option) *Worker {
	worker, err := NewWorkerE(options...)
	if err != nil {
		panic(err)
	}
	return worker
}

// Deprecated: use redisstream.NewWorkerE.
func NewWorkerE(optionValues ...Option) (*Worker, error) {
	configured := options{}
	for _, option := range optionValues {
		option(&configured)
	}
	worker, err := redisstream.NewWorkerE(configured.successor...)
	if err != nil {
		return nil, err
	}
	return &Worker{successor: worker}, nil
}

// Deprecated: use redisstream.WithAddr.
func WithAddr(value string) Option { return successorOption(redisstream.WithAddr(value)) }

// Deprecated: use redisstream.WithBlockTime.
func WithBlockTime(value time.Duration) Option {
	return successorOption(redisstream.WithBlockTime(value))
}

// Deprecated: use redisstream.WithCluster.
func WithCluster() Option { return successorOption(redisstream.WithCluster()) }

// Deprecated: use redisstream.WithCommandTimeout.
func WithCommandTimeout(value time.Duration) Option {
	return successorOption(redisstream.WithCommandTimeout(value))
}

// Deprecated: use redisstream.WithConnectTimeout.
func WithConnectTimeout(value time.Duration) Option {
	return successorOption(redisstream.WithConnectTimeout(value))
}

// Deprecated: use redisstream.WithConnectionString.
func WithConnectionString(value string) Option {
	return successorOption(redisstream.WithConnectionString(value))
}

// Deprecated: use redisstream.WithConsumer.
func WithConsumer(value string) Option { return successorOption(redisstream.WithConsumer(value)) }

// Deprecated: use redisstream.WithDB.
func WithDB(value int) Option { return successorOption(redisstream.WithDB(value)) }

// Deprecated: use redisstream.WithDeadLetter.
func WithDeadLetter(stream string, attempts int64) Option {
	return successorOption(redisstream.WithDeadLetter(stream, attempts))
}

// Deprecated: use redisstream.WithFailureStream.
func WithFailureStream(value string) Option {
	return successorOption(redisstream.WithFailureStream(value))
}

// Deprecated: use redisstream.WithGroup.
func WithGroup(value string) Option { return successorOption(redisstream.WithGroup(value)) }

// Deprecated: use redisstream.WithLogger.
func WithLogger(value queue.Logger) Option { return successorOption(redisstream.WithLogger(value)) }

// Deprecated: use redisstream.WithManagementStatus.
func WithManagementStatus(value management.StatusMetadata) Option {
	return successorOption(redisstream.WithManagementStatus(value))
}

// Deprecated: use redisstream.WithMaxLength.
func WithMaxLength(value int64) Option { return successorOption(redisstream.WithMaxLength(value)) }

// Deprecated: use redisstream.WithPassword.
func WithPassword(value string) Option { return successorOption(redisstream.WithPassword(value)) }

// Deprecated: use redisstream.WithReclaim.
func WithReclaim(idle, interval time.Duration, batch int64) Option {
	return successorOption(redisstream.WithReclaim(idle, interval, batch))
}

// Deprecated: use redisstream.WithRecordRetention.
func WithRecordRetention(value int64) Option {
	return successorOption(redisstream.WithRecordRetention(value))
}

// Deprecated: use redisstream.WithReplayDestinations.
func WithReplayDestinations(values ...string) Option {
	return successorOption(redisstream.WithReplayDestinations(values...))
}

// Deprecated: use redisstream.WithRequestTimeout.
func WithRequestTimeout(value time.Duration) Option {
	return successorOption(redisstream.WithRequestTimeout(value))
}

// Deprecated: use redisstream.WithRunFunc.
func WithRunFunc(value func(context.Context, core.TaskMessage) error) Option {
	return successorOption(redisstream.WithRunFunc(value))
}

// Deprecated: use redisstream.WithSkipTLSVerify.
func WithSkipTLSVerify() Option { return successorOption(redisstream.WithSkipTLSVerify()) }

// Deprecated: use redisstream.WithStreamName.
func WithStreamName(value string) Option { return successorOption(redisstream.WithStreamName(value)) }

// Deprecated: use redisstream.WithTLS.
func WithTLS() Option { return successorOption(redisstream.WithTLS()) }

// Deprecated: use redisstream.WithUsername.
func WithUsername(value string) Option { return successorOption(redisstream.WithUsername(value)) }

func (worker *Worker) BackendName() string               { return worker.successor.BackendName() }
func (worker *Worker) QueueName() string                 { return worker.successor.QueueName() }
func (worker *Worker) Shutdown() error                   { return worker.successor.Shutdown() }
func (worker *Worker) Queue(task core.TaskMessage) error { return worker.successor.Queue(task) }
func (worker *Worker) Run(ctx context.Context, task core.TaskMessage) error {
	return worker.successor.Run(ctx, task)
}
func (worker *Worker) Request() (core.TaskMessage, error) { return worker.successor.Request() }
func (worker *Worker) Stats(ctx context.Context) (Stats, error) {
	stats, err := worker.successor.Stats(ctx)
	return Stats(stats), err
}
func (worker *Worker) Execute(ctx context.Context, command management.Command) (management.CommandResult, error) {
	return worker.successor.Execute(ctx, command)
}
func (worker *Worker) ListFailures(ctx context.Context, request management.PageRequest) (management.RecordPage, error) {
	return worker.successor.ListFailures(ctx, request)
}
func (worker *Worker) ListDeadLetters(ctx context.Context, request management.PageRequest) (management.RecordPage, error) {
	return worker.successor.ListDeadLetters(ctx, request)
}
func (worker *Worker) Inspect(ctx context.Context, request management.InspectRequest) (management.JobRecord, error) {
	return worker.successor.Inspect(ctx, request)
}
func (worker *Worker) ObserveWorker(ctx context.Context) (management.WorkerStatus, error) {
	return worker.successor.ObserveWorker(ctx)
}
func (worker *Worker) ObserveQueue(ctx context.Context) (management.QueueStatus, error) {
	return worker.successor.ObserveQueue(ctx)
}
