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
	// Depth is Pending plus Lag, or -1 when Lag is unknown.
	Depth int64
	// Pending counts delivered entries still pending for this consumer group.
	Pending int64
	// Lag counts entries not yet delivered to the group when known.
	Lag int64
	// LagKnown distinguishes determinate depth from unsupported group lag.
	LagKnown bool
	// OldestJobAge is the oldest inspected pending/undelivered age; zero may be unavailable.
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
	// ErrManagementControlDisabled retains the successor control-disabled sentinel.
	//
	// Deprecated: use redisstream.ErrManagementControlDisabled.
	ErrManagementControlDisabled = redisstream.ErrManagementControlDisabled
	// ErrManagementStatusDisabled retains the successor status-disabled sentinel.
	//
	// Deprecated: use redisstream.ErrManagementStatusDisabled.
	ErrManagementStatusDisabled = redisstream.ErrManagementStatusDisabled
	// ErrInvalidManagementStatus retains the successor invalid-status sentinel.
	//
	// Deprecated: use redisstream.ErrInvalidManagementStatus.
	ErrInvalidManagementStatus = redisstream.ErrInvalidManagementStatus
)

// NewWorker constructs the successor worker and panics on error.
//
// Deprecated: use redisstream.NewWorker.
func NewWorker(options ...Option) *Worker {
	worker, err := NewWorkerE(options...)
	if err != nil {
		panic(err)
	}
	return worker
}

// NewWorkerE constructs the successor worker and returns its construction error.
//
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

// WithAddr forwards the Redis standalone or comma-separated cluster addresses.
//
// Deprecated: use redisstream.WithAddr.
func WithAddr(value string) Option { return successorOption(redisstream.WithAddr(value)) }

// WithBlockTime forwards the preferred blocking read duration.
//
// Deprecated: use redisstream.WithBlockTime.
func WithBlockTime(value time.Duration) Option {
	return successorOption(redisstream.WithBlockTime(value))
}

// WithCluster selects the successor's cluster client.
//
// Deprecated: use redisstream.WithCluster.
func WithCluster() Option { return successorOption(redisstream.WithCluster()) }

// WithCommandTimeout forwards the record/settlement command bound.
//
// Deprecated: use redisstream.WithCommandTimeout.
func WithCommandTimeout(value time.Duration) Option {
	return successorOption(redisstream.WithCommandTimeout(value))
}

// WithConnectTimeout forwards initial connection validation timeout.
//
// Deprecated: use redisstream.WithConnectTimeout.
func WithConnectTimeout(value time.Duration) Option {
	return successorOption(redisstream.WithConnectTimeout(value))
}

// WithConnectionString forwards the Redis URL; URL options take precedence in the successor.
//
// Deprecated: use redisstream.WithConnectionString.
func WithConnectionString(value string) Option {
	return successorOption(redisstream.WithConnectionString(value))
}

// WithConsumer forwards the reclaim/read owner identity.
//
// Deprecated: use redisstream.WithConsumer.
func WithConsumer(value string) Option { return successorOption(redisstream.WithConsumer(value)) }

// WithDB forwards standalone database selection.
//
// Deprecated: use redisstream.WithDB.
func WithDB(value int) Option { return successorOption(redisstream.WithDB(value)) }

// WithDeadLetter forwards terminal destination and delivery-attempt policy.
//
// Deprecated: use redisstream.WithDeadLetter.
func WithDeadLetter(stream string, attempts int64) Option {
	return successorOption(redisstream.WithDeadLetter(stream, attempts))
}

// WithFailureStream forwards failed-attempt record storage.
//
// Deprecated: use redisstream.WithFailureStream.
func WithFailureStream(value string) Option {
	return successorOption(redisstream.WithFailureStream(value))
}

// WithGroup forwards consumer-group identity.
//
// Deprecated: use redisstream.WithGroup.
func WithGroup(value string) Option { return successorOption(redisstream.WithGroup(value)) }

// WithLogger forwards the borrowed logging implementation.
//
// Deprecated: use redisstream.WithLogger.
func WithLogger(value queue.Logger) Option { return successorOption(redisstream.WithLogger(value)) }

// WithManagementStatus forwards explicit status identity and protocol metadata.
//
// Deprecated: use redisstream.WithManagementStatus.
func WithManagementStatus(value management.StatusMetadata) Option {
	return successorOption(redisstream.WithManagementStatus(value))
}

// WithMaxLength forwards direct-enqueue source capacity; zero is unbounded.
//
// Deprecated: use redisstream.WithMaxLength.
func WithMaxLength(value int64) Option { return successorOption(redisstream.WithMaxLength(value)) }

// WithPassword forwards caller-owned credentials.
//
// Deprecated: use redisstream.WithPassword.
func WithPassword(value string) Option { return successorOption(redisstream.WithPassword(value)) }

// WithReclaim forwards stale pending-entry recovery bounds.
//
// Deprecated: use redisstream.WithReclaim.
func WithReclaim(idle, interval time.Duration, batch int64) Option {
	return successorOption(redisstream.WithReclaim(idle, interval, batch))
}

// WithRecordRetention forwards optional exact maximum-count record retention.
//
// Deprecated: use redisstream.WithRecordRetention.
func WithRecordRetention(value int64) Option {
	return successorOption(redisstream.WithRecordRetention(value))
}

// WithReplayDestinations forwards the explicit replay destination allowlist.
//
// Deprecated: use redisstream.WithReplayDestinations.
func WithReplayDestinations(values ...string) Option {
	return successorOption(redisstream.WithReplayDestinations(values...))
}

// WithRequestTimeout forwards the request wait duration.
//
// Deprecated: use redisstream.WithRequestTimeout.
func WithRequestTimeout(value time.Duration) Option {
	return successorOption(redisstream.WithRequestTimeout(value))
}

// WithRunFunc forwards synchronous handler code that must honor its context.
//
// Deprecated: use redisstream.WithRunFunc.
func WithRunFunc(value func(context.Context, core.TaskMessage) error) Option {
	return successorOption(redisstream.WithRunFunc(value))
}

// WithSkipTLSVerify forwards explicit certificate verification opt-out.
//
// Deprecated: use redisstream.WithSkipTLSVerify.
func WithSkipTLSVerify() Option { return successorOption(redisstream.WithSkipTLSVerify()) }

// WithStreamName forwards source stream identity.
//
// Deprecated: use redisstream.WithStreamName.
func WithStreamName(value string) Option { return successorOption(redisstream.WithStreamName(value)) }

// WithTLS forwards TLS configuration with a TLS1.2 minimum.
//
// Deprecated: use redisstream.WithTLS.
func WithTLS() Option { return successorOption(redisstream.WithTLS()) }

// WithUsername forwards caller-owned ACL identity.
//
// Deprecated: use redisstream.WithUsername.
func WithUsername(value string) Option { return successorOption(redisstream.WithUsername(value)) }

// BackendName returns the successor backend label.
func (worker *Worker) BackendName() string { return worker.successor.BackendName() }

// QueueName returns the successor source stream name.
func (worker *Worker) QueueName() string { return worker.successor.QueueName() }

// Shutdown releases successor resources with its native lifecycle policy.
func (worker *Worker) Shutdown() error { return worker.successor.Shutdown() }

// Queue forwards one task admission, not a handler-completion guarantee.
func (worker *Worker) Queue(task core.TaskMessage) error { return worker.successor.Queue(task) }

// Run forwards one synchronous handler call using ctx.
func (worker *Worker) Run(ctx context.Context, task core.TaskMessage) error {
	return worker.successor.Run(ctx, task)
}

// Request forwards one bounded intake attempt.
func (worker *Worker) Request() (core.TaskMessage, error) { return worker.successor.Request() }

// Stats returns successor consumer-group observations, with unknown lag preserved.
func (worker *Worker) Stats(ctx context.Context) (Stats, error) {
	stats, err := worker.successor.Stats(ctx)
	return Stats(stats), err
}

// Execute applies a command under provider-owned deduplication/outcome rules.
// Authorization remains caller-owned.
func (worker *Worker) Execute(ctx context.Context, command management.Command) (management.CommandResult, error) {
	return worker.successor.Execute(ctx, command)
}

// ListFailures lists bounded metadata with payload and diagnostics hidden.
func (worker *Worker) ListFailures(ctx context.Context, request management.PageRequest) (management.RecordPage, error) {
	return worker.successor.ListFailures(ctx, request)
}

// ListDeadLetters lists bounded terminal metadata with payload and diagnostics hidden.
func (worker *Worker) ListDeadLetters(ctx context.Context, request management.PageRequest) (management.RecordPage, error) {
	return worker.successor.ListDeadLetters(ctx, request)
}

// Inspect returns one record with no more disclosure than requested; authorization is caller-owned.
func (worker *Worker) Inspect(ctx context.Context, request management.InspectRequest) (management.JobRecord, error) {
	return worker.successor.Inspect(ctx, request)
}

// ObserveWorker reports native worker state; the provider owns context handling and concurrency.
func (worker *Worker) ObserveWorker(ctx context.Context) (management.WorkerStatus, error) {
	return worker.successor.ObserveWorker(ctx)
}

// ObserveQueue reports native queue support flags and values, not invented metrics.
func (worker *Worker) ObserveQueue(ctx context.Context) (management.QueueStatus, error) {
	return worker.successor.ObserveQueue(ctx)
}
