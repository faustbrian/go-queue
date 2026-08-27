package rabbitmq

import (
	"context"
	"sync/atomic"
	"time"

	queue "github.com/faustbrian/go-queue"
	"github.com/faustbrian/go-queue/core"
)

var _ core.Worker = (*Worker)(nil)
var _ core.WorkerMetadata = (*Worker)(nil)

const (
	deliveryAttemptHeader  = "x-queue-delivery-attempt"
	classificationHeader   = "x-queue-classification"
	failureCodeHeader      = "x-queue-failure-code"
	envelopeVersionHeader  = "x-queue-envelope-version"
	sourceQueueHeader      = "x-queue-source-queue"
	sourceExchangeHeader   = "x-queue-source-exchange"
	sourceRoutingKeyHeader = "x-queue-source-routing-key"
)

// ReconnectConfig is retained for source compatibility. NativeConfig.Connection.Recovery
// owns runtime recovery for compatibility-adapter workers.
type ReconnectConfig struct {
	MaxRetries   int
	InitialDelay time.Duration
	MaxDelay     time.Duration
}

// Worker preserves the go-queue worker contract while delegating RabbitMQ
// policy and resource ownership to go-rabbitmq-queues.
type Worker struct {
	adapter  *adapterWorker
	stopFlag int32
	opts     options
}

// BackendName identifies RabbitMQ in lifecycle events.
func (*Worker) BackendName() string { return "rabbitmq" }

// QueueName returns the configured RabbitMQ queue.
func (worker *Worker) QueueName() string { return worker.opts.queue }

// NewWorker creates a compatibility worker and panics when its explicit native
// policy is invalid or its producer cannot be opened.
func NewWorker(opts ...Option) *Worker {
	worker, err := NewWorkerE(opts...)
	if err != nil {
		panic(err)
	}
	return worker
}

// NewWorkerE creates a compatibility worker with an eagerly opened producer.
// The consumer remains unopened until Request is called.
func NewWorkerE(opts ...Option) (*Worker, error) {
	worker := &Worker{opts: newOptions(opts...)}
	if !worker.opts.nativeConfigured {
		return nil, queue.ErrInvalidConfiguration
	}
	adapter, err := newAdapterWorker(worker.opts)
	if err != nil {
		return nil, err
	}
	worker.adapter = adapter
	return worker, nil
}

func (worker *Worker) startConsumer() error {
	if atomic.LoadInt32(&worker.stopFlag) == 1 {
		return queue.ErrQueueShutdown
	}
	return worker.adapter.ensureConsumer()
}

// Run executes the configured go-queue handler.
func (worker *Worker) Run(ctx context.Context, task core.TaskMessage) error {
	return worker.opts.runFunc(ctx, task)
}

// Shutdown closes consumer and producer resources once. Repeated calls return
// queue.ErrQueueShutdown.
func (worker *Worker) Shutdown() error {
	if !atomic.CompareAndSwapInt32(&worker.stopFlag, 0, 1) {
		return queue.ErrQueueShutdown
	}
	return worker.adapter.close()
}

// Queue publishes one mandatory persistent task and waits for a definitive
// broker confirmation.
func (worker *Worker) Queue(task core.TaskMessage) error {
	if atomic.LoadInt32(&worker.stopFlag) == 1 {
		return queue.ErrQueueShutdown
	}
	return worker.adapter.queue(worker.opts, task)
}

// Request returns one decoded task from the bounded native delivery bridge.
func (worker *Worker) Request() (core.TaskMessage, error) {
	if atomic.LoadInt32(&worker.stopFlag) == 1 {
		return nil, queue.ErrQueueHasBeenClosed
	}
	return worker.adapter.request(worker.opts.requestTimeout)
}
