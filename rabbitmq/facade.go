// Package rabbitmq preserves the released RabbitMQ adapter import path.
//
// Deprecated: use github.com/faustbrian/go-queue/adapters/rabbitmq.
package rabbitmq

import (
	"context"
	"sync"
	"time"

	queue "github.com/faustbrian/go-queue"
	successor "github.com/faustbrian/go-queue/adapters/rabbitmq"
	"github.com/faustbrian/go-queue/core"
	"github.com/faustbrian/go-queue/job"
	rabbitmqqueue "github.com/faustbrian/go-rabbitmq-queues"
)

const (
	// ExchangeDirect retains the legacy direct exchange name.
	//
	// Deprecated: use successor.ExchangeDirect.
	ExchangeDirect = successor.ExchangeDirect
	// ExchangeFanout retains the legacy constant; the native adapter rejects it.
	//
	// Deprecated: use successor.ExchangeFanout.
	ExchangeFanout = successor.ExchangeFanout
	// ExchangeTopic retains the legacy topic exchange name.
	//
	// Deprecated: use successor.ExchangeTopic.
	ExchangeTopic = successor.ExchangeTopic
	// ExchangeHeaders retains the legacy constant; the native adapter rejects it.
	//
	// Deprecated: use successor.ExchangeHeaders.
	ExchangeHeaders = successor.ExchangeHeaders
)

// DeadLetterConfig preserves the released terminal-route configuration.
//
// Deprecated: use successor.DeadLetterConfig.
type DeadLetterConfig struct {
	// Exchange is the terminal exchange.
	Exchange string
	// Queue is the terminal queue.
	Queue string
	// RoutingKey is the terminal routing key.
	RoutingKey string
	// MaxDeliveryAttempts bounds delivery attempts before terminal routing.
	MaxDeliveryAttempts uint32
}

// NativeConfig preserves the released explicit RabbitMQ policy boundary.
//
// Deprecated: use successor.NativeConfig.
type NativeConfig struct {
	// Connection configures the caller-approved native connection.
	Connection rabbitmqqueue.ConnectionConfig
	// Producer configures confirmed mandatory publication.
	Producer rabbitmqqueue.ProducerConfig
	// Consumer configures bounded manual-settlement consumption.
	Consumer rabbitmqqueue.ConsumerConfig
	// MessageID derives stable identity for an outgoing task.
	MessageID func(core.TaskMessage) (string, error)
	// DeliveryMessageID derives identity for a decoded broker delivery.
	DeliveryMessageID func(rabbitmqqueue.Delivery, *job.Message) (string, error)
}

// ReconnectConfig preserves the released source-compatible recovery options.
// NativeConfig.Connection.Recovery owns runtime recovery.
//
// Deprecated: use successor.ReconnectConfig.
type ReconnectConfig struct {
	// MaxRetries is retained for source compatibility.
	MaxRetries int
	// InitialDelay is retained for source compatibility.
	InitialDelay time.Duration
	// MaxDelay is retained for source compatibility.
	MaxDelay time.Duration
}

// Option preserves the released worker configuration surface.
//
// Deprecated: use successor.Option.
type Option func(*options)
type options struct{ successor []successor.Option }

type workerDelegate interface {
	BackendName() string
	QueueName() string
	Run(context.Context, core.TaskMessage) error
	Shutdown() error
	Queue(core.TaskMessage) error
	Request() (core.TaskMessage, error)
}

type workerFactory func(...successor.Option) (workerDelegate, error)

func successorOption(option successor.Option) Option {
	return func(options *options) { options.successor = append(options.successor, option) }
}

// Worker preserves the released queue worker API while delegating all runtime
// ownership and behavior to the target-oriented successor.
//
// Deprecated: use successor.Worker.
type Worker struct {
	successor workerDelegate
	zero      sync.Once
}

func (worker *Worker) delegate() workerDelegate {
	worker.zero.Do(func() {
		if worker.successor == nil {
			worker.successor = new(successor.Worker)
		}
	})
	return worker.successor
}

// NewWorker constructs the successor worker and panics on error.
//
// Deprecated: use successor.NewWorker.
func NewWorker(options ...Option) *Worker {
	return mustWorker(NewWorkerE(options...))
}

func mustWorker(worker *Worker, err error) *Worker {
	if err != nil {
		panic(err)
	}
	return worker
}

// NewWorkerE constructs the successor worker and returns its construction error.
//
// Deprecated: use successor.NewWorkerE.
func NewWorkerE(optionValues ...Option) (*Worker, error) {
	return newWorkerE(func(options ...successor.Option) (workerDelegate, error) {
		return successor.NewWorkerE(options...)
	}, optionValues...)
}

func newWorkerE(factory workerFactory, optionValues ...Option) (*Worker, error) {
	configured := options{}
	for _, option := range optionValues {
		option(&configured)
	}
	worker, err := factory(configured.successor...)
	if err != nil {
		return nil, err
	}
	return &Worker{successor: worker}, nil
}

// WithAddr retains the legacy address option; native RabbitMQ connections use NativeConfig.
//
// Deprecated: use successor.WithAddr.
func WithAddr(value string) Option { return successorOption(successor.WithAddr(value)) }

// WithAutoAck forwards acknowledgement policy; the native adapter rejects automatic acknowledgement.
//
// Deprecated: use successor.WithAutoAck.
func WithAutoAck(value bool) Option { return successorOption(successor.WithAutoAck(value)) }

// WithDeadLetter forwards terminal destination and delivery-attempt policy.
//
// Deprecated: use successor.WithDeadLetter.
func WithDeadLetter(value DeadLetterConfig) Option {
	return successorOption(successor.WithDeadLetter(successor.DeadLetterConfig(value)))
}

// WithExchangeName forwards the infrastructure-owned exchange name.
//
// Deprecated: use successor.WithExchangeName.
func WithExchangeName(value string) Option { return successorOption(successor.WithExchangeName(value)) }

// WithExchangeType forwards exchange kind; the native adapter accepts direct or topic.
//
// Deprecated: use successor.WithExchangeType.
func WithExchangeType(value string) Option { return successorOption(successor.WithExchangeType(value)) }

// WithLogger forwards the borrowed logging implementation.
//
// Deprecated: use successor.WithLogger.
func WithLogger(value queue.Logger) Option { return successorOption(successor.WithLogger(value)) }

// WithNativeConfig forwards explicit native connection and resource policy.
//
// Deprecated: use successor.WithNativeConfig.
func WithNativeConfig(value NativeConfig) Option {
	return successorOption(successor.WithNativeConfig(successor.NativeConfig(value)))
}

// WithPublishTimeout forwards the per-publish time bound.
//
// Deprecated: use successor.WithPublishTimeout.
func WithPublishTimeout(value time.Duration) Option {
	return successorOption(successor.WithPublishTimeout(value))
}

// WithQueue forwards the logical queue name.
//
// Deprecated: use successor.WithQueue.
func WithQueue(value string) Option { return successorOption(successor.WithQueue(value)) }

// WithReconnectConfig retains legacy configuration; NativeConfig owns runtime recovery.
//
// Deprecated: use successor.WithReconnectConfig.
func WithReconnectConfig(value ReconnectConfig) Option {
	return successorOption(successor.WithReconnectConfig(successor.ReconnectConfig(value)))
}

// WithRequestTimeout forwards the request wait duration.
//
// Deprecated: use successor.WithRequestTimeout.
func WithRequestTimeout(value time.Duration) Option {
	return successorOption(successor.WithRequestTimeout(value))
}

// WithRoutingKey forwards the exchange route.
//
// Deprecated: use successor.WithRoutingKey.
func WithRoutingKey(value string) Option { return successorOption(successor.WithRoutingKey(value)) }

// WithRunFunc forwards synchronous handler code that must honor its context.
//
// Deprecated: use successor.WithRunFunc.
func WithRunFunc(value func(context.Context, core.TaskMessage) error) Option {
	return successorOption(successor.WithRunFunc(value))
}

// WithTag forwards the consumer name.
//
// Deprecated: use successor.WithTag.
func WithTag(value string) Option { return successorOption(successor.WithTag(value)) }

// BackendName identifies RabbitMQ in lifecycle events.
func (*Worker) BackendName() string { return "rabbitmq" }

// QueueName returns the configured RabbitMQ queue.
func (worker *Worker) QueueName() string { return worker.delegate().QueueName() }

// Run executes the configured go-queue handler.
func (worker *Worker) Run(ctx context.Context, task core.TaskMessage) error {
	return worker.delegate().Run(ctx, task)
}

// Shutdown closes consumer and producer resources once.
func (worker *Worker) Shutdown() error { return worker.delegate().Shutdown() }

// Queue publishes one mandatory persistent task and waits for broker confirmation.
func (worker *Worker) Queue(task core.TaskMessage) error { return worker.delegate().Queue(task) }

// Request returns one decoded task from the bounded delivery bridge.
func (worker *Worker) Request() (core.TaskMessage, error) { return worker.delegate().Request() }
