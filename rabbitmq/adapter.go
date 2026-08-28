package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"time"

	queue "github.com/faustbrian/go-queue"
	"github.com/faustbrian/go-queue/core"
	"github.com/faustbrian/go-queue/job"
	"github.com/faustbrian/go-queue/management"
	rabbitmqqueue "github.com/faustbrian/go-rabbitmq-queues"
)

// NativeConfig supplies explicit native connection, resource bounds, queue
// type, and stable application message identity to the compatibility adapter.
type NativeConfig struct {
	Connection        rabbitmqqueue.ConnectionConfig
	Producer          rabbitmqqueue.ProducerConfig
	Consumer          rabbitmqqueue.ConsumerConfig
	MessageID         func(core.TaskMessage) (string, error)
	DeliveryMessageID func(rabbitmqqueue.Delivery, *job.Message) (string, error)
}

type nativeProducer interface {
	Publish(context.Context, rabbitmqqueue.Publication) (rabbitmqqueue.PublishResult, error)
	Close(context.Context) error
}

type nativeConsumer interface {
	Done() <-chan struct{}
	Err() error
	Close(context.Context) error
}

var openNativeProducer = func(ctx context.Context, connection rabbitmqqueue.ConnectionConfig, config rabbitmqqueue.ProducerConfig) (nativeProducer, error) {
	return rabbitmqqueue.OpenProducer(ctx, connection, config)
}

var openNativeConsumer = func(ctx context.Context, connection rabbitmqqueue.ConnectionConfig, config rabbitmqqueue.ConsumerConfig, handler rabbitmqqueue.DeliveryHandler) (nativeConsumer, error) {
	return rabbitmqqueue.OpenConsumer(ctx, connection, config, handler)
}

var awaitNativeSettlement = func(delivery rabbitmqqueue.Delivery, ctx context.Context) error {
	return delivery.AwaitSettlement(ctx)
}

type adapterWorker struct {
	config   NativeConfig
	options  options
	producer nativeProducer
	consumer nativeConsumer
	deliver  chan *adapterDelivery
	lifetime context.Context
	cancel   context.CancelFunc

	consumerOnce     sync.Once
	consumerErr      error
	consumerMu       sync.Mutex
	consumerOpen     bool
	consumerStop     bool
	consumerDone     chan struct{}
	consumerCloseErr error
	closeOnce        sync.Once
	closeErr         error
}

type adapterDelivery struct {
	delivery   rabbitmqqueue.Delivery
	message    *job.Message
	requested  chan adapterSettlementRequest
	settleOnce sync.Once
	settled    chan struct{}
	settleErr  error
}

type adapterSettlementRequest struct {
	success    bool
	handlerErr error
}

func newAdapterWorker(opts options) (*adapterWorker, error) {
	if !opts.nativeConfigured || opts.autoAck || opts.native.MessageID == nil {
		return nil, queue.ErrInvalidConfiguration
	}
	if _, err := nativeExchangeKind(opts.exchangeType); err != nil {
		return nil, err
	}
	if err := opts.validateDeadLetter(); err != nil {
		return nil, err
	}
	config := opts.native
	config.Producer.PublishTimeout = opts.publishTimeout
	config.Consumer.Queue.Name = opts.queue
	config.Consumer.Name = opts.tag
	if err := config.Connection.Validate(); err != nil {
		return nil, errors.Join(queue.ErrInvalidConfiguration, err)
	}
	if err := config.Producer.Validate(); err != nil {
		return nil, errors.Join(queue.ErrInvalidConfiguration, err)
	}
	if err := config.Consumer.Validate(); err != nil {
		return nil, errors.Join(queue.ErrInvalidConfiguration, err)
	}
	lifetime, cancel := context.WithCancel(context.Background())
	producer, err := openNativeProducer(lifetime, config.Connection, config.Producer)
	if err != nil {
		cancel()
		return nil, err
	}
	return &adapterWorker{
		config: config, options: opts, producer: producer,
		deliver:      make(chan *adapterDelivery, config.Consumer.Prefetch),
		lifetime:     lifetime,
		cancel:       cancel,
		consumerDone: make(chan struct{}),
	}, nil
}

func nativeExchangeKind(kind string) (rabbitmqqueue.ExchangeKind, error) {
	switch kind {
	case ExchangeDirect:
		return rabbitmqqueue.ExchangeDirect, nil
	case ExchangeTopic:
		return rabbitmqqueue.ExchangeTopic, nil
	default:
		return "", queue.ErrInvalidConfiguration
	}
}

func (adapter *adapterWorker) queue(opts options, task core.TaskMessage) error {
	messageID, err := adapter.config.MessageID(task)
	if err != nil || messageID == "" {
		return errors.Join(queue.ErrInvalidConfiguration, rabbitmqqueue.ErrMessageIDRequired)
	}
	kind, err := nativeExchangeKind(opts.exchangeType)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(adapter.lifetime, adapter.config.Producer.PublishTimeout)
	defer cancel()
	result, err := adapter.producer.Publish(ctx, rabbitmqqueue.Publication{
		Exchange: opts.exchangeName, ExchangeKind: kind, RoutingKey: opts.routingKey,
		Mandatory: true, DeliveryMode: rabbitmqqueue.DeliveryPersistent,
		Message: rabbitmqqueue.Message{Body: task.Bytes(), MessageID: messageID},
	})
	return adapterPublishError(result, err)
}

func (adapter *adapterWorker) ensureConsumer() error {
	adapter.consumerOnce.Do(func() {
		adapter.consumerMu.Lock()
		if adapter.consumerStop {
			adapter.consumerErr = queue.ErrQueueHasBeenClosed
			close(adapter.consumerDone)
			adapter.consumerMu.Unlock()
			return
		}
		adapter.consumerOpen = true
		adapter.consumerMu.Unlock()

		consumer, openErr := openNativeConsumer(
			adapter.lifetime, adapter.config.Connection, adapter.config.Consumer, adapter.handle,
		)
		if openErr != nil {
			consumer = nil
		}
		adapter.consumerMu.Lock()
		stopping := adapter.consumerStop
		if !stopping {
			adapter.consumer = consumer
			adapter.consumerErr = openErr
			adapter.consumerOpen = false
			close(adapter.consumerDone)
			adapter.consumerMu.Unlock()
			return
		}
		adapter.consumerMu.Unlock()

		var closeErr error
		if consumer != nil {
			ctx, cancel := context.WithTimeout(context.Background(), adapter.config.Consumer.HandlerTimeout)
			closeErr = consumer.Close(ctx)
			cancel()
		}
		adapter.consumerMu.Lock()
		adapter.consumerErr = errors.Join(queue.ErrQueueHasBeenClosed, openErr, closeErr)
		adapter.consumerCloseErr = closeErr
		adapter.consumerOpen = false
		close(adapter.consumerDone)
		adapter.consumerMu.Unlock()
	})
	adapter.consumerMu.Lock()
	defer adapter.consumerMu.Unlock()
	return adapter.consumerErr
}

func (adapter *adapterWorker) handle(ctx context.Context, delivery rabbitmqqueue.Delivery) (rabbitmqqueue.Settlement, error) {
	message, err := job.DecodeE(delivery.Body, job.DefaultMaxMessageBytes)
	if err != nil {
		failure := management.NewFailure(
			management.ClassificationMalformed,
			"malformed_delivery",
			err,
		)
		return adapter.resolveFailure(ctx, delivery, nil, failure), nil
	}
	admitted := &adapterDelivery{
		delivery: delivery, message: message,
		requested: make(chan adapterSettlementRequest, 1), settled: make(chan struct{}),
	}
	select {
	case adapter.deliver <- admitted:
	case <-ctx.Done():
		return adapter.config.Consumer.Failure, ctx.Err()
	case <-adapter.lifetime.Done():
		return adapter.config.Consumer.Failure, adapter.lifetime.Err()
	}
	select {
	case request := <-admitted.requested:
		if request.success {
			return rabbitmqqueue.Acknowledge(), nil
		}
		return adapter.resolveFailure(ctx, delivery, message, request.handlerErr), nil
	case <-ctx.Done():
		return adapter.config.Consumer.Failure, ctx.Err()
	case <-adapter.lifetime.Done():
		return adapter.config.Consumer.Failure, adapter.lifetime.Err()
	}
}

func (adapter *adapterWorker) request(timeout time.Duration) (core.TaskMessage, error) {
	if err := adapter.ensureConsumer(); err != nil {
		return nil, err
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case admitted := <-adapter.deliver:
		admitted.message.SetFailureAcknowledgement(
			func() error { return adapter.settle(admitted, adapterSettlementRequest{success: true}) },
			func(handlerErr error) error {
				return adapter.settle(admitted, adapterSettlementRequest{handlerErr: handlerErr})
			},
		)
		return admitted.message, nil
	case <-timer.C:
		return nil, queue.ErrNoTaskInQueue
	case <-adapter.consumer.Done():
		return nil, queue.ErrQueueHasBeenClosed
	case <-adapter.lifetime.Done():
		return nil, queue.ErrQueueHasBeenClosed
	}
}

func (adapter *adapterWorker) settle(delivery *adapterDelivery, request adapterSettlementRequest) error {
	delivery.settleOnce.Do(func() {
		delivery.requested <- request
		ctx, cancel := context.WithTimeout(adapter.lifetime, adapter.config.Consumer.HandlerTimeout)
		defer cancel()
		delivery.settleErr = awaitNativeSettlement(delivery.delivery, ctx)
		close(delivery.settled)
	})
	<-delivery.settled
	return delivery.settleErr
}

func (adapter *adapterWorker) resolveFailure(
	ctx context.Context,
	delivery rabbitmqqueue.Delivery,
	message *job.Message,
	handlerErr error,
) rabbitmqqueue.Settlement {
	if handlerErr == nil {
		return rabbitmqqueue.NegativeAcknowledge(true)
	}
	resolution := management.ResolveFailure(handlerErr)
	if resolution.Classification == management.ClassificationCanceled ||
		resolution.Classification == management.ClassificationInfrastructure {
		return rabbitmqqueue.NegativeAcknowledge(true)
	}
	attempt, valid := adapterDeliveryAttempt(delivery.Headers)
	if !valid {
		resolution.Classification = management.ClassificationMalformed
		resolution.Code = "malformed_delivery_attempt"
		attempt = 1
	}
	if resolution.Code == "" {
		resolution.Code = "handler_failed"
	}
	terminal := resolution.Classification == management.ClassificationPermanent ||
		resolution.Classification == management.ClassificationMalformed ||
		attempt >= int64(adapter.options.deadLetter.MaxDeliveryAttempts)
	exchange := adapter.options.exchangeName
	routingKey := adapter.options.routingKey
	kind, _ := nativeExchangeKind(adapter.options.exchangeType)
	nextAttempt := attempt + 1
	if terminal {
		exchange = adapter.options.deadLetter.Exchange
		routingKey = adapter.options.deadLetter.RoutingKey
		kind = rabbitmqqueue.ExchangeDirect
		nextAttempt = attempt
		if attempt >= int64(adapter.options.deadLetter.MaxDeliveryAttempts) &&
			resolution.Classification == management.ClassificationRetryable {
			resolution.Code = "attempts_exhausted"
		}
	}
	messageID := delivery.MessageID
	if messageID == "" && adapter.config.DeliveryMessageID != nil {
		var identityErr error
		messageID, identityErr = adapter.config.DeliveryMessageID(delivery, message)
		if identityErr != nil {
			return rabbitmqqueue.NegativeAcknowledge(true)
		}
	}
	if messageID == "" {
		return rabbitmqqueue.NegativeAcknowledge(true)
	}
	headers := adapterSettlementHeaders(delivery.Headers, nextAttempt)
	if terminal {
		headers = append(headers,
			rabbitmqqueue.StringHeader(classificationHeader, string(resolution.Classification)),
			rabbitmqqueue.StringHeader(failureCodeHeader, resolution.Code),
			rabbitmqqueue.Int64Header(envelopeVersionHeader, int64(management.CurrentEnvelopeVersion)),
			rabbitmqqueue.StringHeader(sourceQueueHeader, adapter.options.queue),
			rabbitmqqueue.StringHeader(sourceExchangeHeader, adapter.options.exchangeName),
			rabbitmqqueue.StringHeader(sourceRoutingKeyHeader, adapter.options.routingKey),
		)
	}
	var priority *uint16
	if delivery.Priority > 0 {
		value := uint16(delivery.Priority)
		priority = &value
	}
	publishContext, cancel := context.WithTimeout(ctx, adapter.config.Producer.PublishTimeout)
	defer cancel()
	result, err := adapter.producer.Publish(publishContext, rabbitmqqueue.Publication{
		Exchange: exchange, ExchangeKind: kind, RoutingKey: routingKey,
		Mandatory: true, DeliveryMode: rabbitmqqueue.DeliveryPersistent,
		Message: rabbitmqqueue.Message{
			Body: delivery.Body, MessageID: messageID, CorrelationID: delivery.CorrelationID,
			ReplyTo: delivery.ReplyTo, ContentType: delivery.ContentType,
			ContentEncoding: delivery.ContentEncoding, Type: delivery.Type, AppID: delivery.AppID,
			Timestamp: delivery.Timestamp, Expiration: delivery.Expiration, Priority: priority, Headers: headers,
		},
	})
	if adapterPublishError(result, err) != nil {
		return rabbitmqqueue.NegativeAcknowledge(true)
	}
	return rabbitmqqueue.Acknowledge()
}

func adapterPublishError(result rabbitmqqueue.PublishResult, err error) error {
	if err != nil {
		return err
	}
	if !result.Valid() {
		return rabbitmqqueue.ErrProducerUnavailable
	}
	switch result.State {
	case rabbitmqqueue.PublishConfirmed:
		return nil
	case rabbitmqqueue.PublishReturned:
		return rabbitmqqueue.ErrPublishReturned
	case rabbitmqqueue.PublishRejected:
		return rabbitmqqueue.ErrPublishRejected
	case rabbitmqqueue.PublishAmbiguous:
		return rabbitmqqueue.ErrPublishAmbiguous
	default:
		return rabbitmqqueue.ErrProducerUnavailable
	}
}

func adapterDeliveryAttempt(headers []rabbitmqqueue.Header) (int64, bool) {
	for _, header := range headers {
		if header.Key != deliveryAttemptHeader {
			continue
		}
		return header.Int64, header.Kind == rabbitmqqueue.HeaderInt64 &&
			header.Int64 >= 1 && header.Int64 <= job.MaxRetryCount+1
	}
	return 1, true
}

func adapterSettlementHeaders(headers []rabbitmqqueue.Header, attempt int64) []rabbitmqqueue.Header {
	result := make([]rabbitmqqueue.Header, 0, len(headers)+1)
	for _, header := range headers {
		switch header.Key {
		case deliveryAttemptHeader, classificationHeader, failureCodeHeader, envelopeVersionHeader,
			sourceQueueHeader, sourceExchangeHeader, sourceRoutingKeyHeader:
			continue
		default:
			result = append(result, header)
		}
	}
	return append(result, rabbitmqqueue.Int64Header(deliveryAttemptHeader, attempt))
}

func (adapter *adapterWorker) close() error {
	adapter.closeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), adapter.config.Consumer.HandlerTimeout)
		defer cancel()
		adapter.consumerMu.Lock()
		adapter.consumerStop = true
		opening := adapter.consumerOpen
		consumerDone := adapter.consumerDone
		consumer := adapter.consumer
		adapter.consumerMu.Unlock()
		if opening {
			select {
			case <-consumerDone:
			case <-ctx.Done():
				adapter.closeErr = ctx.Err()
			}
			adapter.consumerMu.Lock()
			consumer = adapter.consumer
			adapter.closeErr = errors.Join(adapter.closeErr, adapter.consumerCloseErr)
			adapter.consumerMu.Unlock()
		}
		if consumer != nil {
			adapter.closeErr = errors.Join(adapter.closeErr, consumer.Close(ctx))
		}
		adapter.closeErr = errors.Join(adapter.closeErr, adapter.producer.Close(ctx))
		adapter.cancel()
	})
	return adapter.closeErr
}
