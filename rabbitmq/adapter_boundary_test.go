package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	queue "github.com/faustbrian/go-queue"
	"github.com/faustbrian/go-queue/core"
	"github.com/faustbrian/go-queue/job"
	rabbitmqqueue "github.com/faustbrian/go-rabbitmq-queues"
)

type recordingAdapterLogger struct {
	mu    sync.Mutex
	calls int
}

func (logger *recordingAdapterLogger) record(...any) {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	logger.calls++
}

func (logger *recordingAdapterLogger) Infof(string, ...any)  { logger.record() }
func (logger *recordingAdapterLogger) Errorf(string, ...any) { logger.record() }
func (logger *recordingAdapterLogger) Fatalf(string, ...any) { logger.record() }
func (logger *recordingAdapterLogger) Info(...any)           { logger.record() }
func (logger *recordingAdapterLogger) Error(...any)          { logger.record() }
func (logger *recordingAdapterLogger) Fatal(...any)          { logger.record() }

func (logger *recordingAdapterLogger) count() int {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	return logger.calls
}

func TestAdapterRejectsUnsupportedAndInvalidNativePolicies(t *testing.T) {
	tests := map[string]func(*NativeConfig, *[]Option){
		"automatic acknowledgement": func(_ *NativeConfig, options *[]Option) {
			*options = append(*options, WithAutoAck(true))
		},
		"fanout exchange": func(_ *NativeConfig, options *[]Option) {
			*options = append(*options, WithExchangeType(ExchangeFanout))
		},
		"headers exchange": func(_ *NativeConfig, options *[]Option) {
			*options = append(*options, WithExchangeType(ExchangeHeaders))
		},
		"message identity": func(config *NativeConfig, _ *[]Option) {
			config.MessageID = nil
		},
		"connection": func(config *NativeConfig, _ *[]Option) {
			config.Connection.Endpoints = nil
		},
		"producer": func(config *NativeConfig, _ *[]Option) {
			config.Producer.MaxOutstanding = 0
		},
		"consumer": func(config *NativeConfig, _ *[]Option) {
			config.Consumer.Prefetch = 0
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			config := testNativeConfig()
			options := []Option{
				WithQueue("jobs"), WithTag("worker"),
				WithExchangeName("events"), WithExchangeType(ExchangeDirect),
				WithRoutingKey("jobs.created"),
			}
			mutate(&config, &options)
			options = append(options, WithNativeConfig(config))
			worker, err := NewWorkerE(options...)
			if worker != nil || !errors.Is(err, queue.ErrInvalidConfiguration) {
				t.Fatalf("NewWorkerE() = (%v, %v), want invalid configuration", worker, err)
			}
		})
	}
}

func TestAdapterRejectsInvalidExchangeWithoutLoggingCallerIdentity(t *testing.T) {
	logger := &recordingAdapterLogger{}
	worker, err := NewWorkerE(
		WithNativeConfig(testNativeConfig()),
		WithLogger(logger),
		WithQueue("jobs"),
		WithTag("worker"),
		WithExchangeName("events"),
		WithExchangeType("caller-controlled-private-identity"),
		WithRoutingKey("jobs.created"),
	)
	if worker != nil || !errors.Is(err, queue.ErrInvalidConfiguration) {
		t.Fatalf("NewWorkerE() = (%v, %v), want invalid configuration", worker, err)
	}
	if got := logger.count(); got != 0 {
		t.Fatalf("logger calls = %d, want none", got)
	}
}

func TestAdapterPropagatesProducerOpenFailureAndAcceptsTopic(t *testing.T) {
	openErr := errors.New("producer unavailable")
	originalProducer := openNativeProducer
	openNativeProducer = func(
		context.Context,
		rabbitmqqueue.ConnectionConfig,
		rabbitmqqueue.ProducerConfig,
	) (nativeProducer, error) {
		return nil, openErr
	}
	t.Cleanup(func() { openNativeProducer = originalProducer })

	options := []Option{
		WithNativeConfig(testNativeConfig()), WithQueue("jobs"), WithTag("worker"),
		WithExchangeName("events"), WithExchangeType(ExchangeTopic),
		WithRoutingKey("jobs.*"),
	}
	worker, err := NewWorkerE(options...)
	if worker != nil || !errors.Is(err, openErr) {
		t.Fatalf("NewWorkerE() = (%v, %v), want producer open error", worker, err)
	}

	producer := &recordingNativeProducer{}
	openNativeProducer = func(
		context.Context,
		rabbitmqqueue.ConnectionConfig,
		rabbitmqqueue.ProducerConfig,
	) (nativeProducer, error) {
		return producer, nil
	}
	worker, err = NewWorkerE(options...)
	if err != nil {
		t.Fatalf("NewWorkerE(topic): %v", err)
	}
	if err := worker.Shutdown(); err != nil {
		t.Fatalf("Shutdown(): %v", err)
	}
}

func TestAdapterMapsEveryNativePublishOutcome(t *testing.T) {
	operationErr := errors.New("operation failed")
	returned := &rabbitmqqueue.Return{Code: 312, Exchange: "events", RoutingKey: "missing"}
	tests := map[string]struct {
		result rabbitmqqueue.PublishResult
		err    error
		want   error
	}{
		"operation error": {
			result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed},
			err:    operationErr, want: operationErr,
		},
		"confirmed": {result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed}},
		"returned": {
			result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishReturned, Return: returned},
			want:   rabbitmqqueue.ErrPublishReturned,
		},
		"rejected": {
			result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishRejected},
			want:   rabbitmqqueue.ErrPublishRejected,
		},
		"ambiguous": {
			result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishAmbiguous},
			want:   rabbitmqqueue.ErrPublishAmbiguous,
		},
		"not sent": {
			result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishNotSent},
			want:   rabbitmqqueue.ErrProducerUnavailable,
		},
		"invalid state": {
			result: rabbitmqqueue.PublishResult{}, want: rabbitmqqueue.ErrProducerUnavailable,
		},
		"invalid return": {
			result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishReturned},
			want:   rabbitmqqueue.ErrProducerUnavailable,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := adapterPublishError(test.result, test.err)
			if test.want == nil && err != nil {
				t.Fatalf("adapterPublishError(): %v", err)
			}
			if test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("adapterPublishError() = %v, want %v", err, test.want)
			}
		})
	}
}

func TestAdapterQueueRequiresStableMessageIdentity(t *testing.T) {
	for name, identity := range map[string]func(core.TaskMessage) (string, error){
		"empty": func(core.TaskMessage) (string, error) { return "", nil },
		"error": func(core.TaskMessage) (string, error) {
			return "untrusted", errors.New("identity unavailable")
		},
	} {
		t.Run(name, func(t *testing.T) {
			config := testNativeConfig()
			config.MessageID = identity
			producer := &recordingNativeProducer{
				result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed},
			}
			adapter := &adapterWorker{
				config: config, producer: producer, lifetime: t.Context(),
			}
			err := adapter.queue(newOptions(
				WithExchangeName("events"), WithExchangeType(ExchangeDirect),
				WithRoutingKey("jobs.created"),
			), adapterTask{body: []byte("payload")})
			if !errors.Is(err, queue.ErrInvalidConfiguration) ||
				!errors.Is(err, rabbitmqqueue.ErrMessageIDRequired) {
				t.Fatalf("queue error = %v, want stable identity failure", err)
			}
			if len(producer.publications) != 0 {
				t.Fatalf("publications = %d, want none", len(producer.publications))
			}
		})
	}
}

func TestAdapterQueueRejectsUnsupportedExchangeAtBoundary(t *testing.T) {
	config := testNativeConfig()
	adapter := &adapterWorker{
		config: config,
		producer: &recordingNativeProducer{
			result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed},
		},
		lifetime: t.Context(),
	}
	err := adapter.queue(newOptions(
		WithExchangeType(ExchangeFanout), WithExchangeName("events"),
	), adapterTask{body: []byte("payload")})
	if !errors.Is(err, queue.ErrInvalidConfiguration) {
		t.Fatalf("queue error = %v, want invalid configuration", err)
	}
}

func TestAdapterDerivesIdentityAndPreservesDeliveryMetadata(t *testing.T) {
	producer := &recordingNativeProducer{
		result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed},
	}
	config := testNativeConfig()
	config.DeliveryMessageID = func(rabbitmqqueue.Delivery, *job.Message) (string, error) {
		return "derived-job-1", nil
	}
	message := job.NewMessage(adapterTask{body: []byte("payload")})
	expiration := 3 * time.Second
	adapter := &adapterWorker{
		config: config,
		options: newOptions(
			WithQueue("jobs"), WithExchangeName("events"),
			WithExchangeType(ExchangeDirect), WithRoutingKey("jobs.created"),
		),
		producer: producer,
	}
	settlement := adapter.resolveFailure(
		t.Context(),
		rabbitmqqueue.Delivery{
			Body: message.Bytes(), CorrelationID: "correlation-1", ReplyTo: "reply",
			ContentType: "application/json", ContentEncoding: "identity",
			Type: "job", AppID: "orders", Timestamp: time.Unix(1, 0),
			Expiration: &expiration, Priority: 7,
			Headers: []rabbitmqqueue.Header{
				rabbitmqqueue.StringHeader("tenant", "one"),
			},
		},
		&message,
		errors.New("retryable"),
	)
	if settlement.Method != rabbitmqqueue.SettlementAcknowledge {
		t.Fatalf("source settlement = %#v, want ACK", settlement)
	}
	publication := producer.publications[0]
	if publication.Message.MessageID != "derived-job-1" ||
		publication.Message.CorrelationID != "correlation-1" ||
		publication.Message.Priority == nil || *publication.Message.Priority != 7 ||
		publication.Message.Expiration == nil || *publication.Message.Expiration != expiration {
		t.Fatalf("replacement metadata = %#v", publication.Message)
	}
}

func TestAdapterRequestReportsEveryBoundedTerminalState(t *testing.T) {
	consumerErr := errors.New("consumer open failed")
	tests := map[string]struct {
		prepare func(*adapterWorker, *recordingNativeConsumer)
		timeout time.Duration
		want    error
	}{
		"consumer open": {
			prepare: func(adapter *adapterWorker, _ *recordingNativeConsumer) {
				adapter.consumerErr = consumerErr
			},
			timeout: time.Hour, want: consumerErr,
		},
		"request timeout": {
			prepare: func(*adapterWorker, *recordingNativeConsumer) {},
			timeout: time.Nanosecond, want: queue.ErrNoTaskInQueue,
		},
		"consumer closed": {
			prepare: func(_ *adapterWorker, consumer *recordingNativeConsumer) {
				close(consumer.done)
			},
			timeout: time.Hour, want: queue.ErrQueueHasBeenClosed,
		},
		"worker closed": {
			prepare: func(adapter *adapterWorker, _ *recordingNativeConsumer) {
				adapter.cancel()
			},
			timeout: time.Hour, want: queue.ErrQueueHasBeenClosed,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			lifetime, cancel := context.WithCancel(t.Context())
			defer cancel()
			consumer := &recordingNativeConsumer{done: make(chan struct{})}
			adapter := &adapterWorker{
				config: testNativeConfig(), consumer: consumer,
				deliver: make(chan *adapterDelivery), lifetime: lifetime, cancel: cancel,
			}
			adapter.consumerOnce.Do(func() {})
			test.prepare(adapter, consumer)
			message, err := adapter.request(test.timeout)
			if message != nil || !errors.Is(err, test.want) {
				t.Fatalf("request() = (%v, %v), want %v", message, err, test.want)
			}
		})
	}
}

func TestAdapterHandlerReturnsConfiguredFailureWhenAdmissionIsCanceled(t *testing.T) {
	message := job.NewMessage(adapterTask{body: []byte("payload")})
	tests := map[string]struct {
		cancelContext  bool
		cancelLifetime bool
	}{
		"delivery context": {cancelContext: true},
		"worker lifetime":  {cancelLifetime: true},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			lifetime, cancelLifetime := context.WithCancel(t.Context())
			defer cancelLifetime()
			deliveryContext, cancelDelivery := context.WithCancel(t.Context())
			defer cancelDelivery()
			if test.cancelContext {
				cancelDelivery()
			}
			if test.cancelLifetime {
				cancelLifetime()
			}
			config := testNativeConfig()
			adapter := &adapterWorker{
				config: config, deliver: make(chan *adapterDelivery), lifetime: lifetime,
			}
			settlement, err := adapter.handle(deliveryContext, rabbitmqqueue.Delivery{
				Body: message.Bytes(), MessageID: "job-1",
			})
			if settlement != config.Consumer.Failure || !errors.Is(err, context.Canceled) {
				t.Fatalf("handle() = (%#v, %v), want configured failure and cancellation", settlement, err)
			}
		})
	}
}

func TestAdapterHandlerReturnsConfiguredFailureWhileAwaitingLegacySettlement(t *testing.T) {
	message := job.NewMessage(adapterTask{body: []byte("payload")})
	tests := map[string]struct {
		cancel func(context.CancelFunc, context.CancelFunc)
	}{
		"delivery context": {
			cancel: func(cancelDelivery, _ context.CancelFunc) { cancelDelivery() },
		},
		"worker lifetime": {
			cancel: func(_, cancelLifetime context.CancelFunc) { cancelLifetime() },
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			lifetime, cancelLifetime := context.WithCancel(t.Context())
			defer cancelLifetime()
			deliveryContext, cancelDelivery := context.WithCancel(t.Context())
			defer cancelDelivery()
			config := testNativeConfig()
			adapter := &adapterWorker{
				config: config, deliver: make(chan *adapterDelivery), lifetime: lifetime,
			}
			result := make(chan struct {
				settlement rabbitmqqueue.Settlement
				err        error
			}, 1)
			go func() {
				settlement, err := adapter.handle(deliveryContext, rabbitmqqueue.Delivery{
					Body: message.Bytes(), MessageID: "job-1",
				})
				result <- struct {
					settlement rabbitmqqueue.Settlement
					err        error
				}{settlement: settlement, err: err}
			}()
			<-adapter.deliver
			test.cancel(cancelDelivery, cancelLifetime)
			got := <-result
			if got.settlement != config.Consumer.Failure || !errors.Is(got.err, context.Canceled) {
				t.Fatalf("handle() = (%#v, %v), want configured failure and cancellation", got.settlement, got.err)
			}
		})
	}
}

func TestAdapterCloseAttemptsBothResourcesAndCachesResult(t *testing.T) {
	consumerCloseErr := errors.New("consumer close failed")
	producerCloseErr := errors.New("producer close failed")
	consumer := &recordingNativeConsumer{
		done: make(chan struct{}), closeErr: consumerCloseErr,
	}
	producer := &recordingNativeProducer{closeErr: producerCloseErr}
	lifetime, cancel := context.WithCancel(t.Context())
	adapter := &adapterWorker{
		config: testNativeConfig(), consumer: consumer, producer: producer,
		lifetime: lifetime, cancel: cancel,
	}
	err := adapter.close()
	if !errors.Is(err, consumerCloseErr) || !errors.Is(err, producerCloseErr) {
		t.Fatalf("close() error = %v, want both resource failures", err)
	}
	if err := adapter.close(); !errors.Is(err, consumerCloseErr) || !errors.Is(err, producerCloseErr) {
		t.Fatalf("repeated close() error = %v, want cached failures", err)
	}
	if consumer.closeCalls != 1 || producer.closeCalls != 1 {
		t.Fatalf("close calls = consumer %d producer %d, want one each", consumer.closeCalls, producer.closeCalls)
	}
	select {
	case <-lifetime.Done():
	default:
		t.Fatal("adapter lifetime remains active after close")
	}
}

func TestAdapterShutdownWaitsForAndClosesConsumerOpeningConcurrently(t *testing.T) {
	producer := &recordingNativeProducer{}
	consumerCloseErr := errors.New("consumer close failed")
	consumer := &recordingNativeConsumer{
		done: make(chan struct{}), closeErr: consumerCloseErr,
	}
	openStarted := make(chan struct{})
	releaseOpen := make(chan struct{})
	originalProducer := openNativeProducer
	originalConsumer := openNativeConsumer
	openNativeProducer = func(
		context.Context,
		rabbitmqqueue.ConnectionConfig,
		rabbitmqqueue.ProducerConfig,
	) (nativeProducer, error) {
		return producer, nil
	}
	openNativeConsumer = func(
		context.Context,
		rabbitmqqueue.ConnectionConfig,
		rabbitmqqueue.ConsumerConfig,
		rabbitmqqueue.DeliveryHandler,
	) (nativeConsumer, error) {
		close(openStarted)
		<-releaseOpen
		return consumer, nil
	}
	t.Cleanup(func() {
		openNativeProducer = originalProducer
		openNativeConsumer = originalConsumer
	})

	worker, err := NewWorkerE(
		WithNativeConfig(testNativeConfig()), WithQueue("jobs"), WithTag("worker"),
		WithExchangeName("events"), WithExchangeType(ExchangeDirect),
		WithRoutingKey("jobs.created"),
	)
	if err != nil {
		t.Fatalf("NewWorkerE(): %v", err)
	}
	requestDone := make(chan error, 1)
	go func() {
		_, requestErr := worker.Request()
		requestDone <- requestErr
	}()
	<-openStarted
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- worker.Shutdown() }()

	var shutdownErr error
	returnedBeforeOpen := false
	select {
	case shutdownErr = <-shutdownDone:
		returnedBeforeOpen = true
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseOpen)
	if !returnedBeforeOpen {
		shutdownErr = <-shutdownDone
	}
	if !errors.Is(shutdownErr, consumerCloseErr) {
		t.Fatalf("Shutdown() error = %v, want consumer close failure", shutdownErr)
	}
	if requestErr := <-requestDone; !errors.Is(requestErr, queue.ErrQueueHasBeenClosed) {
		t.Fatalf("Request() error = %v, want closed queue", requestErr)
	}
	if returnedBeforeOpen {
		t.Fatal("Shutdown() returned before the in-flight consumer open completed")
	}
	if consumer.closeCalls != 1 {
		t.Fatalf("consumer close calls = %d, want one", consumer.closeCalls)
	}
}

func TestAdapterDoesNotOpenConsumerAfterCloseStarts(t *testing.T) {
	producer := &recordingNativeProducer{}
	consumerOpens := 0
	originalProducer := openNativeProducer
	originalConsumer := openNativeConsumer
	openNativeProducer = func(
		context.Context,
		rabbitmqqueue.ConnectionConfig,
		rabbitmqqueue.ProducerConfig,
	) (nativeProducer, error) {
		return producer, nil
	}
	openNativeConsumer = func(
		context.Context,
		rabbitmqqueue.ConnectionConfig,
		rabbitmqqueue.ConsumerConfig,
		rabbitmqqueue.DeliveryHandler,
	) (nativeConsumer, error) {
		consumerOpens++
		return &recordingNativeConsumer{done: make(chan struct{})}, nil
	}
	t.Cleanup(func() {
		openNativeProducer = originalProducer
		openNativeConsumer = originalConsumer
	})

	worker, err := NewWorkerE(
		WithNativeConfig(testNativeConfig()), WithQueue("jobs"), WithTag("worker"),
		WithExchangeName("events"), WithExchangeType(ExchangeDirect),
		WithRoutingKey("jobs.created"),
	)
	if err != nil {
		t.Fatalf("NewWorkerE(): %v", err)
	}
	if err := worker.adapter.close(); err != nil {
		t.Fatalf("close(): %v", err)
	}
	if err := worker.adapter.ensureConsumer(); !errors.Is(err, queue.ErrQueueHasBeenClosed) {
		t.Fatalf("ensureConsumer() error = %v, want closed queue", err)
	}
	if consumerOpens != 0 {
		t.Fatalf("consumer opens = %d, want none", consumerOpens)
	}
}

func TestAdapterShutdownBoundsConcurrentConsumerOpen(t *testing.T) {
	producer := &recordingNativeProducer{}
	consumer := &recordingNativeConsumer{done: make(chan struct{})}
	openStarted := make(chan struct{})
	releaseOpen := make(chan struct{})
	originalProducer := openNativeProducer
	originalConsumer := openNativeConsumer
	openNativeProducer = func(
		context.Context,
		rabbitmqqueue.ConnectionConfig,
		rabbitmqqueue.ProducerConfig,
	) (nativeProducer, error) {
		return producer, nil
	}
	openNativeConsumer = func(
		context.Context,
		rabbitmqqueue.ConnectionConfig,
		rabbitmqqueue.ConsumerConfig,
		rabbitmqqueue.DeliveryHandler,
	) (nativeConsumer, error) {
		close(openStarted)
		<-releaseOpen
		return consumer, nil
	}
	t.Cleanup(func() {
		openNativeProducer = originalProducer
		openNativeConsumer = originalConsumer
	})

	config := testNativeConfig()
	config.Consumer.HandlerTimeout = time.Millisecond
	worker, err := NewWorkerE(
		WithNativeConfig(config), WithQueue("jobs"), WithTag("worker"),
		WithExchangeName("events"), WithExchangeType(ExchangeDirect),
		WithRoutingKey("jobs.created"),
	)
	if err != nil {
		t.Fatalf("NewWorkerE(): %v", err)
	}
	requestDone := make(chan error, 1)
	go func() {
		_, requestErr := worker.Request()
		requestDone <- requestErr
	}()
	<-openStarted
	shutdownErr := worker.Shutdown()
	if !errors.Is(shutdownErr, context.DeadlineExceeded) {
		t.Fatalf("Shutdown() error = %v, want bounded deadline", shutdownErr)
	}
	close(releaseOpen)
	if requestErr := <-requestDone; !errors.Is(requestErr, queue.ErrQueueHasBeenClosed) {
		t.Fatalf("Request() error = %v, want closed queue", requestErr)
	}
	if consumer.closeCalls != 1 {
		t.Fatalf("consumer close calls = %d, want one", consumer.closeCalls)
	}
}

func TestWorkerPreservesPublicSurfaceAndProducerConsumerIsolation(t *testing.T) {
	producer := &recordingNativeProducer{
		result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed},
	}
	consumerOpenErr := errors.New("consumer unavailable")
	consumerOpens := 0
	originalProducer := openNativeProducer
	originalConsumer := openNativeConsumer
	openNativeProducer = func(
		context.Context,
		rabbitmqqueue.ConnectionConfig,
		rabbitmqqueue.ProducerConfig,
	) (nativeProducer, error) {
		return producer, nil
	}
	openNativeConsumer = func(
		context.Context,
		rabbitmqqueue.ConnectionConfig,
		rabbitmqqueue.ConsumerConfig,
		rabbitmqqueue.DeliveryHandler,
	) (nativeConsumer, error) {
		consumerOpens++
		return nil, consumerOpenErr
	}
	t.Cleanup(func() {
		openNativeProducer = originalProducer
		openNativeConsumer = originalConsumer
	})

	runErr := errors.New("handler result")
	worker, err := NewWorkerE(
		WithNativeConfig(testNativeConfig()),
		WithAddr("amqp://legacy.invalid"),
		WithReconnectConfig(ReconnectConfig{MaxRetries: 9}),
		WithQueue("jobs"), WithTag("worker"),
		WithExchangeName("events"), WithExchangeType(ExchangeDirect),
		WithRoutingKey("jobs.created"), WithPublishTimeout(time.Second),
		WithLogger(queue.NewEmptyLogger()),
		WithRunFunc(func(context.Context, core.TaskMessage) error { return runErr }),
	)
	if err != nil {
		t.Fatalf("NewWorkerE(): %v", err)
	}
	if worker.BackendName() != "rabbitmq" || worker.QueueName() != "jobs" {
		t.Fatalf("worker metadata = %s/%s", worker.BackendName(), worker.QueueName())
	}
	if err := worker.Run(t.Context(), adapterTask{body: []byte("payload")}); !errors.Is(err, runErr) {
		t.Fatalf("Run() error = %v, want handler result", err)
	}
	if message, err := worker.Request(); message != nil || !errors.Is(err, consumerOpenErr) {
		t.Fatalf("Request() = (%v, %v), want consumer error", message, err)
	}
	if err := worker.startConsumer(); !errors.Is(err, consumerOpenErr) {
		t.Fatalf("startConsumer() error = %v, want cached consumer error", err)
	}
	if consumerOpens != 1 {
		t.Fatalf("consumer opens = %d, want one", consumerOpens)
	}
	if err := worker.Queue(adapterTask{body: []byte("payload")}); err != nil {
		t.Fatalf("Queue() after consumer failure: %v", err)
	}
	if err := worker.Shutdown(); err != nil {
		t.Fatalf("Shutdown(): %v", err)
	}
	if message, err := worker.Request(); message != nil || !errors.Is(err, queue.ErrQueueHasBeenClosed) {
		t.Fatalf("Request() after shutdown = (%v, %v)", message, err)
	}
	if err := worker.startConsumer(); !errors.Is(err, queue.ErrQueueShutdown) {
		t.Fatalf("startConsumer() after shutdown = %v", err)
	}
}

func TestAdapterShutdownAfterTypedNilConsumerOpenFailure(t *testing.T) {
	producer := &recordingNativeProducer{}
	consumerOpenErr := errors.New("consumer unavailable")
	originalProducer := openNativeProducer
	originalConsumer := openNativeConsumer
	openNativeProducer = func(
		context.Context,
		rabbitmqqueue.ConnectionConfig,
		rabbitmqqueue.ProducerConfig,
	) (nativeProducer, error) {
		return producer, nil
	}
	openNativeConsumer = func(
		context.Context,
		rabbitmqqueue.ConnectionConfig,
		rabbitmqqueue.ConsumerConfig,
		rabbitmqqueue.DeliveryHandler,
	) (nativeConsumer, error) {
		var consumer *rabbitmqqueue.Consumer
		return consumer, consumerOpenErr
	}
	t.Cleanup(func() {
		openNativeProducer = originalProducer
		openNativeConsumer = originalConsumer
	})

	worker, err := NewWorkerE(
		WithNativeConfig(testNativeConfig()), WithQueue("jobs"), WithTag("worker"),
		WithExchangeName("events"), WithExchangeType(ExchangeDirect),
		WithRoutingKey("jobs.created"),
	)
	if err != nil {
		t.Fatalf("NewWorkerE(): %v", err)
	}
	if message, err := worker.Request(); message != nil || !errors.Is(err, consumerOpenErr) {
		t.Fatalf("Request() = (%v, %v), want consumer error", message, err)
	}
	if err := worker.Shutdown(); err != nil {
		t.Fatalf("Shutdown(): %v", err)
	}
	if producer.closeCalls != 1 {
		t.Fatalf("producer close calls = %d, want one", producer.closeCalls)
	}
}

func TestNewWorkerPreservesPanicConstructor(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewWorker() did not panic for missing native policy")
		}
	}()
	_ = NewWorker()
}

func TestDefaultNativeBoundariesRejectInvalidInputs(t *testing.T) {
	if _, err := openNativeProducer(
		t.Context(), rabbitmqqueue.ConnectionConfig{}, rabbitmqqueue.ProducerConfig{},
	); err == nil {
		t.Fatal("openNativeProducer() accepted invalid configuration")
	}
	if _, err := openNativeConsumer(
		t.Context(), rabbitmqqueue.ConnectionConfig{}, rabbitmqqueue.ConsumerConfig{}, nil,
	); err == nil {
		t.Fatal("openNativeConsumer() accepted invalid configuration")
	}
	if err := awaitNativeSettlement(rabbitmqqueue.Delivery{}, t.Context()); !errors.Is(err, rabbitmqqueue.ErrSettlementResultUnavailable) {
		t.Fatalf("awaitNativeSettlement() error = %v", err)
	}
}

func TestAdapterRequeuesDeliveryWithoutStableIdentity(t *testing.T) {
	producer := &recordingNativeProducer{
		result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed},
	}
	config := testNativeConfig()
	config.DeliveryMessageID = nil
	message := job.NewMessage(adapterTask{body: []byte("payload")})
	adapter := &adapterWorker{
		config: config,
		options: newOptions(
			WithQueue("jobs"), WithExchangeName("events"),
			WithExchangeType(ExchangeDirect), WithRoutingKey("jobs.created"),
		),
		producer: producer,
	}
	settlement := adapter.resolveFailure(
		t.Context(), rabbitmqqueue.Delivery{Body: message.Bytes()},
		&message, errors.New("retryable"),
	)
	if settlement.Method != rabbitmqqueue.SettlementNegativeAcknowledge || !settlement.Requeue {
		t.Fatalf("source settlement = %#v, want requeue", settlement)
	}
	if len(producer.publications) != 0 {
		t.Fatalf("publications = %d, want none", len(producer.publications))
	}
}

func TestOptionsRetainInvalidLegacyExchangeForAdapterValidation(t *testing.T) {
	options := newOptions(
		WithLogger(queue.NewEmptyLogger()),
		WithExchangeType("unsupported"),
	)
	if options.exchangeType != "unsupported" {
		t.Fatalf("exchange type = %q, want unsupported", options.exchangeType)
	}
}

func TestNewWorkerReturnsConfiguredAdapter(t *testing.T) {
	producer := &recordingNativeProducer{}
	originalProducer := openNativeProducer
	openNativeProducer = func(
		context.Context,
		rabbitmqqueue.ConnectionConfig,
		rabbitmqqueue.ProducerConfig,
	) (nativeProducer, error) {
		return producer, nil
	}
	t.Cleanup(func() { openNativeProducer = originalProducer })

	worker := NewWorker(
		WithNativeConfig(testNativeConfig()), WithQueue("jobs"), WithTag("worker"),
		WithExchangeName("events"), WithExchangeType(ExchangeDirect),
		WithRoutingKey("jobs.created"),
	)
	if worker == nil || worker.adapter == nil {
		t.Fatal("NewWorker() did not return configured adapter")
	}
	if err := worker.Shutdown(); err != nil {
		t.Fatalf("Shutdown(): %v", err)
	}
}
