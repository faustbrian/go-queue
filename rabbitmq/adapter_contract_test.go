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
	"github.com/faustbrian/go-queue/management"
	rabbitmqqueue "github.com/faustbrian/go-rabbitmq-queues"
)

type adapterTask struct{ body []byte }

func (task adapterTask) Bytes() []byte   { return append([]byte(nil), task.body...) }
func (task adapterTask) Payload() []byte { return append([]byte(nil), task.body...) }

type recordingNativeProducer struct {
	mu           sync.Mutex
	publications []rabbitmqqueue.Publication
	result       rabbitmqqueue.PublishResult
	err          error
	closeCalls   int
	closeErr     error
}

func TestAdapterConfirmsRetryReplacementBeforeAcknowledgingSource(t *testing.T) {
	producer := &recordingNativeProducer{result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed}}
	consumer := &recordingNativeConsumer{done: make(chan struct{})}
	handlers := make(chan rabbitmqqueue.DeliveryHandler, 1)
	originalProducer := openNativeProducer
	originalConsumer := openNativeConsumer
	originalAwait := awaitNativeSettlement
	openNativeProducer = func(context.Context, rabbitmqqueue.ConnectionConfig, rabbitmqqueue.ProducerConfig) (nativeProducer, error) {
		return producer, nil
	}
	openNativeConsumer = func(_ context.Context, _ rabbitmqqueue.ConnectionConfig, _ rabbitmqqueue.ConsumerConfig, handler rabbitmqqueue.DeliveryHandler) (nativeConsumer, error) {
		handlers <- handler
		return consumer, nil
	}
	awaitNativeSettlement = func(rabbitmqqueue.Delivery, context.Context) error { return nil }
	t.Cleanup(func() {
		openNativeProducer = originalProducer
		openNativeConsumer = originalConsumer
		awaitNativeSettlement = originalAwait
	})

	worker, err := NewWorkerE(
		WithNativeConfig(testNativeConfig()), WithQueue("jobs"), WithTag("worker"),
		WithExchangeName("events"), WithExchangeType(ExchangeDirect), WithRoutingKey("jobs.created"),
		WithRequestTimeout(time.Second),
	)
	if err != nil {
		t.Fatalf("NewWorkerE(): %v", err)
	}
	requested := make(chan core.TaskMessage, 1)
	go func() {
		message, _ := worker.Request()
		requested <- message
	}()
	handler := <-handlers
	envelope := job.NewMessage(adapterTask{body: []byte("payload")})
	handlerResult := make(chan rabbitmqqueue.Settlement, 1)
	go func() {
		settlement, _ := handler(t.Context(), rabbitmqqueue.Delivery{
			Body: envelope.Bytes(), MessageID: "job-1", ContentType: "application/json",
			Exchange: "events", RoutingKey: "jobs.created",
			Headers: []rabbitmqqueue.Header{rabbitmqqueue.Int64Header(deliveryAttemptHeader, 1)},
		})
		handlerResult <- settlement
	}()
	delivery := (<-requested).(*job.Message)
	failure := management.NewFailure(
		management.ClassificationRetryable, "temporary_failure", errors.New("sensitive cause"),
	)
	if err := delivery.NackFailure(failure); err != nil {
		t.Fatalf("NackFailure(): %v", err)
	}
	if settlement := <-handlerResult; settlement.Method != rabbitmqqueue.SettlementAcknowledge {
		t.Fatalf("source settlement = %#v, want ACK after replacement confirm", settlement)
	}
	producer.mu.Lock()
	defer producer.mu.Unlock()
	if len(producer.publications) != 1 {
		t.Fatalf("replacement publications = %d, want one", len(producer.publications))
	}
	replacement := producer.publications[0]
	if replacement.Exchange != "events" || replacement.RoutingKey != "jobs.created" ||
		replacement.Message.MessageID != "job-1" || !replacement.Mandatory {
		t.Fatalf("replacement = %#v", replacement)
	}
	foundAttempt := false
	for _, header := range replacement.Message.Headers {
		if header.Key == deliveryAttemptHeader && header.Kind == rabbitmqqueue.HeaderInt64 && header.Int64 == 2 {
			foundAttempt = true
		}
	}
	if !foundAttempt {
		t.Fatalf("replacement headers = %#v, want attempt 2", replacement.Message.Headers)
	}
}

func TestAdapterKeepsSourceRecoverableUnlessReplacementIsConfirmed(t *testing.T) {
	t.Parallel()

	returned := &rabbitmqqueue.Return{
		Code: 312, Exchange: "events", RoutingKey: "jobs.created",
	}
	for name, result := range map[string]rabbitmqqueue.PublishResult{
		"not sent":  {State: rabbitmqqueue.PublishNotSent},
		"rejected":  {State: rabbitmqqueue.PublishRejected},
		"returned":  {State: rabbitmqqueue.PublishReturned, Return: returned},
		"ambiguous": {State: rabbitmqqueue.PublishAmbiguous},
	} {
		t.Run(name, func(t *testing.T) {
			producer := &recordingNativeProducer{result: result}
			message := job.NewMessage(adapterTask{body: []byte("payload")})
			opts := newOptions(
				WithQueue("jobs"), WithExchangeName("events"),
				WithExchangeType(ExchangeDirect), WithRoutingKey("jobs.created"),
			)
			adapter := &adapterWorker{
				config: testNativeConfig(), options: opts, producer: producer,
			}
			settlement := adapter.resolveFailure(
				t.Context(),
				rabbitmqqueue.Delivery{
					Body: []byte("payload"), MessageID: "job-1",
					Headers: []rabbitmqqueue.Header{
						rabbitmqqueue.Int64Header(deliveryAttemptHeader, 1),
					},
				},
				&message,
				management.NewFailure(
					management.ClassificationRetryable,
					"temporary_failure",
					errors.New("sensitive cause"),
				),
			)
			if settlement.Method != rabbitmqqueue.SettlementNegativeAcknowledge || !settlement.Requeue {
				t.Fatalf("source settlement = %#v, want requeue for %s replacement", settlement, result.State)
			}
		})
	}
}

func TestAdapterDeadLettersMalformedAttemptMetadata(t *testing.T) {
	t.Parallel()

	producer := &recordingNativeProducer{
		result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed},
	}
	message := job.NewMessage(adapterTask{body: []byte("payload")})
	opts := newOptions(
		WithQueue("jobs"), WithExchangeName("events"),
		WithExchangeType(ExchangeDirect), WithRoutingKey("jobs.created"),
		WithDeadLetter(DeadLetterConfig{
			Exchange: "events-dead", Queue: "jobs-dead",
			RoutingKey: "jobs.dead", MaxDeliveryAttempts: 5,
		}),
	)
	adapter := &adapterWorker{config: testNativeConfig(), options: opts, producer: producer}
	settlement := adapter.resolveFailure(
		t.Context(),
		rabbitmqqueue.Delivery{
			Body: []byte("payload"), MessageID: "job-1",
			Headers: []rabbitmqqueue.Header{
				rabbitmqqueue.Int64Header(deliveryAttemptHeader, int64(job.MaxRetryCount+2)),
			},
		},
		&message,
		errors.New("temporary failure"),
	)
	if settlement.Method != rabbitmqqueue.SettlementAcknowledge {
		t.Fatalf("source settlement = %#v, want ACK after terminal confirm", settlement)
	}
	producer.mu.Lock()
	defer producer.mu.Unlock()
	if len(producer.publications) != 1 {
		t.Fatalf("dead-letter publications = %d, want one", len(producer.publications))
	}
	publication := producer.publications[0]
	if publication.Exchange != "events-dead" || publication.RoutingKey != "jobs.dead" {
		t.Fatalf("dead-letter destination = %s/%s", publication.Exchange, publication.RoutingKey)
	}
	wantHeaders := map[string]any{
		deliveryAttemptHeader: int64(1),
		classificationHeader:  string(management.ClassificationMalformed),
		failureCodeHeader:     "malformed_delivery_attempt",
	}
	for key, want := range wantHeaders {
		if got, found := adapterHeaderValue(publication.Message.Headers, key); !found || got != want {
			t.Fatalf("header %s = %#v (found %t), want %#v", key, got, found, want)
		}
	}
}

func TestAdapterRequeuesWhenLegacyMessageIdentityCannotBeDerived(t *testing.T) {
	t.Parallel()

	producer := &recordingNativeProducer{
		result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed},
	}
	config := testNativeConfig()
	config.DeliveryMessageID = func(rabbitmqqueue.Delivery, *job.Message) (string, error) {
		return "derived-but-untrusted", errors.New("identity source unavailable")
	}
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
		t.Context(),
		rabbitmqqueue.Delivery{
			Body: message.Bytes(),
			Headers: []rabbitmqqueue.Header{
				rabbitmqqueue.Int64Header(deliveryAttemptHeader, 1),
			},
		},
		&message,
		errors.New("temporary failure"),
	)
	if settlement.Method != rabbitmqqueue.SettlementNegativeAcknowledge || !settlement.Requeue {
		t.Fatalf("source settlement = %#v, want requeue", settlement)
	}
	producer.mu.Lock()
	defer producer.mu.Unlock()
	if len(producer.publications) != 0 {
		t.Fatalf("replacement publications = %d, want none", len(producer.publications))
	}
}

func TestAdapterShutdownRetainsLegacyRepeatedCallContract(t *testing.T) {
	producer := &recordingNativeProducer{
		result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed},
	}
	originalProducer := openNativeProducer
	openNativeProducer = func(
		context.Context,
		rabbitmqqueue.ConnectionConfig,
		rabbitmqqueue.ProducerConfig,
	) (nativeProducer, error) {
		return producer, nil
	}
	t.Cleanup(func() { openNativeProducer = originalProducer })

	worker, err := NewWorkerE(
		WithNativeConfig(testNativeConfig()), WithQueue("jobs"), WithTag("worker"),
		WithExchangeName("events"), WithExchangeType(ExchangeDirect),
		WithRoutingKey("jobs.created"),
	)
	if err != nil {
		t.Fatalf("NewWorkerE(): %v", err)
	}
	if err := worker.Shutdown(); err != nil {
		t.Fatalf("first Shutdown(): %v", err)
	}
	if err := worker.Queue(adapterTask{body: []byte("payload")}); !errors.Is(err, queue.ErrQueueShutdown) {
		t.Fatalf("Queue() after shutdown error = %v, want %v", err, queue.ErrQueueShutdown)
	}
	if err := worker.Shutdown(); !errors.Is(err, queue.ErrQueueShutdown) {
		t.Fatalf("second Shutdown() error = %v, want %v", err, queue.ErrQueueShutdown)
	}
	producer.mu.Lock()
	defer producer.mu.Unlock()
	if producer.closeCalls != 1 {
		t.Fatalf("producer close calls = %d, want one", producer.closeCalls)
	}
}

func TestAdapterDeadLettersMalformedTaskEnvelopeAfterConfirmation(t *testing.T) {
	t.Parallel()

	producer := &recordingNativeProducer{
		result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed},
	}
	adapter := &adapterWorker{
		config: testNativeConfig(),
		options: newOptions(
			WithQueue("jobs"), WithExchangeName("events"),
			WithExchangeType(ExchangeDirect), WithRoutingKey("jobs.created"),
			WithDeadLetter(DeadLetterConfig{
				Exchange: "events-dead", Queue: "jobs-dead",
				RoutingKey: "jobs.dead", MaxDeliveryAttempts: 5,
			}),
		),
		producer: producer,
	}
	settlement, err := adapter.handle(t.Context(), rabbitmqqueue.Delivery{
		Body: []byte("not-a-go-queue-envelope"), MessageID: "job-1",
	})
	if err != nil {
		t.Fatalf("handle malformed envelope: %v", err)
	}
	if settlement.Method != rabbitmqqueue.SettlementAcknowledge {
		t.Fatalf("source settlement = %#v, want ACK after terminal confirm", settlement)
	}
	producer.mu.Lock()
	defer producer.mu.Unlock()
	if len(producer.publications) != 1 {
		t.Fatalf("dead-letter publications = %d, want one", len(producer.publications))
	}
	publication := producer.publications[0]
	if publication.Exchange != "events-dead" || publication.RoutingKey != "jobs.dead" {
		t.Fatalf("dead-letter destination = %s/%s", publication.Exchange, publication.RoutingKey)
	}
	wantHeaders := map[string]any{
		classificationHeader: string(management.ClassificationMalformed),
		failureCodeHeader:    "malformed_delivery",
	}
	for key, want := range wantHeaders {
		if got, found := adapterHeaderValue(publication.Message.Headers, key); !found || got != want {
			t.Fatalf("header %s = %#v (found %t), want %#v", key, got, found, want)
		}
	}
}

func TestAdapterBareNackRequeuesWithoutPublishingReplacement(t *testing.T) {
	t.Parallel()

	producer := &recordingNativeProducer{
		result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed},
	}
	message := job.NewMessage(adapterTask{body: []byte("payload")})
	adapter := &adapterWorker{
		config: testNativeConfig(),
		options: newOptions(
			WithQueue("jobs"), WithExchangeName("events"),
			WithExchangeType(ExchangeDirect), WithRoutingKey("jobs.created"),
		),
		producer: producer,
	}
	settlement := adapter.resolveFailure(
		t.Context(),
		rabbitmqqueue.Delivery{Body: message.Bytes(), MessageID: "job-1"},
		&message,
		nil,
	)
	if settlement.Method != rabbitmqqueue.SettlementNegativeAcknowledge || !settlement.Requeue {
		t.Fatalf("source settlement = %#v, want requeue", settlement)
	}
	producer.mu.Lock()
	defer producer.mu.Unlock()
	if len(producer.publications) != 0 {
		t.Fatalf("replacement publications = %d, want none", len(producer.publications))
	}
}

func TestAdapterRequeuesCanceledAndInfrastructureFailures(t *testing.T) {
	t.Parallel()

	for name, failure := range map[string]error{
		"canceled": context.Canceled,
		"infrastructure": management.NewFailure(
			management.ClassificationInfrastructure,
			management.FailureCodeLeaseLost,
			errors.New("broker ownership changed"),
		),
	} {
		t.Run(name, func(t *testing.T) {
			producer := &recordingNativeProducer{
				result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed},
			}
			message := job.NewMessage(adapterTask{body: []byte("payload")})
			adapter := &adapterWorker{
				config: testNativeConfig(),
				options: newOptions(
					WithQueue("jobs"), WithExchangeName("events"),
					WithExchangeType(ExchangeDirect), WithRoutingKey("jobs.created"),
				),
				producer: producer,
			}
			settlement := adapter.resolveFailure(
				t.Context(),
				rabbitmqqueue.Delivery{Body: message.Bytes(), MessageID: "job-1"},
				&message,
				failure,
			)
			if settlement.Method != rabbitmqqueue.SettlementNegativeAcknowledge || !settlement.Requeue {
				t.Fatalf("source settlement = %#v, want requeue", settlement)
			}
			producer.mu.Lock()
			defer producer.mu.Unlock()
			if len(producer.publications) != 0 {
				t.Fatalf("replacement publications = %d, want none", len(producer.publications))
			}
		})
	}
}

func TestAdapterPublishesTerminalFailuresWithLegacyDeadLetterMetadata(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		failure error
		attempt int64
		code    string
	}{
		"permanent": {
			failure: management.NewFailure(
				management.ClassificationPermanent,
				"invalid_order",
				errors.New("invalid order detail"),
			),
			attempt: 1,
			code:    "invalid_order",
		},
		"attempts exhausted": {
			failure: errors.New("temporary failure"),
			attempt: 5,
			code:    "attempts_exhausted",
		},
	} {
		t.Run(name, func(t *testing.T) {
			producer := &recordingNativeProducer{
				result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed},
			}
			message := job.NewMessage(adapterTask{body: []byte("payload")})
			adapter := &adapterWorker{
				config: testNativeConfig(),
				options: newOptions(
					WithQueue("jobs"), WithExchangeName("events"),
					WithExchangeType(ExchangeDirect), WithRoutingKey("jobs.created"),
					WithDeadLetter(DeadLetterConfig{
						Exchange: "events-dead", Queue: "jobs-dead",
						RoutingKey: "jobs.dead", MaxDeliveryAttempts: 5,
					}),
				),
				producer: producer,
			}
			settlement := adapter.resolveFailure(
				t.Context(),
				rabbitmqqueue.Delivery{
					Body: message.Bytes(), MessageID: "job-1",
					Headers: []rabbitmqqueue.Header{
						rabbitmqqueue.Int64Header(deliveryAttemptHeader, test.attempt),
					},
				},
				&message,
				test.failure,
			)
			if settlement.Method != rabbitmqqueue.SettlementAcknowledge {
				t.Fatalf("source settlement = %#v, want ACK after terminal confirm", settlement)
			}
			producer.mu.Lock()
			defer producer.mu.Unlock()
			if len(producer.publications) != 1 {
				t.Fatalf("dead-letter publications = %d, want one", len(producer.publications))
			}
			publication := producer.publications[0]
			if publication.Exchange != "events-dead" || publication.RoutingKey != "jobs.dead" {
				t.Fatalf("dead-letter destination = %s/%s", publication.Exchange, publication.RoutingKey)
			}
			wantHeaders := map[string]any{
				deliveryAttemptHeader:  test.attempt,
				failureCodeHeader:      test.code,
				sourceQueueHeader:      "jobs",
				sourceExchangeHeader:   "events",
				sourceRoutingKeyHeader: "jobs.created",
			}
			for key, want := range wantHeaders {
				if got, found := adapterHeaderValue(publication.Message.Headers, key); !found || got != want {
					t.Fatalf("header %s = %#v (found %t), want %#v", key, got, found, want)
				}
			}
		})
	}
}

func adapterHeaderValue(headers []rabbitmqqueue.Header, key string) (any, bool) {
	for _, header := range headers {
		if header.Key != key {
			continue
		}
		switch header.Kind {
		case rabbitmqqueue.HeaderString:
			return header.String, true
		case rabbitmqqueue.HeaderInt64:
			return header.Int64, true
		default:
			return nil, true
		}
	}
	return nil, false
}

func (producer *recordingNativeProducer) Publish(
	_ context.Context,
	publication rabbitmqqueue.Publication,
) (rabbitmqqueue.PublishResult, error) {
	producer.mu.Lock()
	defer producer.mu.Unlock()
	producer.publications = append(producer.publications, publication)
	return producer.result, producer.err
}

func (producer *recordingNativeProducer) Close(context.Context) error {
	producer.mu.Lock()
	defer producer.mu.Unlock()
	producer.closeCalls++
	return producer.closeErr
}

type recordingNativeConsumer struct {
	done       chan struct{}
	closeOnce  sync.Once
	closeCalls int
	err        error
	closeErr   error
}

func (consumer *recordingNativeConsumer) Done() <-chan struct{} { return consumer.done }
func (consumer *recordingNativeConsumer) Err() error            { return consumer.err }
func (consumer *recordingNativeConsumer) Close(context.Context) error {
	consumer.closeCalls++
	consumer.closeOnce.Do(func() { close(consumer.done) })
	return consumer.closeErr
}

func testNativeConfig() NativeConfig {
	return NativeConfig{
		Connection: rabbitmqqueue.ConnectionConfig{
			Endpoints:   []rabbitmqqueue.Endpoint{{Host: "rabbitmq.internal", Port: 5671}},
			VirtualHost: "/",
			Credentials: rabbitmqqueue.CredentialProviderFunc(func(context.Context) (rabbitmqqueue.Credentials, error) {
				return rabbitmqqueue.Credentials{Username: "worker", Password: []byte("credential")}, nil
			}),
			TLS:         rabbitmqqueue.TLSConfig{ServerName: "rabbitmq.internal"},
			DialTimeout: time.Second,
			Heartbeat:   30 * time.Second,
			Recovery: rabbitmqqueue.RecoveryPolicy{
				MaxAttempts: 2, InitialDelay: time.Millisecond, MaxDelay: time.Second,
			},
		},
		Producer: rabbitmqqueue.ProducerConfig{
			Limits: rabbitmqqueue.DefaultLimits(), MaxOutstanding: 4, PublishTimeout: time.Second,
		},
		Consumer: rabbitmqqueue.ConsumerConfig{
			Limits: rabbitmqqueue.DefaultLimits(),
			Queue:  rabbitmqqueue.QueueReference{Name: "jobs", Type: rabbitmqqueue.QueueClassic},
			Name:   "worker", Prefetch: 4, Concurrency: 2, HandlerTimeout: time.Second,
			MaxRequeues: 1, Failure: rabbitmqqueue.Reject(false),
		},
		MessageID: func(core.TaskMessage) (string, error) { return "job-1", nil },
	}
}

func TestAdapterRequiresExplicitNativePolicy(t *testing.T) {
	worker, err := NewWorkerE()
	if worker != nil || !errors.Is(err, queue.ErrInvalidConfiguration) {
		t.Fatalf("NewWorkerE() = (%v, %v), want explicit-policy failure", worker, err)
	}
}

func TestAdapterRejectsUnsafeDeadLetterPolicyBeforeOpeningProducer(t *testing.T) {
	producerOpens := 0
	originalProducer := openNativeProducer
	openNativeProducer = func(
		context.Context,
		rabbitmqqueue.ConnectionConfig,
		rabbitmqqueue.ProducerConfig,
	) (nativeProducer, error) {
		producerOpens++
		return &recordingNativeProducer{}, nil
	}
	t.Cleanup(func() { openNativeProducer = originalProducer })

	worker, err := NewWorkerE(
		WithNativeConfig(testNativeConfig()), WithQueue("jobs"), WithTag("worker"),
		WithExchangeName("events"), WithExchangeType(ExchangeDirect),
		WithRoutingKey("jobs.created"),
		WithDeadLetter(DeadLetterConfig{
			Exchange: "events-dead", Queue: "jobs-dead",
			RoutingKey: "jobs.dead", MaxDeliveryAttempts: 1,
		}),
	)
	if worker != nil || !errors.Is(err, queue.ErrInvalidConfiguration) {
		t.Fatalf("NewWorkerE() = (%v, %v), want invalid dead-letter policy", worker, err)
	}
	if producerOpens != 0 {
		t.Fatalf("producer opens = %d, want none", producerOpens)
	}
}

func TestAdapterQueueUsesProducerOnlyMandatoryNativePublication(t *testing.T) {
	producer := &recordingNativeProducer{result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed}}
	producerOpens := 0
	consumerOpens := 0
	originalProducer := openNativeProducer
	originalConsumer := openNativeConsumer
	openNativeProducer = func(context.Context, rabbitmqqueue.ConnectionConfig, rabbitmqqueue.ProducerConfig) (nativeProducer, error) {
		producerOpens++
		return producer, nil
	}
	openNativeConsumer = func(context.Context, rabbitmqqueue.ConnectionConfig, rabbitmqqueue.ConsumerConfig, rabbitmqqueue.DeliveryHandler) (nativeConsumer, error) {
		consumerOpens++
		return nil, errors.New("consumer must remain unopened")
	}
	t.Cleanup(func() {
		openNativeProducer = originalProducer
		openNativeConsumer = originalConsumer
	})

	worker, err := NewWorkerE(
		WithNativeConfig(testNativeConfig()), WithQueue("jobs"), WithTag("worker"),
		WithExchangeName("events"), WithExchangeType(ExchangeDirect), WithRoutingKey("jobs.created"),
	)
	if err != nil {
		t.Fatalf("NewWorkerE(): %v", err)
	}
	if err := worker.Queue(adapterTask{body: []byte("payload")}); err != nil {
		t.Fatalf("Queue(): %v", err)
	}
	if producerOpens != 1 || consumerOpens != 0 {
		t.Fatalf("resource opens = producer %d consumer %d", producerOpens, consumerOpens)
	}
	producer.mu.Lock()
	defer producer.mu.Unlock()
	if len(producer.publications) != 1 {
		t.Fatalf("publications = %d, want one", len(producer.publications))
	}
	publication := producer.publications[0]
	if !publication.Mandatory || publication.DeliveryMode != rabbitmqqueue.DeliveryPersistent ||
		publication.Exchange != "events" || publication.ExchangeKind != rabbitmqqueue.ExchangeDirect ||
		publication.RoutingKey != "jobs.created" || publication.Message.MessageID != "job-1" ||
		string(publication.Message.Body) != "payload" {
		t.Fatalf("publication = %#v", publication)
	}
}

func TestAdapterRequestWaitsForNativeBrokerSettlement(t *testing.T) {
	producer := &recordingNativeProducer{result: rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed}}
	consumer := &recordingNativeConsumer{done: make(chan struct{})}
	handlers := make(chan rabbitmqqueue.DeliveryHandler, 1)
	originalProducer := openNativeProducer
	originalConsumer := openNativeConsumer
	originalAwait := awaitNativeSettlement
	openNativeProducer = func(context.Context, rabbitmqqueue.ConnectionConfig, rabbitmqqueue.ProducerConfig) (nativeProducer, error) {
		return producer, nil
	}
	openNativeConsumer = func(_ context.Context, _ rabbitmqqueue.ConnectionConfig, _ rabbitmqqueue.ConsumerConfig, handler rabbitmqqueue.DeliveryHandler) (nativeConsumer, error) {
		handlers <- handler
		return consumer, nil
	}
	settlementStarted := make(chan struct{})
	releaseSettlement := make(chan struct{})
	awaitCalls := 0
	awaitNativeSettlement = func(rabbitmqqueue.Delivery, context.Context) error {
		awaitCalls++
		close(settlementStarted)
		<-releaseSettlement
		return nil
	}
	t.Cleanup(func() {
		openNativeProducer = originalProducer
		openNativeConsumer = originalConsumer
		awaitNativeSettlement = originalAwait
	})

	worker, err := NewWorkerE(
		WithNativeConfig(testNativeConfig()), WithQueue("jobs"), WithTag("worker"),
		WithExchangeName("events"), WithExchangeType(ExchangeDirect), WithRoutingKey("jobs.created"),
		WithRequestTimeout(time.Second),
	)
	if err != nil {
		t.Fatalf("NewWorkerE(): %v", err)
	}
	requested := make(chan core.TaskMessage, 1)
	requestErrors := make(chan error, 1)
	go func() {
		message, requestErr := worker.Request()
		requested <- message
		requestErrors <- requestErr
	}()
	handler := <-handlers
	envelope := job.NewMessage(adapterTask{body: []byte("payload")})
	handlerResult := make(chan rabbitmqqueue.Settlement, 1)
	go func() {
		settlement, _ := handler(t.Context(), rabbitmqqueue.Delivery{
			Body: envelope.Bytes(), MessageID: "job-1",
		})
		handlerResult <- settlement
	}()
	message := <-requested
	if err := <-requestErrors; err != nil {
		t.Fatalf("Request(): %v", err)
	}
	delivery, ok := message.(*job.Message)
	if !ok || !delivery.AcknowledgementRequired() {
		t.Fatalf("Request() message = %#v, want settlement-aware job", message)
	}
	acknowledged := make(chan error, 1)
	go func() { acknowledged <- delivery.Ack() }()
	<-settlementStarted
	select {
	case err := <-acknowledged:
		t.Fatalf("Ack() returned before broker settlement: %v", err)
	case <-time.After(time.Millisecond):
	}
	close(releaseSettlement)
	if err := <-acknowledged; err != nil {
		t.Fatalf("Ack(): %v", err)
	}
	if settlement := <-handlerResult; settlement.Method != rabbitmqqueue.SettlementAcknowledge {
		t.Fatalf("handler settlement = %#v, want ACK", settlement)
	}
	if err := delivery.NackFailure(errors.New("late failure")); err != nil {
		t.Fatalf("repeated settlement: %v", err)
	}
	if awaitCalls != 1 {
		t.Fatalf("broker settlement waits = %d, want one", awaitCalls)
	}
}
