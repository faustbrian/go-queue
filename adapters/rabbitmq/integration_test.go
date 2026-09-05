//go:build integration

package rabbitmq

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	queue "github.com/faustbrian/go-queue"
	"github.com/faustbrian/go-queue/core"
	"github.com/faustbrian/go-queue/job"
	"github.com/faustbrian/go-queue/management"
	rabbitmqqueue "github.com/faustbrian/go-rabbitmq-queues"
)

const (
	liveAdapterConfigEnvironment = "RABBITMQ_ADAPTER_LIVE_CONFIG"
	maximumLiveAdapterConfig     = 64 << 10
	liveAdapterOperationTimeout  = 15 * time.Second
)

type liveAdapterEndpoint struct {
	Host string `json:"host"`
	Port uint16 `json:"port"`
}

type liveAdapterTLS struct {
	ServerName string `json:"server_name"`
	RootCAFile string `json:"root_ca_file"`
}

type liveAdapterQueue struct {
	Name       string                  `json:"name"`
	RoutingKey string                  `json:"routing_key"`
	QueueType  rabbitmqqueue.QueueType `json:"queue_type"`
}

type liveAdapterDeadLetter struct {
	Exchange   string `json:"exchange"`
	QueueName  string `json:"queue_name"`
	RoutingKey string `json:"routing_key"`
}

type liveAdapterFixture struct {
	Endpoints            []liveAdapterEndpoint `json:"endpoints"`
	VirtualHost          string                `json:"virtual_host"`
	Username             string                `json:"username"`
	Password             string                `json:"password"`
	TLS                  liveAdapterTLS        `json:"tls"`
	Exchange             string                `json:"exchange"`
	Jobs                 liveAdapterQueue      `json:"jobs"`
	Retry                liveAdapterQueue      `json:"retry"`
	DeadLetter           liveAdapterDeadLetter `json:"dead_letter"`
	UnroutableRoutingKey string                `json:"unroutable_routing_key"`
	MissingQueue         string                `json:"missing_queue"`
}

type liveAdapterPayload []byte

func (payload liveAdapterPayload) Bytes() []byte {
	return append([]byte(nil), payload...)
}

func TestLiveRabbitMQAdapterPolicy(t *testing.T) {
	fixture := readLiveAdapterFixture(t)

	t.Run("confirmed queue request and settlement", func(t *testing.T) {
		publisher := openLiveAdapterWorker(t, fixture, fixture.Jobs, fixture.Jobs.RoutingKey)
		consumer := openLiveAdapterWorker(t, fixture, fixture.Jobs, fixture.UnroutableRoutingKey)
		message := liveAdapterMessage("confirmed")
		if err := publisher.Queue(message); err != nil {
			t.Fatalf("queue confirmed message: %v", err)
		}
		delivery := requestLiveAdapterMessage(t, consumer)
		if !bytes.Equal(delivery.Payload(), message.Payload()) {
			t.Fatal("requested payload does not match the confirmed publication")
		}
		if err := delivery.(*job.Message).Ack(); err != nil {
			t.Fatalf("acknowledge confirmed delivery: %v", err)
		}
	})

	t.Run("producer and consumer failures remain isolated", func(t *testing.T) {
		consumer := openLiveAdapterWorker(t, fixture, fixture.Jobs, fixture.UnroutableRoutingKey)
		if err := consumer.Queue(liveAdapterMessage("unroutable")); !errors.Is(err, rabbitmqqueue.ErrPublishReturned) {
			t.Fatalf("unroutable queue error = %v, want mandatory return", err)
		}

		publisher := openLiveAdapterWorker(t, fixture, fixture.Jobs, fixture.Jobs.RoutingKey)
		if err := publisher.Queue(liveAdapterMessage("after-return")); err != nil {
			t.Fatalf("queue after independent producer return: %v", err)
		}
		if err := requestLiveAdapterMessage(t, consumer).(*job.Message).Ack(); err != nil {
			t.Fatalf("consumer settlement after producer return: %v", err)
		}

		missing := liveAdapterQueue{
			Name: fixture.MissingQueue, RoutingKey: fixture.Jobs.RoutingKey,
			QueueType: rabbitmqqueue.QueueClassic,
		}
		consumerFailure := openLiveAdapterWorker(t, fixture, missing, fixture.Jobs.RoutingKey)
		if err := consumerFailure.Queue(liveAdapterMessage("before-consumer-failure")); err != nil {
			t.Fatalf("queue before consumer failure: %v", err)
		}
		if _, err := consumerFailure.Request(); err == nil {
			t.Fatal("request unexpectedly opened a missing queue")
		}
		if err := consumerFailure.Queue(liveAdapterMessage("after-consumer-failure")); err != nil {
			t.Fatalf("queue after independent consumer failure: %v", err)
		}
		for range 2 {
			if err := requestLiveAdapterMessage(t, consumer).(*job.Message).Ack(); err != nil {
				t.Fatalf("settle publication from consumer-failed worker: %v", err)
			}
		}
	})

	t.Run("confirmed retry precedes source ack and terminal replacement", func(t *testing.T) {
		deadDeliveries := make(chan rabbitmqqueue.Delivery, 1)
		deadConsumer := openLiveDeadLetterConsumer(t, fixture, deadDeliveries)
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), liveAdapterOperationTimeout)
			defer cancel()
			if err := deadConsumer.Close(ctx); err != nil {
				t.Errorf("close live dead-letter consumer: %v", err)
			}
		})

		worker := openLiveAdapterWorker(t, fixture, fixture.Retry, fixture.Retry.RoutingKey)
		message := liveAdapterMessage("retry-then-terminal")
		if err := worker.Queue(message); err != nil {
			t.Fatalf("queue retry message: %v", err)
		}
		first := requestLiveAdapterMessage(t, worker).(*job.Message)
		retryable := management.NewFailure(
			management.ClassificationRetryable,
			"temporary_dependency",
			errors.New("sensitive retry cause"),
		)
		if err := first.NackFailure(retryable); err != nil {
			t.Fatalf("settle retry replacement: %v", err)
		}
		second := requestLiveAdapterMessage(t, worker).(*job.Message)
		if !bytes.Equal(second.Payload(), message.Payload()) {
			t.Fatal("retry replacement changed the task payload")
		}
		permanent := management.NewFailure(
			management.ClassificationPermanent,
			"invalid_job",
			errors.New("sensitive permanent cause"),
		)
		if err := second.NackFailure(permanent); err != nil {
			t.Fatalf("settle terminal replacement: %v", err)
		}

		select {
		case delivery := <-deadDeliveries:
			assertLiveAdapterHeader(t, delivery.Headers, deliveryAttemptHeader, int64(2))
			assertLiveAdapterHeader(t, delivery.Headers, classificationHeader, "permanent")
			assertLiveAdapterHeader(t, delivery.Headers, failureCodeHeader, "invalid_job")
			assertLiveAdapterHeader(t, delivery.Headers, sourceQueueHeader, fixture.Retry.Name)
		case <-time.After(liveAdapterOperationTimeout):
			t.Fatal("timed out waiting for confirmed terminal replacement")
		}
	})
}

func readLiveAdapterFixture(t *testing.T) liveAdapterFixture {
	t.Helper()
	path := os.Getenv(liveAdapterConfigEnvironment)
	if path == "" {
		t.Fatalf("%s must point to CI-hosted RabbitMQ configuration", liveAdapterConfigEnvironment)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open live adapter configuration: %v", err)
	}
	defer file.Close()
	contents, err := io.ReadAll(io.LimitReader(file, maximumLiveAdapterConfig+1))
	if err != nil {
		t.Fatalf("read live adapter configuration: %v", err)
	}
	if len(contents) > maximumLiveAdapterConfig {
		t.Fatal("live adapter configuration exceeds its bounded size")
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var fixture liveAdapterFixture
	if err := decoder.Decode(&fixture); err != nil {
		t.Fatalf("decode live adapter configuration: %v", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		t.Fatal("live adapter configuration must contain one JSON object")
	}
	if len(fixture.Endpoints) != 1 || fixture.Endpoints[0].Port == 0 ||
		fixture.VirtualHost == "" || fixture.Username == "" || fixture.Password == "" ||
		fixture.TLS.ServerName == "" || fixture.TLS.RootCAFile == "" ||
		fixture.Exchange == "" || fixture.Jobs.Name == "" || fixture.Retry.Name == "" ||
		fixture.DeadLetter.Exchange == "" || fixture.DeadLetter.QueueName == "" ||
		fixture.DeadLetter.RoutingKey == "" || fixture.UnroutableRoutingKey == "" ||
		fixture.MissingQueue == "" {
		t.Fatal("live adapter configuration is incomplete")
	}
	return fixture
}

func (fixture liveAdapterFixture) connection(t *testing.T) rabbitmqqueue.ConnectionConfig {
	t.Helper()
	file, err := os.Open(fixture.TLS.RootCAFile)
	if err != nil {
		t.Fatalf("read configured root CA: %v", err)
	}
	defer file.Close()
	rootCA, err := io.ReadAll(io.LimitReader(file, rabbitmqqueue.MaxTLSMaterialBytes+1))
	if err != nil {
		t.Fatalf("read configured root CA: %v", err)
	}
	if len(rootCA) == 0 || len(rootCA) > rabbitmqqueue.MaxTLSMaterialBytes {
		t.Fatal("configured root CA has an invalid bounded size")
	}
	password := fixture.Password
	return rabbitmqqueue.ConnectionConfig{
		Endpoints: []rabbitmqqueue.Endpoint{{
			Host: fixture.Endpoints[0].Host, Port: fixture.Endpoints[0].Port,
		}},
		VirtualHost: fixture.VirtualHost,
		Credentials: rabbitmqqueue.CredentialProviderFunc(
			func(context.Context) (rabbitmqqueue.Credentials, error) {
				return rabbitmqqueue.Credentials{
					Username: fixture.Username, Password: []byte(password),
				}, nil
			},
		),
		TLS: rabbitmqqueue.TLSConfig{
			ServerName: fixture.TLS.ServerName,
			RootCAs:    [][]byte{rootCA},
		},
		DialTimeout: 10 * time.Second,
		Heartbeat:   10 * time.Second,
		Recovery: rabbitmqqueue.RecoveryPolicy{
			MaxAttempts: 3, InitialDelay: 100 * time.Millisecond, MaxDelay: time.Second,
		},
	}
}

func openLiveAdapterWorker(
	t *testing.T,
	fixture liveAdapterFixture,
	resource liveAdapterQueue,
	routingKey string,
) *Worker {
	t.Helper()
	config := NativeConfig{
		Connection: fixture.connection(t),
		Producer: rabbitmqqueue.ProducerConfig{
			Limits: rabbitmqqueue.DefaultLimits(), MaxOutstanding: 16,
			PublishTimeout: liveAdapterOperationTimeout,
		},
		Consumer: rabbitmqqueue.ConsumerConfig{
			Limits:   rabbitmqqueue.DefaultLimits(),
			Queue:    rabbitmqqueue.QueueReference{Name: resource.Name, Type: resource.QueueType},
			Name:     "live-adapter-" + resource.Name,
			Prefetch: 4, Concurrency: 1,
			HandlerTimeout: liveAdapterOperationTimeout,
			MaxRequeues:    1, Failure: rabbitmqqueue.NegativeAcknowledge(true),
		},
		MessageID: func(task core.TaskMessage) (string, error) {
			digest := sha256.Sum256(task.Bytes())
			return hex.EncodeToString(digest[:]), nil
		},
	}
	worker, err := NewWorkerE(
		WithNativeConfig(config),
		WithQueue(resource.Name),
		WithTag("live-adapter-"+resource.Name),
		WithExchangeName(fixture.Exchange),
		WithExchangeType(ExchangeDirect),
		WithRoutingKey(routingKey),
		WithPublishTimeout(liveAdapterOperationTimeout),
		WithRequestTimeout(liveAdapterOperationTimeout),
		WithDeadLetter(DeadLetterConfig{
			Exchange:            fixture.DeadLetter.Exchange,
			Queue:               fixture.DeadLetter.QueueName,
			RoutingKey:          fixture.DeadLetter.RoutingKey,
			MaxDeliveryAttempts: 2,
		}),
	)
	if err != nil {
		t.Fatalf("open live adapter worker: %v", err)
	}
	t.Cleanup(func() {
		if err := worker.Shutdown(); err != nil && !errors.Is(err, queue.ErrQueueShutdown) {
			t.Errorf("shutdown live adapter worker: %v", err)
		}
	})
	return worker
}

func openLiveDeadLetterConsumer(
	t *testing.T,
	fixture liveAdapterFixture,
	deliveries chan<- rabbitmqqueue.Delivery,
) *rabbitmqqueue.Consumer {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), liveAdapterOperationTimeout)
	defer cancel()
	consumer, err := rabbitmqqueue.OpenConsumer(
		ctx,
		fixture.connection(t),
		rabbitmqqueue.ConsumerConfig{
			Limits: rabbitmqqueue.DefaultLimits(),
			Queue: rabbitmqqueue.QueueReference{
				Name: fixture.DeadLetter.QueueName, Type: rabbitmqqueue.QueueQuorum,
			},
			Name: "live-adapter-dead-letter", Prefetch: 1, Concurrency: 1,
			HandlerTimeout: liveAdapterOperationTimeout,
			MaxRequeues:    1, Failure: rabbitmqqueue.NegativeAcknowledge(true),
		},
		func(_ context.Context, delivery rabbitmqqueue.Delivery) (rabbitmqqueue.Settlement, error) {
			deliveries <- delivery
			return rabbitmqqueue.Acknowledge(), nil
		},
	)
	if err != nil {
		t.Fatalf("open live dead-letter consumer: %v", err)
	}
	return consumer
}

func liveAdapterMessage(payload string) *job.Message {
	message := job.NewMessage(liveAdapterPayload(payload))
	return &message
}

func requestLiveAdapterMessage(t *testing.T, worker *Worker) core.TaskMessage {
	t.Helper()
	message, err := worker.Request()
	if err != nil {
		t.Fatalf("request live adapter message: %v", err)
	}
	return message
}

func assertLiveAdapterHeader(t *testing.T, headers []rabbitmqqueue.Header, key string, want any) {
	t.Helper()
	got, found := adapterHeaderValue(headers, key)
	if !found || got != want {
		t.Fatalf("terminal header %s = %#v (found %t), want %#v", key, got, found, want)
	}
}
