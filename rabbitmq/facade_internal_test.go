package rabbitmq

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	queue "github.com/faustbrian/go-queue"
	successor "github.com/faustbrian/go-queue/adapters/rabbitmq"
	"github.com/faustbrian/go-queue/core"
	"github.com/faustbrian/go-queue/job"
	rabbitmqqueue "github.com/faustbrian/go-rabbitmq-queues"
)

type facadeMessage string

func (message facadeMessage) Bytes() []byte   { return []byte(message) }
func (message facadeMessage) Payload() []byte { return []byte(message) }

type facadeWorker struct {
	run      error
	shutdown error
	queue    error
	request  core.TaskMessage
}

func (*facadeWorker) BackendName() string { return "rabbitmq" }
func (*facadeWorker) QueueName() string   { return "jobs" }
func (worker *facadeWorker) Run(context.Context, core.TaskMessage) error {
	return worker.run
}
func (worker *facadeWorker) Shutdown() error                    { return worker.shutdown }
func (worker *facadeWorker) Queue(core.TaskMessage) error       { return worker.queue }
func (worker *facadeWorker) Request() (core.TaskMessage, error) { return worker.request, nil }

func TestLegacyWorkerDelegatesEveryRuntimeOperation(t *testing.T) {
	cause := errors.New("cause")
	delegate := &facadeWorker{
		run: cause, shutdown: cause, queue: cause, request: facadeMessage("delivery"),
	}
	factory := func(options ...successor.Option) (workerDelegate, error) {
		if len(options) != 1 {
			t.Fatalf("successor options = %d, want 1", len(options))
		}
		return delegate, nil
	}

	worker, err := newWorkerE(factory, WithQueue("jobs"))
	if err != nil {
		t.Fatalf("NewWorkerE() error = %v", err)
	}
	if worker.BackendName() != "rabbitmq" || worker.QueueName() != "jobs" {
		t.Fatalf("worker identity = (%q, %q)", worker.BackendName(), worker.QueueName())
	}
	message := facadeMessage("message")
	if !errors.Is(worker.Run(t.Context(), message), cause) ||
		!errors.Is(worker.Queue(message), cause) ||
		!errors.Is(worker.Shutdown(), cause) {
		t.Fatal("worker did not preserve delegate errors")
	}
	delivery, err := worker.Request()
	if err != nil || string(delivery.Payload()) != "delivery" {
		t.Fatalf("Request() = (%v, %v)", delivery, err)
	}
	if mustWorker(newWorkerE(factory, WithQueue("jobs"))).successor != delegate {
		t.Fatal("mustWorker() did not preserve delegate")
	}
}

func TestLegacyWorkerZeroValuePreservesReleasedState(t *testing.T) {
	worker := new(Worker)
	if worker.BackendName() != "rabbitmq" || worker.QueueName() != "" {
		t.Fatalf("zero worker identity = (%q, %q)", worker.BackendName(), worker.QueueName())
	}
	assertPanics := func(name string, invoke func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatalf("zero-value %s did not preserve released panic", name)
			}
		}()
		invoke()
	}
	assertPanics("Run", func() { _ = worker.Run(t.Context(), facadeMessage("message")) })
	assertPanics("Queue", func() { _ = worker.Queue(facadeMessage("message")) })
	assertPanics("Request", func() { _, _ = worker.Request() })
	assertPanics("first Shutdown", func() { _ = worker.Shutdown() })
	if !errors.Is(worker.Shutdown(), queue.ErrQueueShutdown) {
		t.Fatal("second zero-value Shutdown() did not preserve stopped state")
	}
	if !errors.Is(worker.Queue(facadeMessage("message")), queue.ErrQueueShutdown) {
		t.Fatal("zero-value Queue() did not preserve stopped state")
	}
	if _, err := worker.Request(); !errors.Is(err, queue.ErrQueueHasBeenClosed) {
		t.Fatal("zero-value Request() did not preserve stopped state")
	}
}

func TestLegacyWorkerTypedNilBackendNamePreservesReleasedState(t *testing.T) {
	var worker *Worker
	if worker.BackendName() != "rabbitmq" {
		t.Fatalf("typed-nil BackendName() = %q, want rabbitmq", worker.BackendName())
	}
}

func TestLegacyOptionsMapToMatchingSuccessorOption(t *testing.T) {
	run := func(context.Context, core.TaskMessage) error { return nil }
	logger := queue.NewLogger()
	priority := int32(-3)
	credentials := rabbitmqqueue.CredentialProviderFunc(func(context.Context) (rabbitmqqueue.Credentials, error) {
		return rabbitmqqueue.Credentials{Username: "user", Password: []byte("password")}, nil
	})
	messageID := func(core.TaskMessage) (string, error) { return "message", nil }
	deliveryMessageID := func(rabbitmqqueue.Delivery, *job.Message) (string, error) {
		return "delivery", nil
	}
	limits := rabbitmqqueue.Limits{
		MaxPayloadBytes: 1, MaxHeaderEntries: 2, MaxHeaderBytes: 3,
		MaxNameBytes: 4, MaxRoutingKeyBytes: 5,
	}
	native := NativeConfig{
		Connection: rabbitmqqueue.ConnectionConfig{
			Endpoints:   []rabbitmqqueue.Endpoint{{Host: "broker", Port: 5671}},
			VirtualHost: "vhost",
			Credentials: credentials,
			TLS: rabbitmqqueue.TLSConfig{
				ServerName: "broker", RootCAs: [][]byte{{1, 2}},
				ClientCertificate: []byte{3, 4}, ClientPrivateKey: []byte{5, 6},
			},
			DialTimeout: 2 * time.Second,
			Heartbeat:   3 * time.Second,
			Recovery: rabbitmqqueue.RecoveryPolicy{
				MaxAttempts: 4, InitialDelay: 5 * time.Second, MaxDelay: 6 * time.Second,
			},
		},
		Producer: rabbitmqqueue.ProducerConfig{
			Limits: limits, MaxOutstanding: 7, PublishTimeout: 8 * time.Second,
		},
		Consumer: rabbitmqqueue.ConsumerConfig{
			Limits: limits,
			Queue: rabbitmqqueue.QueueReference{
				Name: "queue", Type: rabbitmqqueue.QueueQuorum, SingleActiveConsumer: true,
				Transient: &rabbitmqqueue.TransientQueue{
					Exchange: rabbitmqqueue.Exchange{
						Name: "exchange", Kind: rabbitmqqueue.ExchangeTopic,
						Durable: true, AutoDelete: true, Internal: true,
					},
					RoutingKey: "route",
					Arguments:  []rabbitmqqueue.Header{rabbitmqqueue.BytesHeader("header", []byte{9})},
				},
			},
			Name: "consumer", Priority: &priority, Exclusive: true,
			Prefetch: 9, Concurrency: 10, HandlerTimeout: 11 * time.Second,
			MaxRequeues: 12, Failure: rabbitmqqueue.Reject(true),
		},
		MessageID: messageID, DeliveryMessageID: deliveryMessageID,
	}
	deadLetter := DeadLetterConfig{
		Exchange: "dead-exchange", Queue: "dead-queue", RoutingKey: "dead-route", MaxDeliveryAttempts: 13,
	}
	reconnect := ReconnectConfig{
		MaxRetries: 14, InitialDelay: 15 * time.Second, MaxDelay: 16 * time.Second,
	}
	tests := []struct {
		name      string
		legacy    Option
		successor successor.Option
	}{
		//lint:ignore SA1019 Exact deprecated successor mapping is the compatibility oracle.
		{name: "address", legacy: WithAddr("amqp://example"), successor: successor.WithAddr("amqp://example")}, //nolint:staticcheck // The preceding Staticcheck directive documents the exact deprecated parity call.
		{name: "auto acknowledgment", legacy: WithAutoAck(true), successor: successor.WithAutoAck(true)},
		{name: "dead letter", legacy: WithDeadLetter(deadLetter), successor: successor.WithDeadLetter(successor.DeadLetterConfig(deadLetter))},
		{name: "exchange name", legacy: WithExchangeName("exchange"), successor: successor.WithExchangeName("exchange")},
		{name: "exchange type", legacy: WithExchangeType(ExchangeTopic), successor: successor.WithExchangeType(successor.ExchangeTopic)},
		{name: "logger", legacy: WithLogger(logger), successor: successor.WithLogger(logger)},
		{name: "native config", legacy: WithNativeConfig(native), successor: successor.WithNativeConfig(successor.NativeConfig(native))},
		{name: "publish timeout", legacy: WithPublishTimeout(time.Second), successor: successor.WithPublishTimeout(time.Second)},
		{name: "queue", legacy: WithQueue("queue"), successor: successor.WithQueue("queue")},
		//lint:ignore SA1019 Exact deprecated successor mapping is the compatibility oracle.
		{name: "reconnect", legacy: WithReconnectConfig(reconnect), successor: successor.WithReconnectConfig(successor.ReconnectConfig(reconnect))}, //nolint:staticcheck // The preceding Staticcheck directive documents the exact deprecated parity call.
		{name: "request timeout", legacy: WithRequestTimeout(time.Second), successor: successor.WithRequestTimeout(time.Second)},
		{name: "routing key", legacy: WithRoutingKey("key"), successor: successor.WithRoutingKey("key")},
		{name: "run", legacy: WithRunFunc(run), successor: successor.WithRunFunc(run)},
		{name: "tag", legacy: WithTag("consumer"), successor: successor.WithTag("consumer")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var captured successor.Option
			_, err := newWorkerE(func(options ...successor.Option) (workerDelegate, error) {
				if len(options) != 1 {
					t.Fatalf("successor options = %d, want 1", len(options))
				}
				captured = options[0]
				return new(facadeWorker), nil
			}, test.legacy)
			if err != nil {
				t.Fatalf("newWorkerE() error = %v", err)
			}
			assertSuccessorOptionStateEqual(t, captured, test.successor)
		})
	}

	t.Run("zero values", func(t *testing.T) {
		assertMappedOptionsEqual(t,
			[]Option{
				WithAddr(""), WithAutoAck(false), WithDeadLetter(DeadLetterConfig{}),
				WithExchangeName(""), WithExchangeType(""), WithLogger(nil),
				WithNativeConfig(NativeConfig{}), WithPublishTimeout(0), WithQueue(""),
				WithReconnectConfig(ReconnectConfig{}), WithRequestTimeout(0),
				WithRoutingKey(""), WithRunFunc(nil), WithTag(""),
			},
			[]successor.Option{
				//lint:ignore SA1019 Zero-value parity includes the deprecated successor option.
				successor.WithAddr(""), successor.WithAutoAck(false), //nolint:staticcheck // The preceding Staticcheck directive documents the deprecated zero-value parity call.
				successor.WithDeadLetter(successor.DeadLetterConfig{}),
				successor.WithExchangeName(""), successor.WithExchangeType(""),
				successor.WithLogger(nil), successor.WithNativeConfig(successor.NativeConfig{}),
				successor.WithPublishTimeout(0), successor.WithQueue(""),
				//lint:ignore SA1019 Zero-value parity includes the deprecated successor option.
				successor.WithReconnectConfig(successor.ReconnectConfig{}), //nolint:staticcheck // The preceding Staticcheck directive documents the deprecated zero-value parity call.
				successor.WithRequestTimeout(0), successor.WithRoutingKey(""),
				successor.WithRunFunc(nil), successor.WithTag(""),
			},
		)
	})

	t.Run("ordering", func(t *testing.T) {
		assertMappedOptionsEqual(t,
			[]Option{WithQueue("first"), WithQueue("second"), WithAutoAck(true), WithAutoAck(false)},
			[]successor.Option{
				successor.WithQueue("first"), successor.WithQueue("second"),
				successor.WithAutoAck(true), successor.WithAutoAck(false),
			},
		)
	})
}

func assertMappedOptionsEqual(t *testing.T, legacy []Option, expected []successor.Option) {
	t.Helper()
	var captured []successor.Option
	_, err := newWorkerE(func(options ...successor.Option) (workerDelegate, error) {
		captured = options
		return new(facadeWorker), nil
	}, legacy...)
	if err != nil {
		t.Fatalf("newWorkerE() error = %v", err)
	}
	if len(captured) != len(expected) {
		t.Fatalf("successor options = %d, want %d", len(captured), len(expected))
	}
	for index := range expected {
		assertSuccessorOptionStateEqual(t, captured[index], expected[index])
	}
}

func assertSuccessorOptionStateEqual(t *testing.T, actual, expected successor.Option) {
	t.Helper()
	apply := func(option successor.Option) reflect.Value {
		function := reflect.ValueOf(option)
		configured := reflect.New(function.Type().In(0).Elem())
		function.Call([]reflect.Value{configured})
		return configured.Elem()
	}
	assertReflectedValueEqual(t, "successor option state", apply(actual), apply(expected))
}

func assertReflectedValueEqual(t *testing.T, path string, actual, expected reflect.Value) {
	t.Helper()
	if actual.Type() != expected.Type() {
		t.Fatalf("%s type = %v, want %v", path, actual.Type(), expected.Type())
	}
	switch actual.Kind() {
	case reflect.Interface:
		if actual.IsNil() != expected.IsNil() {
			t.Fatalf("%s nil = %v, want %v", path, actual.IsNil(), expected.IsNil())
		}
		if !actual.IsNil() {
			assertReflectedValueEqual(t, path, actual.Elem(), expected.Elem())
		}
	case reflect.Pointer, reflect.Func, reflect.Chan, reflect.UnsafePointer:
		if actual.IsNil() != expected.IsNil() || (!actual.IsNil() && actual.Pointer() != expected.Pointer()) {
			t.Fatalf("%s identity differs", path)
		}
	case reflect.Struct:
		for index := range actual.NumField() {
			assertReflectedValueEqual(t, path+"."+actual.Type().Field(index).Name, actual.Field(index), expected.Field(index))
		}
	case reflect.Slice, reflect.Array:
		if actual.Kind() == reflect.Slice && actual.IsNil() != expected.IsNil() {
			t.Fatalf("%s nil = %v, want %v", path, actual.IsNil(), expected.IsNil())
		}
		if actual.Len() != expected.Len() {
			t.Fatalf("%s length = %d, want %d", path, actual.Len(), expected.Len())
		}
		for index := range actual.Len() {
			assertReflectedValueEqual(t, path, actual.Index(index), expected.Index(index))
		}
	case reflect.String:
		if actual.String() != expected.String() {
			t.Fatalf("%s = %q, want %q", path, actual.String(), expected.String())
		}
	case reflect.Bool:
		if actual.Bool() != expected.Bool() {
			t.Fatalf("%s = %v, want %v", path, actual.Bool(), expected.Bool())
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if actual.Int() != expected.Int() {
			t.Fatalf("%s = %d, want %d", path, actual.Int(), expected.Int())
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if actual.Uint() != expected.Uint() {
			t.Fatalf("%s = %d, want %d", path, actual.Uint(), expected.Uint())
		}
	default:
		t.Fatalf("%s has unsupported kind %v", path, actual.Kind())
	}
}
