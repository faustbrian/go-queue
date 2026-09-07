package rabbitmq_test

import (
	"context"
	"testing"
	"time"

	successor "github.com/faustbrian/go-queue/adapters/rabbitmq"
	"github.com/faustbrian/go-queue/core"
	legacy "github.com/faustbrian/go-queue/rabbitmq"
)

func TestLegacyFacadePreservesContracts(t *testing.T) {
	var _ core.Worker = (*legacy.Worker)(nil)
	if legacy.ExchangeDirect != successor.ExchangeDirect || legacy.ExchangeTopic != successor.ExchangeTopic {
		t.Fatal("legacy exchange constants changed")
	}
}

func TestLegacyOptionsDelegateToSuccessor(t *testing.T) {
	options := []legacy.Option{
		legacy.WithAddr("amqp://localhost"),
		legacy.WithAutoAck(true),
		legacy.WithDeadLetter(legacy.DeadLetterConfig{}),
		legacy.WithExchangeName("exchange"),
		legacy.WithExchangeType(legacy.ExchangeDirect),
		legacy.WithLogger(nil),
		legacy.WithNativeConfig(legacy.NativeConfig{}),
		legacy.WithPublishTimeout(time.Second),
		legacy.WithQueue("queue"),
		legacy.WithReconnectConfig(legacy.ReconnectConfig{}),
		legacy.WithRequestTimeout(time.Second),
		legacy.WithRoutingKey("routing-key"),
		legacy.WithRunFunc(func(context.Context, core.TaskMessage) error { return nil }),
		legacy.WithTag("consumer"),
	}
	for index, option := range options {
		if option == nil {
			t.Fatalf("legacy option %d is nil", index)
		}
	}
	if worker, err := legacy.NewWorkerE(options...); worker != nil || err == nil {
		t.Fatalf("NewWorkerE(all options) = (%v, %v), want validation error", worker, err)
	}
}

func TestLegacyConstructorsPreserveValidation(t *testing.T) {
	if worker, err := legacy.NewWorkerE(); worker != nil || err == nil {
		t.Fatalf("NewWorkerE() = (%v, %v), want nil worker and validation error", worker, err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("NewWorker() did not preserve successor panic contract")
		}
	}()
	legacy.NewWorker()
}
