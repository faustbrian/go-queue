package rabbitmq_test

import (
	"testing"

	"github.com/faustbrian/go-queue/adapters/rabbitmq"
)

func TestSuccessorExposesRabbitMQCompatibilityAPI(t *testing.T) {
	options := []rabbitmq.Option{
		rabbitmq.WithQueue("jobs"),
		rabbitmq.WithExchangeName("jobs"),
		rabbitmq.WithExchangeType(rabbitmq.ExchangeDirect),
		rabbitmq.WithRoutingKey("jobs"),
	}
	if len(options) != 4 {
		t.Fatalf("option count = %d, want 4", len(options))
	}
}
