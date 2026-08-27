package rabbitmq_test

import (
	"context"
	"time"

	"github.com/faustbrian/go-queue/core"
	rabbitmq "github.com/faustbrian/go-queue/rabbitmq"
	rabbitmqqueue "github.com/faustbrian/go-rabbitmq-queues"
)

func ExampleNewWorkerE() {
	credentials := rabbitmqqueue.CredentialProviderFunc(
		func(context.Context) (rabbitmqqueue.Credentials, error) {
			return rabbitmqqueue.Credentials{Username: "worker", Password: []byte("owned-secret")}, nil
		},
	)
	_, _ = rabbitmq.NewWorkerE(
		rabbitmq.WithNativeConfig(rabbitmq.NativeConfig{
			Connection: rabbitmqqueue.ConnectionConfig{
				Endpoints:   []rabbitmqqueue.Endpoint{{Host: "rabbitmq.internal", Port: 5671}},
				VirtualHost: "/", Credentials: credentials,
				TLS:         rabbitmqqueue.TLSConfig{ServerName: "rabbitmq.internal"},
				DialTimeout: 5 * time.Second, Heartbeat: 30 * time.Second,
				Recovery: rabbitmqqueue.RecoveryPolicy{
					MaxAttempts: 8, InitialDelay: 100 * time.Millisecond, MaxDelay: 5 * time.Second,
				},
			},
			Producer: rabbitmqqueue.ProducerConfig{
				Limits: rabbitmqqueue.DefaultLimits(), MaxOutstanding: 256,
				PublishTimeout: 5 * time.Second,
			},
			Consumer: rabbitmqqueue.ConsumerConfig{
				Limits: rabbitmqqueue.DefaultLimits(),
				Queue:  rabbitmqqueue.QueueReference{Name: "jobs", Type: rabbitmqqueue.QueueQuorum},
				Name:   "jobs-worker", Prefetch: 32, Concurrency: 8,
				HandlerTimeout: time.Minute, MaxRequeues: 1,
				Failure: rabbitmqqueue.Reject(false),
			},
			MessageID: func(core.TaskMessage) (string, error) { return "stable-job-id", nil },
		}),
		rabbitmq.WithQueue("jobs"),
		rabbitmq.WithTag("jobs-worker"),
		rabbitmq.WithExchangeName("jobs.events"),
		rabbitmq.WithExchangeType(rabbitmq.ExchangeTopic),
		rabbitmq.WithRoutingKey("jobs.created"),
	)
}
