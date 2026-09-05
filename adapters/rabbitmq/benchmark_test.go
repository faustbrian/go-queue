package rabbitmq

import (
	"context"
	"testing"

	"github.com/faustbrian/go-queue/core"
	rabbitmqqueue "github.com/faustbrian/go-rabbitmq-queues"
)

type benchmarkNativeProducer struct{}

func (benchmarkNativeProducer) Publish(
	context.Context,
	rabbitmqqueue.Publication,
) (rabbitmqqueue.PublishResult, error) {
	return rabbitmqqueue.PublishResult{State: rabbitmqqueue.PublishConfirmed}, nil
}

func (benchmarkNativeProducer) Close(context.Context) error { return nil }

func BenchmarkAdapterConfirmedPublishOverhead(benchmark *testing.B) {
	config := testNativeConfig()
	config.MessageID = func(core.TaskMessage) (string, error) { return "benchmark-job", nil }
	adapter := &adapterWorker{
		config:   config,
		producer: benchmarkNativeProducer{},
		lifetime: benchmark.Context(),
	}
	options := newOptions(
		WithExchangeName("events"),
		WithExchangeType(ExchangeDirect),
		WithRoutingKey("jobs"),
	)
	task := adapterTask{body: []byte("representative adapter payload")}

	benchmark.ReportAllocs()
	benchmark.ResetTimer()
	for range benchmark.N {
		if err := adapter.queue(options, task); err != nil {
			benchmark.Fatal(err)
		}
	}
}
