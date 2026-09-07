package queueservice

import (
	"context"
	"testing"

	"github.com/faustbrian/go-correlation"
	"github.com/faustbrian/go-queue/core"
	"github.com/faustbrian/go-queue/job"
)

func BenchmarkLegacyProducerPublish(benchmark *testing.B) {
	factory, err := correlation.NewFactory(correlation.FactoryOptions{})
	if err != nil {
		benchmark.Fatal(err)
	}
	producer, err := NewProducer(ProducerOptions[int]{
		Name: "benchmark-producer", Resource: 1, Correlation: factory,
		Publish: func(context.Context, int, core.QueuedMessage, ...job.AllowOption) error { return nil },
	})
	if err != nil {
		benchmark.Fatal(err)
	}
	component := producer.Component()
	if err = component.Start(context.Background()); err != nil {
		benchmark.Fatal(err)
	}
	ctx := facadeProducerContext()
	message := facadeQueuedPayload("payload")
	benchmark.ReportAllocs()
	benchmark.ResetTimer()
	for benchmark.Loop() {
		if _, err = producer.Publish(ctx, message); err != nil {
			benchmark.Fatal(err)
		}
	}
}
