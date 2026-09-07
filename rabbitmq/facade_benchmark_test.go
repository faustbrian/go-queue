package rabbitmq

import "testing"

func BenchmarkLegacyWorkerQueue(benchmark *testing.B) {
	worker := &Worker{successor: new(facadeWorker)}
	message := facadeMessage("payload")
	benchmark.ReportAllocs()
	for benchmark.Loop() {
		if err := worker.Queue(message); err != nil {
			benchmark.Fatal(err)
		}
	}
}
