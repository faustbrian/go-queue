package rabbitmq

import (
	"testing"

	"github.com/faustbrian/go-queue/job"
	rabbitmqqueue "github.com/faustbrian/go-rabbitmq-queues"
)

func FuzzAdapterDeliveryAttempt(f *testing.F) {
	f.Add(uint8(rabbitmqqueue.HeaderInt64), int64(1))
	f.Add(uint8(rabbitmqqueue.HeaderString), int64(1))
	f.Add(uint8(rabbitmqqueue.HeaderInt64), job.MaxRetryCount+2)

	f.Fuzz(func(t *testing.T, kind uint8, value int64) {
		headerKind := rabbitmqqueue.HeaderKind(kind)
		attempt, valid := adapterDeliveryAttempt([]rabbitmqqueue.Header{{
			Key: deliveryAttemptHeader, Kind: headerKind, Int64: value,
		}})
		wantValid := headerKind == rabbitmqqueue.HeaderInt64 &&
			value >= 1 && value <= job.MaxRetryCount+1
		if valid != wantValid {
			t.Fatalf("attempt validity = %t, want %t", valid, wantValid)
		}
		if valid && attempt != value {
			t.Fatalf("attempt = %d, want %d", attempt, value)
		}
	})
}
