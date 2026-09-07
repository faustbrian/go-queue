package rabbitmq

import (
	"testing"
	"time"

	successor "github.com/faustbrian/go-queue/adapters/rabbitmq"
)

func FuzzLegacyOptionValuesReachSuccessor(f *testing.F) {
	f.Add("queue", int64(time.Second), true)
	f.Add("", int64(0), false)
	f.Fuzz(func(t *testing.T, value string, nanoseconds int64, autoAck bool) {
		const maximumTextBytes = 256
		if len(value) > maximumTextBytes {
			value = value[:maximumTextBytes]
		}
		duration := time.Duration(nanoseconds)
		assertMappedOptionsEqual(t,
			[]Option{
				WithQueue(value), WithRoutingKey(value), WithExchangeName(value),
				WithPublishTimeout(duration), WithRequestTimeout(duration), WithAutoAck(autoAck),
			},
			[]successor.Option{
				successor.WithQueue(value), successor.WithRoutingKey(value), successor.WithExchangeName(value),
				successor.WithPublishTimeout(duration), successor.WithRequestTimeout(duration), successor.WithAutoAck(autoAck),
			},
		)
	})
}
