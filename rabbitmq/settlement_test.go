package rabbitmq

import (
	"testing"

	"github.com/faustbrian/go-queue/job"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

type recordingAcknowledger struct {
	acks  int
	nacks int
}

func (a *recordingAcknowledger) Ack(uint64, bool) error {
	a.acks++
	return nil
}

func (a *recordingAcknowledger) Nack(uint64, bool, bool) error {
	a.nacks++
	return nil
}

func (a *recordingAcknowledger) Reject(uint64, bool) error { return nil }

func TestRequestDefersRabbitMQSettlement(t *testing.T) {
	acknowledger := &recordingAcknowledger{}
	message := job.NewTask(nil)
	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- amqp.Delivery{
		Acknowledger: acknowledger,
		DeliveryTag:  1,
		Body:         message.Bytes(),
	}
	worker := &Worker{opts: newOptions(), tasks: deliveries}
	worker.startOnce.Do(func() {})

	task, err := worker.Request()
	require.NoError(t, err)
	require.Zero(t, acknowledger.acks)
	require.Zero(t, acknowledger.nacks)

	delivery := task.(*job.Message)
	require.NoError(t, delivery.Ack())
	require.NoError(t, delivery.Nack())
	require.Equal(t, 1, acknowledger.acks)
	require.Equal(t, 1, acknowledger.nacks)
}
