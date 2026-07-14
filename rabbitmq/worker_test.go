package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	queue "github.com/faustbrian/go-queue"
	"github.com/faustbrian/go-queue/core"
	"github.com/faustbrian/go-queue/job"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOptionsConfigureRabbitMQ(t *testing.T) {
	logger := queue.NewEmptyLogger()
	runErr := errors.New("run")
	reconnect := ReconnectConfig{MaxRetries: 3, InitialDelay: time.Millisecond, MaxDelay: time.Second}
	opts := newOptions(
		WithAddr("amqp://rabbit"),
		WithExchangeName("events"),
		WithExchangeType(ExchangeTopic),
		WithRoutingKey("jobs.created"),
		WithTag("worker-1"),
		WithAutoAck(true),
		WithQueue("jobs"),
		WithRunFunc(func(context.Context, core.TaskMessage) error { return runErr }),
		WithLogger(logger),
		WithReconnectConfig(reconnect),
		WithRequestTimeout(25*time.Millisecond),
	)

	assert.Equal(t, "amqp://rabbit", opts.addr)
	assert.Equal(t, "events", opts.exchangeName)
	assert.Equal(t, ExchangeTopic, opts.exchangeType)
	assert.Equal(t, "jobs.created", opts.routingKey)
	assert.Equal(t, "worker-1", opts.tag)
	assert.True(t, opts.autoAck)
	assert.Equal(t, "jobs", opts.queue)
	assert.Equal(t, logger, opts.logger)
	assert.Equal(t, reconnect, opts.reconnect)
	assert.Equal(t, 25*time.Millisecond, opts.requestTimeout)
	assert.ErrorIs(t, opts.runFunc(context.Background(), nil), runErr)
	assert.NoError(t, newOptions().runFunc(context.Background(), nil))
	newOptions(WithLogger(logger), WithExchangeType("invalid"))

	for _, exchange := range []string{ExchangeDirect, ExchangeFanout, ExchangeTopic, ExchangeHeaders} {
		assert.True(t, isVaildExchange(exchange))
	}
}

func TestDialWithRetryValidatesRetriesAndBacksOff(t *testing.T) {
	_, err := dialWithRetry("amqp://rabbit", ReconnectConfig{})
	assert.ErrorContains(t, err, "at least one")

	original := dialAMQP
	t.Cleanup(func() { dialAMQP = original })
	attempts := 0
	dialAMQP = func(string) (amqpConnection, error) {
		attempts++
		if attempts < 3 {
			return nil, errors.New("unavailable")
		}
		return &fakeAMQPConnection{}, nil
	}
	connection, err := dialWithRetry("amqp://rabbit", ReconnectConfig{
		MaxRetries: 3, InitialDelay: time.Nanosecond, MaxDelay: time.Nanosecond,
	})
	require.NoError(t, err)
	assert.NotNil(t, connection)
	assert.Equal(t, 3, attempts)

	dialAMQP = func(string) (amqpConnection, error) {
		return nil, errors.New("unavailable")
	}
	connection, err = dialWithRetry("amqp://rabbit", ReconnectConfig{
		MaxRetries: 2, InitialDelay: 0, MaxDelay: 0,
	})
	assert.Nil(t, connection)
	assert.ErrorContains(t, err, "after retries")
}

func TestOpenRabbitMQReturnsDialAndChannelErrors(t *testing.T) {
	originalDial := dialAMQP
	t.Cleanup(func() {
		dialAMQP = originalDial
	})

	dialAMQP = func(string) (amqpConnection, error) { return nil, errors.New("dial") }
	connection, channel, err := openRabbitMQ("amqp://rabbit", ReconnectConfig{MaxRetries: 1})
	assert.Nil(t, connection)
	assert.Nil(t, channel)
	assert.Error(t, err)

	dialAMQP = func(string) (amqpConnection, error) {
		return &fakeAMQPConnection{openErr: errors.New("channel")}, nil
	}
	connection, channel, err = openRabbitMQ("amqp://rabbit", ReconnectConfig{MaxRetries: 1})
	assert.Nil(t, connection)
	assert.Nil(t, channel)
	assert.ErrorContains(t, err, "channel")

	expectedChannel := &amqp.Channel{}
	dialAMQP = func(string) (amqpConnection, error) {
		return &fakeAMQPConnection{rawChannel: expectedChannel}, nil
	}
	connection, channel, err = openRabbitMQ("amqp://rabbit", ReconnectConfig{MaxRetries: 1})
	require.NoError(t, err)
	assert.NotNil(t, connection)
	assert.Same(t, expectedChannel, channel)
}

func TestWorkerConstructors(t *testing.T) {
	t.Run("invalid exchange", func(t *testing.T) {
		worker, err := NewWorkerE(
			WithLogger(queue.NewEmptyLogger()),
			WithExchangeType("invalid"),
		)
		assert.Nil(t, worker)
		assert.ErrorContains(t, err, "exchange type")
	})

	t.Run("connection", func(t *testing.T) {
		withRabbitConnector(t, nil, nil, errors.New("connect"))
		worker, err := NewWorkerE()
		assert.Nil(t, worker)
		assert.ErrorIs(t, err, errConnect)
	})

	t.Run("exchange declaration", func(t *testing.T) {
		connection := &fakeAMQPConnection{}
		channel := &fakeAMQPChannel{exchangeErr: errors.New("exchange")}
		withRabbitConnector(t, connection, channel, nil)
		worker, err := NewWorkerE()
		assert.Nil(t, worker)
		assert.ErrorContains(t, err, "declare")
		assert.Equal(t, 1, connection.closes)
		assert.Equal(t, 1, channel.closes)
	})

	t.Run("success", func(t *testing.T) {
		connection := &fakeAMQPConnection{}
		channel := &fakeAMQPChannel{}
		withRabbitConnector(t, connection, channel, nil)
		worker := NewWorker()
		require.NoError(t, worker.Shutdown())
	})

	t.Run("legacy panic", func(t *testing.T) {
		withRabbitConnector(t, nil, nil, errors.New("connect"))
		assert.Panics(t, func() { NewWorker() })
	})
}

var errConnect = errors.New("connect")

func TestStartConsumerCoversSetupStages(t *testing.T) {
	for _, test := range []struct {
		name      string
		configure func(*fakeAMQPChannel)
	}{
		{name: "success"},
		{name: "declare error", configure: func(c *fakeAMQPChannel) { c.queueErr = errors.New("declare") }},
		{name: "bind error", configure: func(c *fakeAMQPChannel) { c.bindErr = errors.New("bind") }},
		{name: "consume error", configure: func(c *fakeAMQPChannel) { c.consumeErr = errors.New("consume") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			channel := &fakeAMQPChannel{deliveries: make(chan amqp.Delivery)}
			if test.configure != nil {
				test.configure(channel)
			}
			worker := &Worker{
				channel: channel,
				opts:    newOptions(WithLogger(queue.NewEmptyLogger())),
			}

			err := worker.startConsumer()
			if test.configure == nil {
				assert.NoError(t, err)
				assert.Equal(t, channel.deliveries, worker.tasks)
				assert.NoError(t, worker.startConsumer())
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestWorkerRunQueueRequestAndShutdown(t *testing.T) {
	expectedRun := errors.New("run")
	connection := &fakeAMQPConnection{}
	deliveries := make(chan amqp.Delivery, 1)
	channel := &fakeAMQPChannel{deliveries: deliveries}
	worker := &Worker{
		conn:    connection,
		channel: channel,
		stop:    make(chan struct{}),
		opts: newOptions(
			WithLogger(queue.NewEmptyLogger()),
			WithAutoAck(true),
			WithRunFunc(func(context.Context, core.TaskMessage) error { return expectedRun }),
		),
		tasks: deliveries,
	}
	worker.startOnce.Do(func() {})
	message := job.NewMessage(rawMessage("payload"))

	assert.ErrorIs(t, worker.Run(context.Background(), &message), expectedRun)
	require.NoError(t, worker.Queue(&message))
	assert.Equal(t, message.Bytes(), channel.published.Body)

	deliveries <- amqp.Delivery{Body: message.Bytes()}
	received, err := worker.Request()
	require.NoError(t, err)
	assert.Equal(t, []byte("payload"), received.Payload())
	assert.False(t, received.(*job.Message).AcknowledgementRequired())

	require.NoError(t, worker.Shutdown())
	assert.Equal(t, 1, channel.cancels)
	assert.Equal(t, 1, channel.closes)
	assert.Equal(t, 1, connection.closes)
	assert.ErrorIs(t, worker.Shutdown(), queue.ErrQueueShutdown)
	assert.ErrorIs(t, worker.Queue(&message), queue.ErrQueueShutdown)
}

func TestQueueReturnsPublishError(t *testing.T) {
	expected := errors.New("publish")
	worker := &Worker{
		channel: &fakeAMQPChannel{publishErr: expected},
		opts:    newOptions(),
	}
	message := job.NewMessage(rawMessage("payload"))

	assert.ErrorIs(t, worker.Queue(&message), expected)
}

func TestRequestReturnsSetupDecodeClosedAndTimeoutErrors(t *testing.T) {
	t.Run("setup", func(t *testing.T) {
		worker := &Worker{
			channel: &fakeAMQPChannel{queueErr: errors.New("declare")},
			opts:    newOptions(WithLogger(queue.NewEmptyLogger())),
		}
		message, err := worker.Request()
		assert.Nil(t, message)
		assert.Error(t, err)
	})

	t.Run("decode", func(t *testing.T) {
		deliveries := make(chan amqp.Delivery, 1)
		deliveries <- amqp.Delivery{Body: []byte("not-json")}
		worker := &Worker{tasks: deliveries, opts: newOptions()}
		worker.startOnce.Do(func() {})
		message, err := worker.Request()
		assert.Nil(t, message)
		assert.Error(t, err)
	})

	t.Run("closed", func(t *testing.T) {
		deliveries := make(chan amqp.Delivery)
		close(deliveries)
		worker := &Worker{tasks: deliveries, opts: newOptions()}
		worker.startOnce.Do(func() {})
		message, err := worker.Request()
		assert.Nil(t, message)
		assert.ErrorIs(t, err, queue.ErrQueueHasBeenClosed)
	})

	t.Run("timeout", func(t *testing.T) {
		worker := &Worker{
			tasks: make(chan amqp.Delivery),
			opts:  newOptions(WithRequestTimeout(time.Millisecond)),
		}
		worker.startOnce.Do(func() {})
		started := time.Now()
		message, err := worker.Request()
		assert.Nil(t, message)
		assert.ErrorIs(t, err, queue.ErrNoTaskInQueue)
		assert.Less(t, time.Since(started), 100*time.Millisecond)
	})
}

func TestShutdownReturnsFirstResourceError(t *testing.T) {
	for _, test := range []struct {
		name       string
		cancelErr  error
		channelErr error
		connectErr error
		expected   error
	}{
		{name: "cancel", cancelErr: errors.New("cancel"), expected: errors.New("cancel")},
		{name: "channel", channelErr: errors.New("channel"), expected: errors.New("channel")},
		{name: "connection", connectErr: errors.New("connection"), expected: errors.New("connection")},
	} {
		t.Run(test.name, func(t *testing.T) {
			connection := &fakeAMQPConnection{closeErr: test.connectErr}
			channel := &fakeAMQPChannel{cancelErr: test.cancelErr, closeErr: test.channelErr}
			worker := &Worker{
				conn: connection, channel: channel, stop: make(chan struct{}),
				opts: newOptions(WithLogger(queue.NewEmptyLogger())),
			}

			assert.EqualError(t, worker.Shutdown(), test.expected.Error())
		})
	}
}

func TestShutdownWithoutResources(t *testing.T) {
	worker := &Worker{stop: make(chan struct{}), opts: newOptions()}
	require.NoError(t, worker.Shutdown())
}

func withRabbitConnector(
	t *testing.T,
	connection amqpConnection,
	channel amqpChannel,
	err error,
) {
	t.Helper()
	original := connectRabbitMQ
	connectRabbitMQ = func(string, ReconnectConfig) (amqpConnection, amqpChannel, error) {
		if err != nil {
			return nil, nil, errConnect
		}
		return connection, channel, nil
	}
	t.Cleanup(func() { connectRabbitMQ = original })
}

type fakeAMQPConnection struct {
	closes     int
	closeErr   error
	rawChannel *amqp.Channel
	openErr    error
}

func (c *fakeAMQPConnection) Close() error {
	c.closes++
	return c.closeErr
}

func (c *fakeAMQPConnection) Channel() (*amqp.Channel, error) {
	return c.rawChannel, c.openErr
}

type fakeAMQPChannel struct {
	exchangeErr error
	queueErr    error
	bindErr     error
	consumeErr  error
	cancelErr   error
	closeErr    error
	publishErr  error
	deliveries  <-chan amqp.Delivery
	published   amqp.Publishing
	cancels     int
	closes      int
}

func (c *fakeAMQPChannel) ExchangeDeclare(string, string, bool, bool, bool, bool, amqp.Table) error {
	return c.exchangeErr
}

func (c *fakeAMQPChannel) QueueDeclare(string, bool, bool, bool, bool, amqp.Table) (amqp.Queue, error) {
	return amqp.Queue{Name: "jobs"}, c.queueErr
}

func (c *fakeAMQPChannel) QueueBind(string, string, string, bool, amqp.Table) error {
	return c.bindErr
}

func (c *fakeAMQPChannel) Consume(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error) {
	return c.deliveries, c.consumeErr
}

func (c *fakeAMQPChannel) Cancel(string, bool) error {
	c.cancels++
	return c.cancelErr
}

func (c *fakeAMQPChannel) Close() error {
	c.closes++
	return c.closeErr
}

func (c *fakeAMQPChannel) PublishWithContext(
	_ context.Context,
	_, _ string,
	_, _ bool,
	message amqp.Publishing,
) error {
	c.published = message
	return c.publishErr
}

type rawMessage string

func (m rawMessage) Bytes() []byte { return []byte(m) }
