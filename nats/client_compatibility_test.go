package nats

import (
	"net"
	"testing"
	"time"

	queue "github.com/faustbrian/go-queue"
	"github.com/faustbrian/go-queue/job"
	server "github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func TestWorkerReconnectPreservesPayload(t *testing.T) {
	for _, failover := range []bool{false, true} {
		name := "same endpoint restart"
		if failover {
			name = "available alternate broker"
		}
		t.Run(name, func(t *testing.T) {
			first := startCompatibilityBroker(t, -1)
			addresses := []string{first.ClientURL()}
			var second *server.Server
			if failover {
				second = startCompatibilityBroker(t, -1)
				addresses = append(addresses, second.ClientURL())
			}
			worker, err := NewWorkerE(
				WithAddr(addresses...), WithSubj("compatibility"),
				WithQueue("compatibility"), WithRequestTimeout(time.Second),
			)
			require.NoError(t, err)
			t.Cleanup(func() { _ = worker.Shutdown() })
			require.NoError(t, worker.client.Flush())
			before := job.NewMessage(rawMessage("before reconnect"))
			require.NoError(t, worker.Queue(&before))
			require.NoError(t, worker.client.Flush())
			received, err := worker.Request()
			require.NoError(t, err)
			require.Equal(t, []byte("before reconnect"), received.Payload())

			active := first
			var expectedURL string
			if failover {
				switch worker.client.ConnectedUrl() {
				case first.ClientURL():
					expectedURL = second.ClientURL()
				case second.ClientURL():
					active, expectedURL = second, first.ClientURL()
				default:
					t.Fatal("worker connected to an unexpected broker")
				}
			}
			port := active.Addr().(*net.TCPAddr).Port
			active.Shutdown()
			active.WaitForShutdown()
			if !failover {
				require.Eventually(t, func() bool {
					return worker.client.Status() == natsgo.RECONNECTING
				}, 5*time.Second, time.Millisecond)
				expectedURL = startCompatibilityBroker(t, port).ClientURL()
			}
			require.Eventually(t, func() bool {
				return worker.client.Status() == natsgo.CONNECTED &&
					worker.client.ConnectedUrl() == expectedURL
			}, 10*time.Second, 10*time.Millisecond)
			require.NoError(t, worker.client.Flush())
			after := job.NewMessage(rawMessage("after reconnect"))
			require.NoError(t, worker.Queue(&after))
			require.NoError(t, worker.client.Flush())
			received, err = worker.Request()
			require.NoError(t, err)
			require.Equal(t, []byte("after reconnect"), received.Payload())
			require.NoError(t, shutdownWithin(t, worker))
			require.True(t, worker.client.IsClosed())
			require.ErrorIs(t, worker.Shutdown(), queue.ErrQueueShutdown)
		})
	}
}

func TestShutdownRepublishesPendingPayload(t *testing.T) {
	broker := startCompatibilityBroker(t, -1)
	observer, err := natsgo.Connect(broker.ClientURL())
	require.NoError(t, err)
	t.Cleanup(observer.Close)
	subscription, err := observer.SubscribeSync("shutdown-compatibility")
	require.NoError(t, err)
	require.NoError(t, observer.Flush())
	worker, err := NewWorkerE(
		WithAddr(broker.ClientURL()), WithSubj("shutdown-compatibility"),
		WithQueue("shutdown-compatibility"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = worker.Shutdown() })
	require.NoError(t, worker.client.Flush())
	message := job.NewMessage(rawMessage("pending original payload"))
	require.NoError(t, worker.Queue(&message))
	require.NoError(t, worker.client.Flush())
	original, err := subscription.NextMsg(time.Second)
	require.NoError(t, err)
	require.Equal(t, message.Bytes(), original.Data)
	require.Eventually(t, func() bool {
		delivered, err := worker.subscription.Delivered()
		return err == nil && delivered == 1
	}, time.Second, time.Millisecond)

	// No Request consumes the callback, so shutdown releases it to the
	// connected backend's best-effort republication path.
	require.NoError(t, shutdownWithin(t, worker))
	republished, err := subscription.NextMsg(time.Second)
	require.NoError(t, err)
	require.Equal(t, message.Bytes(), republished.Data)
	require.Empty(t, republished.Reply)
	require.True(t, worker.client.IsClosed())
	_, err = worker.Request()
	require.ErrorIs(t, err, queue.ErrQueueHasBeenClosed)
}

func startCompatibilityBroker(t *testing.T, port int) *server.Server {
	t.Helper()
	instance, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: port, NoLog: true, NoSigs: true,
	})
	require.NoError(t, err)
	go instance.Start()
	t.Cleanup(func() {
		instance.Shutdown()
		instance.WaitForShutdown()
	})
	require.True(t, instance.ReadyForConnections(5*time.Second))
	return instance
}
