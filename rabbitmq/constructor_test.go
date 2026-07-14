package rabbitmq

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewWorkerEReturnsConnectionError(t *testing.T) {
	worker, err := NewWorkerE(
		WithAddr(":"),
		WithReconnectConfig(ReconnectConfig{MaxRetries: 1}),
	)

	require.Nil(t, worker)
	require.Error(t, err)
}
