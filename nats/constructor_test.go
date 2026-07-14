package nats

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewWorkerEReturnsConnectionError(t *testing.T) {
	worker, err := NewWorkerE(WithAddr(":"))

	require.Nil(t, worker)
	require.Error(t, err)
}
