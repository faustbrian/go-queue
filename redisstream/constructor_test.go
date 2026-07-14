package redisdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewWorkerEReturnsInvalidConnectionString(t *testing.T) {
	worker, err := NewWorkerE(WithConnectionString(":"))

	require.Nil(t, worker)
	require.Error(t, err)
}

func TestNewWorkerERequiresRedisAddress(t *testing.T) {
	worker, err := NewWorkerE()

	require.Nil(t, worker)
	require.ErrorContains(t, err, "address")
}
