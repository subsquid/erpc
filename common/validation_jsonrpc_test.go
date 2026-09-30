package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJsonRpcUpstreamConfig_Validate_MaxResponseBytes(t *testing.T) {
	t.Run("zero is valid", func(t *testing.T) {
		require.NoError(t, (&JsonRpcUpstreamConfig{}).Validate(&Config{}))
	})

	t.Run("positive is valid", func(t *testing.T) {
		require.NoError(t, (&JsonRpcUpstreamConfig{MaxResponseBytes: 1 << 20}).Validate(&Config{}))
	})

	t.Run("negative rejected", func(t *testing.T) {
		err := (&JsonRpcUpstreamConfig{MaxResponseBytes: -1}).Validate(&Config{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "maxResponseBytes")
	})
}
