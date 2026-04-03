package queueredis

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDecodeJobMessage_CreatedAtOptional(t *testing.T) {
	values := map[string]any{
		"job_id":          "j1",
		"queue":           "q",
		"type":            "t",
		"payload":         "{}",
		"priority":        "0",
		"scheduled_at":    "1000",
		"idempotency_key": "",
	}
	j, err := decodeJobMessage(values)
	require.NoError(t, err)
	require.True(t, j.CreatedAt.IsZero())

	values["created_at"] = "500"
	j, err = decodeJobMessage(values)
	require.NoError(t, err)
	require.Equal(t, time.UnixMilli(500), j.CreatedAt)
}
