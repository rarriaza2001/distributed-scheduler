package app

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadConfigFromEnv_Defaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://x")
	c := LoadConfigFromEnv()
	require.Equal(t, "127.0.0.1:6379", c.RedisAddr)
	require.Equal(t, ":8080", c.HTTPAddr)
	require.Equal(t, 10*time.Second, c.QueueAlivePollInterval)
	require.Equal(t, 45*time.Second, c.WorkersAliveStale)
	require.Equal(t, 300, c.FailedRecentMax)
	require.Equal(t, 15*time.Second, c.HTTPShutdownTimeout)
	require.Equal(t, 5, c.LeaderLeaseTTLSeconds)
}

func TestServiceConfig_Validate(t *testing.T) {
	c := ServiceConfig{DatabaseURL: "x", RedisAddr: "y", HTTPAddr: ":1",
		QueueAlivePollInterval: time.Second, WorkersAliveStale: time.Second,
		FailedRecentMax: 100, HTTPShutdownTimeout: time.Second, LeaderLeaseTTLSeconds: 5}
	require.NoError(t, c.Validate())

	c.DatabaseURL = ""
	require.Error(t, c.Validate())

	c = ServiceConfig{DatabaseURL: "x", RedisAddr: "y", HTTPAddr: ":1",
		QueueAlivePollInterval: 0, WorkersAliveStale: time.Second,
		FailedRecentMax: 100, HTTPShutdownTimeout: time.Second, LeaderLeaseTTLSeconds: 5}
	require.Error(t, c.Validate())
}
