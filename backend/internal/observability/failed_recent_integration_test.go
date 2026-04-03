//go:build integration

package observability

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func redisAddr() string {
	a := os.Getenv("REDIS_ADDR")
	if a == "" {
		a = "127.0.0.1:6379"
	}
	return a
}

func TestFailedRecentStore_BoundedRetention(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: redisAddr()})
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("redis: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	_ = c.Del(ctx, FailedRecentKey).Err()

	const max = 20
	s := NewFailedRecentStore(c, max)
	for i := 0; i < 50; i++ {
		require.NoError(t, s.Push(ctx, FailedJobMeta{
			JobID:    "j" + strconv.Itoa(i),
			JobType:  "t",
			Queue:    "q",
			Terminal: true,
			FailedAt: time.Now().UTC(),
		}))
	}

	n, err := c.LLen(ctx, FailedRecentKey).Result()
	require.NoError(t, err)
	require.LessOrEqual(t, n, int64(max))
}
