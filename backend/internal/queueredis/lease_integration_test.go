//go:build integration

package queueredis

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"scheduler/internal/queue"
)

func testRedisAddr() string {
	a := os.Getenv("REDIS_ADDR")
	if a == "" {
		a = "127.0.0.1:6379"
	}
	return a
}

func TestRedisLeaseStore_OwnershipSafe(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: testRedisAddr()})
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("redis: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	jobID := "lease-test-job-1"
	s1 := NewRedisLeaseStore(c)
	s2 := NewRedisLeaseStore(c)

	_ = c.Del(ctx, "job:"+jobID+":lease").Err()

	ttl := time.Second * 2
	require.NoError(t, s1.Acquire(ctx, jobID, "w1", ttl))

	err := s2.Acquire(ctx, jobID, "w2", ttl)
	require.ErrorIs(t, err, ErrLeaseHeldByOther)

	require.NoError(t, s1.Renew(ctx, jobID, "w1", ttl))
	err = s2.Renew(ctx, jobID, "w2", ttl)
	require.ErrorIs(t, err, ErrLeaseHeldByOther)

	require.NoError(t, s1.Release(ctx, jobID, "w1"))
	err = s2.Release(ctx, jobID, "w2")
	require.NoError(t, err)

	_ = c.Del(ctx, "job:"+jobID+":lease").Err()
}

func TestRedisLeaseStore_StaleWorkerCannotDeleteOtherLease(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: testRedisAddr()})
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("redis: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	jobID := "lease-test-job-2"
	s := NewRedisLeaseStore(c)
	_ = c.Del(ctx, "job:"+jobID+":lease").Err()

	require.NoError(t, s.Acquire(ctx, jobID, "owner", time.Second*5))
	require.NoError(t, s.Release(ctx, jobID, "intruder"))

	active, err := s.IsActive(ctx, jobID)
	require.NoError(t, err)
	require.True(t, active, "wrong worker must not delete lease")

	var ls queue.LeaseStore = s
	_ = ls
}
