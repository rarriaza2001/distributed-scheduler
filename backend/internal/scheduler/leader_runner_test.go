package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"scheduler/internal/queueredis"
	"scheduler/internal/queue"
)

// TestLeaderStartupRace_OneActiveLeader verifies that in a healthy case only
// one SchedulerLeader instance performs control-loop work at a time.
func TestLeaderStartupRace_OneActiveLeader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available: %v", err)
	}

	_ = client.Del(ctx, queueredis.LeaderLeaseKey).Err()

	lease := queueredis.NewRedisLeaderLease(client)

	cfg := SchedulerLeaderConfig{
		LeaseTTLSeconds:  5,
		RenewInterval:    500 * time.Millisecond,
		AcquireInterval:  100 * time.Millisecond,
		ScanInterval:     100 * time.Millisecond,
		ScanLimitPerTick: 10,
	}

	var scans1, scans2 int32

	r1 := &Reassigner{
		submitter:        nil,
		leaseStore:       nil,
		pendingInspector: &countingInspector{counter: &scans1},
		messageLookup:    nil,
	}
	r2 := &Reassigner{
		submitter:        nil,
		leaseStore:       nil,
		pendingInspector: &countingInspector{counter: &scans2},
		messageLookup:    nil,
	}

	l1 := NewSchedulerLeader("s1", lease, r1, cfg)
	l2 := NewSchedulerLeader("s2", lease, r2, cfg)

	go l1.Start(ctx)
	go l2.Start(ctx)

	time.Sleep(2 * time.Second)

	total1 := atomic.LoadInt32(&scans1)
	total2 := atomic.LoadInt32(&scans2)

	require.True(t, (total1 == 0) != (total2 == 0), "expected exactly one leader to perform scans, got s1=%d s2=%d", total1, total2)
}

// countingInspector increments a counter whenever ListPending is called.
type countingInspector struct {
	counter *int32
}

func (c *countingInspector) ListPending(ctx context.Context, count int) ([]queue.PendingMessage, error) {
	_ = ctx
	_ = count
	if c.counter != nil {
		atomic.AddInt32(c.counter, 1)
	}
	return nil, nil
}

// TestLeaderFailover_TakeoverAfterTTL verifies that when the active leader
// stops renewing, another follower can acquire leadership after TTL expiry.
func TestLeaderFailover_TakeoverAfterTTL(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available: %v", err)
	}

	_ = client.Del(ctx, queueredis.LeaderLeaseKey).Err()

	lease := queueredis.NewRedisLeaderLease(client)
	cfg := SchedulerLeaderConfig{
		LeaseTTLSeconds:  2,
		RenewInterval:    500 * time.Millisecond,
		AcquireInterval:  200 * time.Millisecond,
		ScanInterval:     100 * time.Millisecond,
		ScanLimitPerTick: 10,
	}

	var scans1, scans2 int32
	l1 := NewSchedulerLeader("s1", lease, &Reassigner{pendingInspector: &countingInspector{counter: &scans1}}, cfg)
	l2 := NewSchedulerLeader("s2", lease, &Reassigner{pendingInspector: &countingInspector{counter: &scans2}}, cfg)

	leaderCtx, leaderCancel := context.WithCancel(ctx)
	defer leaderCancel()

	go l1.Start(leaderCtx)
	go l2.Start(ctx)

	time.Sleep(1500 * time.Millisecond)

	first1 := atomic.LoadInt32(&scans1)
	first2 := atomic.LoadInt32(&scans2)
	require.True(t, (first1 == 0) != (first2 == 0), "expected exactly one leader initially, got s1=%d s2=%d", first1, first2)

	leaderCancel()

	time.Sleep(3 * time.Second)

	after1 := atomic.LoadInt32(&scans1)
	after2 := atomic.LoadInt32(&scans2)

	require.True(t, after1 > first1 || after2 > first2, "expected some scans after failover, got s1=%d->%d s2=%d->%d", first1, after1, first2, after2)
}

// TestRedisLeaderLease_RenewOwnershipSafe ensures a stale scheduler cannot
// renew someone else's lease blindly.
func TestRedisLeaderLease_RenewOwnershipSafe(t *testing.T) {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available: %v", err)
	}

	_ = client.Del(ctx, queueredis.LeaderLeaseKey).Err()

	lease := queueredis.NewRedisLeaderLease(client)

	ok, err := lease.Acquire(ctx, "leader-A", 2)
	require.NoError(t, err)
	require.True(t, ok)

	err = client.Set(ctx, queueredis.LeaderLeaseKey, "leader-B", 2*time.Second).Err()
	require.NoError(t, err)

	ok, err = lease.Renew(ctx, "leader-A", 2)
	require.NoError(t, err)
	require.False(t, ok, "stale leader should not be able to renew someone else's lease")
}
