//go:build integration

// Redis at REDIS_ADDR (default localhost:6379) must be running.
// Run: go test -tags=integration ./internal/queue/...

package queue_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"scheduler/internal/queue"
	"scheduler/internal/queueredis"
	"scheduler/internal/scheduler"
)

func redisIntegrationClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available at %s: %v", addr, err)
	}
	return c
}

// TestReassignmentTiming_AbandonedPendingWithoutLease exercises the Phase 2 story:
// pending + inactive lease => scheduler resubmits (roughly after lease TTL under these conditions).
// Uses manual StartLeaseLoop + cancel (not ExecuteOne) so the worker never acks the original delivery.
func TestReassignmentTiming_AbandonedPendingWithoutLease(t *testing.T) {
	ctx := context.Background()
	client := redisIntegrationClient(t)

	stream := fmt.Sprintf("test:reassign:%d", time.Now().UnixNano())
	group := "reassign-workers"

	q, err := queueredis.NewRedisStreamQueue(ctx, client, stream, group)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Del(ctx, stream).Err() })

	leaseInterval := 300 * time.Millisecond
	leaseTTL := 900 * time.Millisecond
	heartbeatInterval := 300 * time.Millisecond
	heartbeatTTL := time.Second

	leaseStore := queueredis.NewRedisLeaseStore(client)
	inspector := queueredis.NewRedisPendingInspector(client, stream, group)
	lookup := queueredis.NewRedisMessageLookup(client, stream, group)
	submitter := queue.NewSubmitter(q)
	reassigner := scheduler.NewReassigner(submitter, leaseStore, inspector, lookup)

	workerID := "worker-a"
	w := queue.NewWorker(q, workerID, 1, queueredis.NewRedisHeartbeatStore(client), heartbeatInterval, heartbeatTTL, leaseStore, leaseInterval, leaseTTL)

	jobID := "job-reassign-1"
	msg := queue.JobMessage{
		JobID:          jobID,
		Queue:          "q",
		Type:           "task",
		Payload:        []byte(`{}`),
		Priority:       1,
		ScheduledAt:    time.Now(),
		IdempotencyKey: "idem-1",
	}
	require.NoError(t, submitter.Submit(ctx, msg))

	claimed, err := w.Claim(ctx, 1, 2*time.Second)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.Equal(t, jobID, claimed[0].Job.JobID)

	jobCtx, cancelLease := context.WithCancel(ctx)
	w.StartLeaseLoop(jobCtx, claimed[0])

	time.Sleep(350 * time.Millisecond)
	cancelLease()

	mark := time.Now()

	streamDepth := func() int64 {
		n, err := client.XLen(ctx, stream).Result()
		require.NoError(t, err)
		return n
	}
	require.Equal(t, int64(1), streamDepth())

	deadline := time.Now().Add(5 * time.Second)
	var elapsed time.Duration
	for time.Now().Before(deadline) {
		require.NoError(t, reassigner.ScanAndReassign(ctx, 50))
		if streamDepth() >= 2 {
			elapsed = time.Since(mark)
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	require.True(t, streamDepth() >= 2, "expected a resubmit (second stream entry) after lease expiry")

	minElapsed := leaseTTL - 200*time.Millisecond
	maxElapsed := leaseTTL + 2*time.Second
	require.True(t, elapsed >= minElapsed, "reassignment fired too early (rough timing): elapsed=%v min=%v", elapsed, minElapsed)
	require.True(t, elapsed <= maxElapsed, "reassignment took too long under test conditions (cf. ~3s story when TTL=3s): elapsed=%v max=%v", elapsed, maxElapsed)

	require.True(t, streamDepth() >= 2)
}
