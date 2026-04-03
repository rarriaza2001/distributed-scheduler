package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"scheduler/internal/queue"
	"scheduler/internal/queueredis"
)

func TestRedisStreamQueue_EndToEnd(t *testing.T) {
	ctx := context.Background()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available at %s: %v", addr, err)
	}

	q, err := queueredis.NewRedisStreamQueue(ctx, client, "test:jobs", "test:workers")
	require.NoError(t, err)

	submitter := queue.NewSubmitter(q)
	// heartbeatInterval, heartbeatTTL, leaseInterval, leaseTTL
	worker := queue.NewWorker(q, "worker-1", 1, nil, time.Second, time.Second, nil, time.Second, time.Second)

	msg := queue.JobMessage{
		JobID:          "job-123",
		Queue:          "emails",
		Type:           "send_email",
		Payload:        []byte(`{"to":"a@example.com"}`),
		Priority:       1,
		ScheduledAt:    time.Now(),
		IdempotencyKey: "idemp-1",
	}

	require.NoError(t, submitter.Submit(ctx, msg))

	claimed, err := worker.Claim(ctx, 1, time.Second)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	assert.Equal(t, "test:jobs", claimed[0].Stream)
	assert.Equal(t, "worker-1", claimed[0].ConsumerName)

	err = worker.ExecuteOne(ctx, claimed[0], func(jobCtx context.Context, job queue.JobMessage) error {
		assert.Equal(t, "job-123", job.JobID)
		assert.Equal(t, "send_email", job.Type)
		assert.Equal(t, `{"to":"a@example.com"}`, string(job.Payload))
		return nil
	})
	require.NoError(t, err)
}
