//go:build integration

package scheduler

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"scheduler/internal/integrationtest"
	"scheduler/internal/queueredis"
	"scheduler/internal/store"
)

func retryKey(t *testing.T) string {
	t.Helper()
	return "scheduler:retry:test:" + time.Now().Format("150405.000000000")
}

func TestRecovery_NonLeaderRejected(t *testing.T) {
	ctx := context.Background()
	pool := integrationtest.Pool(t)
	rec := NewReconciler(pool, store.NewPostgresJobStore(), store.NewPostgresAuditStore())
	err := rec.Reconcile(ctx, ReconcileOptions{LeaderAllowed: false})
	require.ErrorIs(t, err, ErrNotSchedulingLeader)
}

func TestRecovery_RestoresRetryCoordWhenMissing(t *testing.T) {
	ctx := context.Background()
	pool := integrationtest.Pool(t)
	rdb := testRecoveryRedis(t)
	key := retryKey(t)
	t.Cleanup(func() { _ = rdb.Del(ctx, key).Err() })

	life := NewJobLifecycle(pool, store.NewPostgresJobStore(), store.NewPostgresAuditStore())
	rec := NewReconciler(pool, store.NewPostgresJobStore(), store.NewPostgresAuditStore())
	coord := queueredis.NewRetrySchedule(rdb, key)

	jobID := "rec-retry-" + time.Now().Format("150405.000000000")
	now := time.Now().UTC().Truncate(time.Millisecond)
	next := now.Add(time.Hour)

	require.NoError(t, life.CreateJobAndAudit(ctx, CreateJobParams{
		Job: store.InsertJobInput{
			JobID: jobID, Queue: "q", Type: "t", Payload: []byte(`{}`),
			MaxAttempts: 3, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
		},
		Source: "test",
	}))

	j, err := store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.NoError(t, store.NewPostgresJobStore().MarkLeased(ctx, pool, jobID, j.Version, "w", now.Add(time.Minute), now))
	j, err = store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.NoError(t, store.NewPostgresJobStore().MarkRunning(ctx, pool, jobID, j.Version, now))
	j, err = store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)

	retryable := true
	require.NoError(t, store.NewPostgresJobStore().MarkFailedAndScheduleRetry(ctx, pool, jobID, j.Version,
		j.AttemptsMade+1, strPtr("e"), strPtr("code"), &retryable, now, next, now))

	ok, err := coord.IsScheduled(ctx, jobID)
	require.NoError(t, err)
	require.False(t, ok)

	require.NoError(t, rec.Reconcile(ctx, ReconcileOptions{
		LeaderAllowed: true,
		Now:           now.Add(time.Minute),
		RetryCoord:    coord,
		Limits:        ReconcileLimits{FailedRetry: 10, StaleRedisScan: 50},
		Source:        "test_recovery",
	}))

	ok, err = coord.IsScheduled(ctx, jobID)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestRecovery_RemovesStaleRetryWhenTerminal(t *testing.T) {
	ctx := context.Background()
	pool := integrationtest.Pool(t)
	rdb := testRecoveryRedis(t)
	key := retryKey(t)
	t.Cleanup(func() { _ = rdb.Del(ctx, key).Err() })

	life := NewJobLifecycle(pool, store.NewPostgresJobStore(), store.NewPostgresAuditStore())
	rec := NewReconciler(pool, store.NewPostgresJobStore(), store.NewPostgresAuditStore())
	coord := queueredis.NewRetrySchedule(rdb, key)

	jobID := "rec-stale-" + time.Now().Format("150405.000000000")
	now := time.Now().UTC().Truncate(time.Millisecond)

	require.NoError(t, life.CreateJobAndAudit(ctx, CreateJobParams{
		Job: store.InsertJobInput{
			JobID: jobID, Queue: "q", Type: "t", Payload: []byte(`{}`),
			MaxAttempts: 3, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
		},
		Source: "test",
	}))
	require.NoError(t, coord.ScheduleRetry(ctx, jobID, now.Add(time.Hour)))

	j, err := store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.NoError(t, store.NewPostgresJobStore().MarkLeased(ctx, pool, jobID, j.Version, "w", now.Add(time.Minute), now))
	j, err = store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.NoError(t, store.NewPostgresJobStore().MarkRunning(ctx, pool, jobID, j.Version, now))
	j, err = store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.NoError(t, store.NewPostgresJobStore().MarkSucceeded(ctx, pool, jobID, j.Version, now))

	require.NoError(t, rec.Reconcile(ctx, ReconcileOptions{
		LeaderAllowed: true,
		Now:           now.Add(time.Minute),
		RetryCoord:    coord,
		Limits:        ReconcileLimits{StaleRedisScan: 50},
	}))

	ok, err := coord.IsScheduled(ctx, jobID)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestRecovery_LeasedExpiredToQueued(t *testing.T) {
	ctx := context.Background()
	pool := integrationtest.Pool(t)
	rec := NewReconciler(pool, store.NewPostgresJobStore(), store.NewPostgresAuditStore())
	life := NewJobLifecycle(pool, store.NewPostgresJobStore(), store.NewPostgresAuditStore())

	jobID := "rec-lease-" + time.Now().Format("150405.000000000")
	now := time.Now().UTC().Truncate(time.Millisecond)
	past := now.Add(-time.Hour)

	require.NoError(t, life.CreateJobAndAudit(ctx, CreateJobParams{
		Job: store.InsertJobInput{
			JobID: jobID, Queue: "q", Type: "t", Payload: []byte(`{}`),
			MaxAttempts: 3, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
		},
		Source: "test",
	}))
	j, err := store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.NoError(t, store.NewPostgresJobStore().MarkLeased(ctx, pool, jobID, j.Version, "w", past, past))

	require.NoError(t, rec.Reconcile(ctx, ReconcileOptions{
		LeaderAllowed: true,
		Now:           now,
		Limits:        ReconcileLimits{LeasedExpired: 10},
	}))

	j, err = store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.Equal(t, store.JobStatusQueued, j.Status)
	require.Nil(t, j.LeaseExpiresAt)
}

func TestRecovery_RunningAbandonedWithoutRedisLease(t *testing.T) {
	ctx := context.Background()
	pool := integrationtest.Pool(t)
	rdb := testRecoveryRedis(t)
	lease := queueredis.NewRedisLeaseStore(rdb)
	rec := NewReconciler(pool, store.NewPostgresJobStore(), store.NewPostgresAuditStore())
	life := NewJobLifecycle(pool, store.NewPostgresJobStore(), store.NewPostgresAuditStore())

	jobID := "rec-run-" + time.Now().Format("150405.000000000")
	now := time.Now().UTC().Truncate(time.Millisecond)

	require.NoError(t, life.CreateJobAndAudit(ctx, CreateJobParams{
		Job: store.InsertJobInput{
			JobID: jobID, Queue: "q", Type: "t", Payload: []byte(`{}`),
			MaxAttempts: 3, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
		},
		Source: "test",
	}))
	j, err := store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.NoError(t, store.NewPostgresJobStore().MarkLeased(ctx, pool, jobID, j.Version, "w", now.Add(time.Minute), now))
	j, err = store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.NoError(t, store.NewPostgresJobStore().MarkRunning(ctx, pool, jobID, j.Version, now))

	require.NoError(t, rec.Reconcile(ctx, ReconcileOptions{
		LeaderAllowed: true,
		Now:           now.Add(time.Minute),
		JobLease:      LeaseProbeFunc(lease.IsActive),
		Limits:        ReconcileLimits{RunningProbe: 10},
	}))

	j, err = store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.Equal(t, store.JobStatusQueued, j.Status)
}

func testRecoveryRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func strPtr(s string) *string { return &s }
