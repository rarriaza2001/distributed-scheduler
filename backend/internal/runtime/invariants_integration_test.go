//go:build integration

// End-to-end invariant checks: DB commits lease while Redis transport step may fail (no cross-store atomicity).
package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"scheduler/internal/integrationtest"
	"scheduler/internal/queueredis"
	"scheduler/internal/scheduler"
	"scheduler/internal/store"
)

var errRedisSubmitFailed = errors.New("redis_submit_injected")

func newStackWithRedis(t *testing.T, stream, group string) (*Coordinator, *pgxpool.Pool, *redis.Client) {
	t.Helper()
	ctx := context.Background()
	pool := integrationtest.Pool(t)
	redisC := redisClient(t)
	q, err := queueredis.NewRedisStreamQueue(ctx, redisC, stream, group)
	require.NoError(t, err)
	life := scheduler.NewJobLifecycle(pool, store.NewPostgresJobStore(), store.NewPostgresAuditStore())
	coord := NewCoordinator(q, life)
	t.Cleanup(func() { _ = redisC.Del(ctx, stream).Err() })
	return coord, pool, redisC
}

func TestInvariant_DBLeaseCommitsWhenRedisSubmitFails_streamEmpty(t *testing.T) {
	ctx := context.Background()
	stream := "inv:xadd:" + randomSuffix()
	coord, pool, rdb := newStackWithRedis(t, stream, "inv-g")

	coord.AfterLeaseCommittedOverride = func(context.Context, store.Job, string, time.Time) error {
		return errRedisSubmitFailed
	}

	jobID := "inv-xadd-" + randomSuffix()
	now := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, coord.CreateJob(ctx, store.InsertJobInput{
		JobID: jobID, Queue: "q", Type: "t", Payload: []byte(`{}`),
		MaxAttempts: 3, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
	}, "test"))

	n, err := coord.Dispatch(ctx, CoordinatorDispatchOptions{
		LeaderAllowed: true,
		AsOf:          now.Add(time.Second),
		Limit:         10,
		Now:           now.Add(time.Second),
		AssignLease: func(job store.Job) (string, time.Time, error) {
			_ = job
			return e2eWorkerID, now.Add(10 * time.Minute), nil
		},
	})
	require.ErrorIs(t, err, errRedisSubmitFailed)
	require.Equal(t, 0, n)

	j, err := store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.Equal(t, store.JobStatusLeased, j.Status)
	require.NotNil(t, j.LeaseOwnerWorkerID)
	require.Equal(t, e2eWorkerID, *j.LeaseOwnerWorkerID)

	xlen, err := rdb.XLen(ctx, stream).Result()
	require.NoError(t, err)
	require.Zero(t, xlen)
}

func TestInvariant_ReconcileAfterRedisSubmitFailure_keepsLeasedTruth(t *testing.T) {
	ctx := context.Background()
	stream := "inv:rec:" + randomSuffix()
	coord, pool, _ := newStackWithRedis(t, stream, "inv-g2")
	coord.AfterLeaseCommittedOverride = func(context.Context, store.Job, string, time.Time) error {
		return errRedisSubmitFailed
	}
	rec := scheduler.NewReconciler(pool, store.NewPostgresJobStore(), store.NewPostgresAuditStore())

	jobID := "inv-rec-" + randomSuffix()
	now := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, coord.CreateJob(ctx, store.InsertJobInput{
		JobID: jobID, Queue: "q", Type: "t", Payload: []byte(`{}`),
		MaxAttempts: 3, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
	}, "test"))

	_, err := coord.Dispatch(ctx, CoordinatorDispatchOptions{
		LeaderAllowed: true,
		AsOf:          now.Add(time.Second),
		Limit:         10,
		Now:           now.Add(time.Second),
		AssignLease: func(job store.Job) (string, time.Time, error) {
			return e2eWorkerID, now.Add(30 * time.Minute), nil
		},
	})
	require.ErrorIs(t, err, errRedisSubmitFailed)

	require.NoError(t, rec.Reconcile(ctx, scheduler.ReconcileOptions{
		LeaderAllowed: true,
		Now:           now.Add(time.Minute),
		Limits:        scheduler.ReconcileLimits{LeasedExpired: 10, StaleRedisScan: 5},
	}))

	j, err := store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.Equal(t, store.JobStatusLeased, j.Status)
}

func TestInvariant_FollowerCoordinatorDoesNotDispatchToLeased(t *testing.T) {
	ctx := context.Background()
	stream := "inv:fol:" + randomSuffix()
	coord, pool, _ := newStackWithRedis(t, stream, "inv-g3")

	jobID := "inv-fol-" + randomSuffix()
	now := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, coord.CreateJob(ctx, store.InsertJobInput{
		JobID: jobID, Queue: "q", Type: "t", Payload: []byte(`{}`),
		MaxAttempts: 3, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
	}, "test"))

	_, err := coord.Dispatch(ctx, CoordinatorDispatchOptions{
		LeaderAllowed: false,
		AsOf:          now.Add(time.Second),
		Limit:         10,
		Now:           now.Add(time.Second),
		AssignLease: func(job store.Job) (string, time.Time, error) {
			return e2eWorkerID, now.Add(10 * time.Minute), nil
		},
	})
	require.ErrorIs(t, err, scheduler.ErrNotSchedulingLeader)

	j, err := store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.Equal(t, store.JobStatusQueued, j.Status)
}

func TestInvariant_FollowerReconcilerRejected(t *testing.T) {
	ctx := context.Background()
	pool := integrationtest.Pool(t)
	rec := scheduler.NewReconciler(pool, store.NewPostgresJobStore(), store.NewPostgresAuditStore())
	err := rec.Reconcile(ctx, scheduler.ReconcileOptions{LeaderAllowed: false})
	require.ErrorIs(t, err, scheduler.ErrNotSchedulingLeader)
}

func TestInvariant_StaleDeadReportIgnored(t *testing.T) {
	ctx := context.Background()
	pool := integrationtest.Pool(t)
	life := scheduler.NewJobLifecycle(pool, store.NewPostgresJobStore(), store.NewPostgresAuditStore())

	jobID := "inv-dead-" + randomSuffix()
	now := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, life.CreateJobAndAudit(ctx, scheduler.CreateJobParams{
		Job: store.InsertJobInput{
			JobID: jobID, Queue: "q", Type: "t", Payload: []byte(`{}`),
			MaxAttempts: 1, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
		},
		Source: "test",
	}))
	_, err := life.DispatchEligibleJobs(ctx, scheduler.DispatchOptions{
		LeaderAllowed: true,
		AsOf:          now.Add(time.Second), Limit: 10,
		AssignLease: func(job store.Job) (string, time.Time, error) {
			return "w1", now.Add(10 * time.Minute), nil
		},
		Now: now.Add(time.Second),
	})
	require.NoError(t, err)
	require.NoError(t, life.HandleWorkerStarted(ctx, scheduler.WorkerStartedParams{
		JobID: jobID, WorkerID: "w1", Source: "worker", Occurred: now.Add(2 * time.Second),
	}))
	require.NoError(t, life.HandleWorkerFailed(ctx, scheduler.WorkerFailedParams{
		JobID: jobID, WorkerID: "w1", Source: "worker",
		ErrorMessage: "fatal", ErrorCode: "f", Retryable: false,
		Occurred: now.Add(3 * time.Second),
	}))

	j, err := store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.Equal(t, store.JobStatusDead, j.Status)
	v := j.Version

	require.NoError(t, life.HandleWorkerFailed(ctx, scheduler.WorkerFailedParams{
		JobID: jobID, WorkerID: "w1", Source: "worker",
		ErrorMessage: "late", ErrorCode: "late", Retryable: true,
		NextRetryAt: now.Add(time.Hour), Occurred: now.Add(4 * time.Second),
	}))
	j, err = store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.Equal(t, store.JobStatusDead, j.Status)
	require.Equal(t, v, j.Version)
}
