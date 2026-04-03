//go:build integration

// Requires Postgres (TEST_DATABASE_URL or DATABASE_URL) and Redis (REDIS_ADDR; prefer 127.0.0.1 on Windows).
// Run: go test -tags=integration ./internal/runtime/...

package runtime

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"scheduler/internal/integrationtest"
	"scheduler/internal/queue"
	"scheduler/internal/queueredis"
	"scheduler/internal/scheduler"
	"scheduler/internal/store"
)

const e2eWorkerID = "e2e-worker-1"

func testRedisAddr() string {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	return addr
}

func redisClient(t *testing.T) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: testRedisAddr()})
	ctx := context.Background()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func newE2EStack(t *testing.T, stream, group string) (*Coordinator, *queue.Worker, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	pool := integrationtest.Pool(t)
	redisC := redisClient(t)

	q, err := queueredis.NewRedisStreamQueue(ctx, redisC, stream, group)
	require.NoError(t, err)

	life := scheduler.NewJobLifecycle(pool, store.NewPostgresJobStore(), store.NewPostgresAuditStore())
	coord := NewCoordinator(q, life)

	lease := queueredis.NewRedisLeaseStore(redisC)
	worker := queue.NewWorker(q, e2eWorkerID, 1, nil, time.Second, time.Second, lease, 300*time.Millisecond, 900*time.Millisecond)

	t.Cleanup(func() { _ = redisC.Del(ctx, stream).Err() })

	return coord, worker, pool
}

func dispatchLeader(ctx context.Context, coord *Coordinator) error {
	now := time.Now().UTC()
	_, err := coord.Dispatch(ctx, CoordinatorDispatchOptions{
		LeaderAllowed: true,
		AsOf:          now,
		Limit:         50,
		Now:           now,
		AssignLease: func(job store.Job) (string, time.Time, error) {
			_ = job
			return e2eWorkerID, now.Add(10*time.Minute), nil
		},
	})
	return err
}

var errRetryOnce = errors.New("retry_once")

// noopQueue satisfies queue.Queue for tests that never touch Redis transport.
type noopQueue struct{}

func (noopQueue) Submit(context.Context, queue.JobMessage) error { return nil }

func (noopQueue) Claim(context.Context, string, int, time.Duration) ([]queue.ClaimedMessage, error) {
	return nil, nil
}

func (noopQueue) Ack(context.Context, string, ...queue.ClaimedMessage) error { return nil }

func TestE2E_CreateDispatchRunningSucceeded(t *testing.T) {
	ctx := context.Background()
	stream := "e2e:ok:" + randomSuffix()
	coord, w, pool := newE2EStack(t, stream, "e2e-g")

	jobID := "e2e-job-ok-" + randomSuffix()
	now := time.Now().UTC().Truncate(time.Millisecond)

	require.NoError(t, coord.CreateJob(ctx, store.InsertJobInput{
		JobID:       jobID,
		Queue:       "q",
		Type:        "task",
		Payload:     []byte(`{}`),
		Priority:    0,
		MaxAttempts: 3,
		ScheduledAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}, "producer"))

	require.NoError(t, dispatchLeader(ctx, coord))

	w.Hooks = coord.WorkerLifecycleHooks("worker", func(msg queue.ClaimedMessage, runErr error) (bool, time.Time, string, string) {
		_ = msg
		return true, time.Now().UTC().Add(time.Minute), runErr.Error(), "e"
	})

	claimed, err := w.Claim(ctx, 1, 3*time.Second)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.NoError(t, w.ExecuteOne(ctx, claimed[0], func(ctx context.Context, job queue.JobMessage) error {
		require.Equal(t, jobID, job.JobID)
		return nil
	}))

	j, err := store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.Equal(t, store.JobStatusSucceeded, j.Status)
}

func TestE2E_CreateDispatchRunningFailedRetryableThenSucceed(t *testing.T) {
	ctx := context.Background()
	stream := "e2e:retry:" + randomSuffix()
	coord, w, pool := newE2EStack(t, stream, "e2e-g")

	jobID := "e2e-job-retry-" + randomSuffix()
	now := time.Now().UTC().Truncate(time.Millisecond)

	require.NoError(t, coord.CreateJob(ctx, store.InsertJobInput{
		JobID:       jobID,
		Queue:       "q",
		Type:        "task",
		Payload:     []byte(`{}`),
		Priority:    0,
		MaxAttempts: 5,
		ScheduledAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}, "producer"))

	require.NoError(t, dispatchLeader(ctx, coord))

	w.Hooks = coord.WorkerLifecycleHooks("worker", func(msg queue.ClaimedMessage, runErr error) (bool, time.Time, string, string) {
		_ = msg
		if errors.Is(runErr, errRetryOnce) {
			return true, time.Now().UTC().Add(400 * time.Millisecond), runErr.Error(), "retry_once"
		}
		return false, time.Time{}, runErr.Error(), "fatal"
	})

	claimed, err := w.Claim(ctx, 1, 3*time.Second)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	err = w.ExecuteOne(ctx, claimed[0], func(ctx context.Context, job queue.JobMessage) error {
		return errRetryOnce
	})
	require.Error(t, err)

	j, err := store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.Equal(t, store.JobStatusFailed, j.Status)
	require.Equal(t, 1, j.AttemptsMade)

	time.Sleep(500 * time.Millisecond)
	require.NoError(t, dispatchLeader(ctx, coord))

	claimed, err = w.Claim(ctx, 1, 5*time.Second)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.NoError(t, w.ExecuteOne(ctx, claimed[0], func(ctx context.Context, job queue.JobMessage) error {
		return nil
	}))

	j, err = store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.Equal(t, store.JobStatusSucceeded, j.Status)
}

func TestE2E_CreateDispatchRunningDeadExhausted(t *testing.T) {
	ctx := context.Background()
	stream := "e2e:dead:" + randomSuffix()
	coord, w, pool := newE2EStack(t, stream, "e2e-g")

	jobID := "e2e-job-dead-" + randomSuffix()
	now := time.Now().UTC().Truncate(time.Millisecond)

	require.NoError(t, coord.CreateJob(ctx, store.InsertJobInput{
		JobID:       jobID,
		Queue:       "q",
		Type:        "task",
		Payload:     []byte(`{}`),
		Priority:    0,
		MaxAttempts: 1,
		ScheduledAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}, "producer"))

	require.NoError(t, dispatchLeader(ctx, coord))

	w.Hooks = coord.WorkerLifecycleHooks("worker", func(msg queue.ClaimedMessage, runErr error) (bool, time.Time, string, string) {
		_ = msg
		return true, time.Now().UTC().Add(time.Hour), runErr.Error(), "e"
	})

	claimed, err := w.Claim(ctx, 1, 3*time.Second)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	err = w.ExecuteOne(ctx, claimed[0], func(ctx context.Context, job queue.JobMessage) error {
		return errors.New("boom")
	})
	require.Error(t, err)

	j, err := store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.Equal(t, store.JobStatusDead, j.Status)
}

func TestE2E_NonLeaderDispatchRejected(t *testing.T) {
	ctx := context.Background()
	pool := integrationtest.Pool(t)
	life := scheduler.NewJobLifecycle(pool, store.NewPostgresJobStore(), store.NewPostgresAuditStore())
	coord := NewCoordinator(noopQueue{}, life)

	now := time.Now().UTC()
	_, err := coord.Dispatch(ctx, CoordinatorDispatchOptions{
		LeaderAllowed: false,
		AsOf:          now,
		Limit:         10,
		Now:           now,
		AssignLease: func(job store.Job) (string, time.Time, error) {
			return e2eWorkerID, now.Add(time.Minute), nil
		},
	})
	require.ErrorIs(t, err, scheduler.ErrNotSchedulingLeader)
}

func TestE2E_StaleFailureDoesNotOverwriteSuccess(t *testing.T) {
	ctx := context.Background()
	stream := "e2e:stale:" + randomSuffix()
	coord, w, pool := newE2EStack(t, stream, "e2e-g")

	jobID := "e2e-job-stale-" + randomSuffix()
	now := time.Now().UTC().Truncate(time.Millisecond)

	require.NoError(t, coord.CreateJob(ctx, store.InsertJobInput{
		JobID:       jobID,
		Queue:       "q",
		Type:        "task",
		Payload:     []byte(`{}`),
		Priority:    0,
		MaxAttempts: 3,
		ScheduledAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}, "producer"))

	require.NoError(t, dispatchLeader(ctx, coord))

	w.Hooks = coord.WorkerLifecycleHooks("worker", defaultRetryPolicyForTest)

	claimed, err := w.Claim(ctx, 1, 3*time.Second)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.NoError(t, w.ExecuteOne(ctx, claimed[0], func(ctx context.Context, job queue.JobMessage) error {
		return nil
	}))

	j, err := store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	ver := j.Version
	require.Equal(t, store.JobStatusSucceeded, j.Status)

	require.NoError(t, coord.Life.HandleWorkerFailed(ctx, scheduler.WorkerFailedParams{
		JobID:         jobID,
		WorkerID:      e2eWorkerID,
		Source:        "stale_worker",
		ErrorMessage:  "late",
		ErrorCode:     "late",
		Retryable:     true,
		NextRetryAt:   time.Now().UTC().Add(time.Hour),
		Occurred:      time.Now().UTC(),
	}))

	j2, err := store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.Equal(t, store.JobStatusSucceeded, j2.Status)
	require.Equal(t, ver, j2.Version)
}

func defaultRetryPolicyForTest(msg queue.ClaimedMessage, runErr error) (bool, time.Time, string, string) {
	_ = msg
	return true, time.Now().UTC().Add(time.Minute), runErr.Error(), "e"
}

func randomSuffix() string {
	return time.Now().Format("150405.000000000")
}
