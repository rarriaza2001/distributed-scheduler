//go:build integration

// Postgres at TEST_DATABASE_URL (or DATABASE_URL) must be running.
// Run: go test -tags=integration ./internal/scheduler/...

package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"scheduler/internal/integrationtest"
	"scheduler/internal/store"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return integrationtest.Pool(t)
}

func newTestLifecycle(t *testing.T, pool *pgxpool.Pool) *JobLifecycle {
	t.Helper()
	return NewJobLifecycle(pool, store.NewPostgresJobStore(), store.NewPostgresAuditStore())
}

func TestIntegration_DispatchRequiresLeader(t *testing.T) {
	pool := testPool(t)
	svc := newTestLifecycle(t, pool)
	ctx := context.Background()

	_, err := svc.DispatchEligibleJobs(ctx, DispatchOptions{
		LeaderAllowed: false,
		AsOf:          time.Now().UTC(),
		Limit:         10,
		AssignLease: func(job store.Job) (string, time.Time, error) {
			return "w1", time.Now().UTC().Add(time.Minute), nil
		},
		Now: time.Now().UTC(),
	})
	require.ErrorIs(t, err, ErrNotSchedulingLeader)
}

func TestIntegration_HappyPath_CreateDispatchStartSucceed(t *testing.T) {
	pool := testPool(t)
	svc := newTestLifecycle(t, pool)
	ctx := context.Background()

	jobID := "job-happy-" + time.Now().Format("150405.000000000")
	now := time.Now().UTC().Truncate(time.Millisecond)

	err := svc.CreateJobAndAudit(ctx, CreateJobParams{
		Job: store.InsertJobInput{
			JobID:       jobID,
			Queue:       "q1",
			Type:        "t1",
			Payload:     []byte(`{"x":1}`),
			Priority:    1,
			MaxAttempts: 3,
			ScheduledAt: now,
			CreatedAt:   now,
			UpdatedAt:   now,
		},
		Source: "producer",
	})
	require.NoError(t, err)

	n, err := svc.DispatchEligibleJobs(ctx, DispatchOptions{
		LeaderAllowed: true,
		AsOf:          now.Add(time.Second),
		Limit:         10,
		AssignLease: func(job store.Job) (string, time.Time, error) {
			return "worker-1", now.Add(5 * time.Minute), nil
		},
		Now: now.Add(time.Second),
	})
	require.NoError(t, err)
	require.Equal(t, 1, n)

	j, err := store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.Equal(t, store.JobStatusLeased, j.Status)
	require.NotNil(t, j.LeaseOwnerWorkerID)
	require.Equal(t, "worker-1", *j.LeaseOwnerWorkerID)

	require.NoError(t, svc.HandleWorkerStarted(ctx, WorkerStartedParams{
		JobID:    jobID,
		WorkerID: "worker-1",
		Source:   "worker",
		Occurred: now.Add(2 * time.Second),
	}))

	j, err = store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.Equal(t, store.JobStatusRunning, j.Status)

	require.NoError(t, svc.HandleWorkerSucceeded(ctx, WorkerSucceededParams{
		JobID:    jobID,
		WorkerID: "worker-1",
		Source:   "worker",
		Occurred: now.Add(3 * time.Second),
	}))

	j, err = store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.Equal(t, store.JobStatusSucceeded, j.Status)
	require.Nil(t, j.LeaseOwnerWorkerID)
}

func TestIntegration_DuplicateWorkerStartedNoExtraMutation(t *testing.T) {
	pool := testPool(t)
	svc := newTestLifecycle(t, pool)
	ctx := context.Background()

	jobID := "job-dupstart-" + time.Now().Format("150405.000000000")
	now := time.Now().UTC().Truncate(time.Millisecond)

	require.NoError(t, svc.CreateJobAndAudit(ctx, CreateJobParams{
		Job: store.InsertJobInput{
			JobID:       jobID,
			Queue:       "q1",
			Type:        "t1",
			Payload:     []byte(`{}`),
			Priority:    0,
			MaxAttempts: 3,
			ScheduledAt: now,
			CreatedAt:   now,
			UpdatedAt:   now,
		},
		Source: "producer",
	}))
	_, err := svc.DispatchEligibleJobs(ctx, DispatchOptions{
		LeaderAllowed: true,
		AsOf:          now.Add(time.Second),
		Limit:         10,
		AssignLease: func(job store.Job) (string, time.Time, error) {
			return "w1", now.Add(time.Minute), nil
		},
		Now: now.Add(time.Second),
	})
	require.NoError(t, err)

	require.NoError(t, svc.HandleWorkerStarted(ctx, WorkerStartedParams{JobID: jobID, WorkerID: "w1", Source: "w"}))
	j1, err := store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	v1 := j1.Version

	require.NoError(t, svc.HandleWorkerStarted(ctx, WorkerStartedParams{JobID: jobID, WorkerID: "w1", Source: "w"}))
	j2, err := store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.Equal(t, v1, j2.Version, "duplicate started should not bump version")
}

func TestIntegration_WrongWorkerStartedIgnored(t *testing.T) {
	pool := testPool(t)
	svc := newTestLifecycle(t, pool)
	ctx := context.Background()

	jobID := "job-wrongw-" + time.Now().Format("150405.000000000")
	now := time.Now().UTC().Truncate(time.Millisecond)

	require.NoError(t, svc.CreateJobAndAudit(ctx, CreateJobParams{
		Job: store.InsertJobInput{
			JobID: jobID, Queue: "q", Type: "t", Payload: []byte(`{}`),
			MaxAttempts: 3, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
		},
		Source: "producer",
	}))
	_, err := svc.DispatchEligibleJobs(ctx, DispatchOptions{
		LeaderAllowed: true,
		AsOf:          now.Add(time.Second),
		Limit:         10,
		AssignLease: func(job store.Job) (string, time.Time, error) {
			return "lease-owner", now.Add(time.Minute), nil
		},
		Now: now.Add(time.Second),
	})
	require.NoError(t, err)

	require.NoError(t, svc.HandleWorkerStarted(ctx, WorkerStartedParams{
		JobID: jobID, WorkerID: "other-worker", Source: "w",
	}))
	j, err := store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.Equal(t, store.JobStatusLeased, j.Status)
}

func TestIntegration_FailedAfterSucceededIgnored(t *testing.T) {
	pool := testPool(t)
	svc := newTestLifecycle(t, pool)
	ctx := context.Background()

	jobID := "job-succfail-" + time.Now().Format("150405.000000000")
	now := time.Now().UTC().Truncate(time.Millisecond)

	require.NoError(t, svc.CreateJobAndAudit(ctx, CreateJobParams{
		Job: store.InsertJobInput{
			JobID: jobID, Queue: "q", Type: "t", Payload: []byte(`{}`),
			MaxAttempts: 3, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
		},
		Source: "p",
	}))
	_, err := svc.DispatchEligibleJobs(ctx, DispatchOptions{
		LeaderAllowed: true,
		AsOf:          now.Add(time.Second),
		Limit:         10,
		AssignLease: func(job store.Job) (string, time.Time, error) {
			return "w", now.Add(time.Minute), nil
		},
		Now: now.Add(time.Second),
	})
	require.NoError(t, err)
	require.NoError(t, svc.HandleWorkerStarted(ctx, WorkerStartedParams{JobID: jobID, WorkerID: "w", Source: "w"}))
	require.NoError(t, svc.HandleWorkerSucceeded(ctx, WorkerSucceededParams{JobID: jobID, WorkerID: "w", Source: "w"}))

	js, err := store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	ver := js.Version

	require.NoError(t, svc.HandleWorkerFailed(ctx, WorkerFailedParams{
		JobID: jobID, WorkerID: "w", Source: "w",
		ErrorMessage: "late", ErrorCode: "E", Retryable: true,
		NextRetryAt: now.Add(time.Hour),
		Occurred:    now.Add(time.Minute),
	}))

	js2, err := store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.Equal(t, store.JobStatusSucceeded, js2.Status)
	require.Equal(t, ver, js2.Version)
}

func TestIntegration_RetryableFailThenRequeueDispatch(t *testing.T) {
	pool := testPool(t)
	svc := newTestLifecycle(t, pool)
	ctx := context.Background()

	jobID := "job-retry-" + time.Now().Format("150405.000000000")
	now := time.Now().UTC().Truncate(time.Millisecond)
	next := now.Add(2 * time.Second)

	require.NoError(t, svc.CreateJobAndAudit(ctx, CreateJobParams{
		Job: store.InsertJobInput{
			JobID: jobID, Queue: "q", Type: "t", Payload: []byte(`{}`),
			MaxAttempts: 5, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
		},
		Source: "p",
	}))
	_, err := svc.DispatchEligibleJobs(ctx, DispatchOptions{
		LeaderAllowed: true,
		AsOf:          now.Add(time.Second),
		Limit:         10,
		AssignLease: func(job store.Job) (string, time.Time, error) {
			return "w", now.Add(10 * time.Minute), nil
		},
		Now: now.Add(time.Second),
	})
	require.NoError(t, err)
	require.NoError(t, svc.HandleWorkerStarted(ctx, WorkerStartedParams{JobID: jobID, WorkerID: "w", Source: "w"}))

	require.NoError(t, svc.HandleWorkerFailed(ctx, WorkerFailedParams{
		JobID: jobID, WorkerID: "w", Source: "w",
		ErrorMessage: "transient", ErrorCode: "T", Retryable: true,
		NextRetryAt: next,
		Occurred:    now.Add(1500 * time.Millisecond),
	}))

	j, err := store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.Equal(t, store.JobStatusFailed, j.Status)
	require.Equal(t, 1, j.AttemptsMade)

	// Leader dispatch after retry time: should MarkRequeued + MarkLeased.
	n, err := svc.DispatchEligibleJobs(ctx, DispatchOptions{
		LeaderAllowed: true,
		AsOf:          next.Add(time.Second),
		Limit:         10,
		AssignLease: func(job store.Job) (string, time.Time, error) {
			return "w", next.Add(10*time.Minute), nil
		},
		Now: next.Add(time.Second),
	})
	require.NoError(t, err)
	require.Equal(t, 1, n)

	j, err = store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.Equal(t, store.JobStatusLeased, j.Status)
}
