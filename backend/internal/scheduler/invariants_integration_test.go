//go:build integration

// Invariant-focused integration tests. Complements recovery_integration_test.go:
//
//	— Restore missing retry ZSET from DB: TestRecovery_RestoresRetryCoordWhenMissing
//	— Stale retry Z vs terminal DB: TestRecovery_RemovesStaleRetryWhenTerminal
//	— Non-leader reconciliation: TestRecovery_NonLeaderRejected
package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"scheduler/internal/integrationtest"
	"scheduler/internal/store"
)

var errInjectedFault = errors.New("injected_fault")

// faultAudit fails AppendEvent for configured event types (transaction rolls back with job row).
type faultAudit struct {
	store.AuditStore
	failTypes map[string]struct{}
}

func newFaultAudit(failTypes ...string) store.AuditStore {
	m := make(map[string]struct{}, len(failTypes))
	for _, t := range failTypes {
		m[t] = struct{}{}
	}
	return &faultAudit{AuditStore: store.NewPostgresAuditStore(), failTypes: m}
}

func (f *faultAudit) AppendEvent(ctx context.Context, q store.Querier, in store.AuditEventInput) error {
	if _, ok := f.failTypes[in.Type]; ok {
		return errInjectedFault
	}
	return f.AuditStore.AppendEvent(ctx, q, in)
}

// faultJobStore delegates to Postgres; MarkSucceeded can be forced to fail (audit must not commit alone).
type faultJobStore struct {
	store.JobStore
	markSucceededErr error
}

func newFaultJobStore(markSucceededErr error) store.JobStore {
	return &faultJobStore{
		JobStore:         store.NewPostgresJobStore(),
		markSucceededErr: markSucceededErr,
	}
}

func (f *faultJobStore) MarkSucceeded(ctx context.Context, q store.Querier, jobID string, expectedVersion int, updatedAt time.Time) error {
	if f.markSucceededErr != nil {
		return f.markSucceededErr
	}
	return f.JobStore.MarkSucceeded(ctx, q, jobID, expectedVersion, updatedAt)
}

func auditCount(ctx context.Context, pool *pgxpool.Pool, jobID, typ string) int {
	var n int
	_ = pool.QueryRow(ctx,
		`SELECT COUNT(*)::int FROM execution_audit WHERE job_id = $1 AND type = $2`,
		jobID, typ).Scan(&n)
	return n
}

func TestInvariant_AuditFailureRollsBackCreate_noJobRow(t *testing.T) {
	pool := integrationtest.Pool(t)
	ctx := context.Background()
	svc := NewJobLifecycle(pool, store.NewPostgresJobStore(), newFaultAudit("job_created"))

	jobID := "inv-create-" + time.Now().Format("150405.000000000")
	now := time.Now().UTC().Truncate(time.Millisecond)

	err := svc.CreateJobAndAudit(ctx, CreateJobParams{
		Job: store.InsertJobInput{
			JobID: jobID, Queue: "q", Type: "t", Payload: []byte(`{}`),
			MaxAttempts: 3, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
		},
		Source: "test",
	})
	require.ErrorIs(t, err, errInjectedFault)

	_, err = store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.ErrorIs(t, err, store.ErrNotFound)
}

func TestInvariant_AuditFailureRollsBackDispatch_staysQueued(t *testing.T) {
	pool := integrationtest.Pool(t)
	ctx := context.Background()
	pg := store.NewPostgresJobStore()
	svc := NewJobLifecycle(pool, pg, newFaultAudit("job_dispatched"))

	jobID := "inv-disp-" + time.Now().Format("150405.000000000")
	now := time.Now().UTC().Truncate(time.Millisecond)

	require.NoError(t, svc.CreateJobAndAudit(ctx, CreateJobParams{
		Job: store.InsertJobInput{
			JobID: jobID, Queue: "q", Type: "t", Payload: []byte(`{}`),
			MaxAttempts: 3, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
		},
		Source: "test",
	}))

	_, err := svc.DispatchEligibleJobs(ctx, DispatchOptions{
		LeaderAllowed: true,
		AsOf:          now.Add(time.Second),
		Limit:         10,
		AssignLease: func(job store.Job) (string, time.Time, error) {
			_ = job
			return "w1", now.Add(10 * time.Minute), nil
		},
		Now: now.Add(time.Second),
	})
	require.ErrorIs(t, err, errInjectedFault)

	j, err := pg.GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.Equal(t, store.JobStatusQueued, j.Status)
	require.Equal(t, 0, auditCount(ctx, pool, jobID, "job_dispatched"))
}

func TestInvariant_AuditFailureRollsBackWorkerStarted_staysLeased(t *testing.T) {
	pool := integrationtest.Pool(t)
	ctx := context.Background()
	pg := store.NewPostgresJobStore()
	svc := NewJobLifecycle(pool, pg, newFaultAudit("worker_started"))

	jobID := "inv-start-" + time.Now().Format("150405.000000000")
	now := time.Now().UTC().Truncate(time.Millisecond)

	require.NoError(t, svc.CreateJobAndAudit(ctx, CreateJobParams{
		Job: store.InsertJobInput{
			JobID: jobID, Queue: "q", Type: "t", Payload: []byte(`{}`),
			MaxAttempts: 3, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
		},
		Source: "test",
	}))

	_, err := svc.DispatchEligibleJobs(ctx, DispatchOptions{
		LeaderAllowed: true,
		AsOf:          now.Add(time.Second),
		Limit:         10,
		AssignLease: func(job store.Job) (string, time.Time, error) {
			return "w1", now.Add(10 * time.Minute), nil
		},
		Now: now.Add(time.Second),
	})
	require.NoError(t, err)

	err = svc.HandleWorkerStarted(ctx, WorkerStartedParams{
		JobID:    jobID,
		WorkerID: "w1",
		Source:   "worker",
		Occurred: now.Add(2 * time.Second),
	})
	require.ErrorIs(t, err, errInjectedFault)

	j, err := pg.GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.Equal(t, store.JobStatusLeased, j.Status)
	require.Equal(t, 0, auditCount(ctx, pool, jobID, "worker_started"))
}

func TestInvariant_JobMutationFailureMeansNoSucceededAudit(t *testing.T) {
	pool := integrationtest.Pool(t)
	ctx := context.Background()
	svc := NewJobLifecycle(pool, newFaultJobStore(errInjectedFault), store.NewPostgresAuditStore())

	jobID := "inv-succ-" + time.Now().Format("150405.000000000")
	now := time.Now().UTC().Truncate(time.Millisecond)

	require.NoError(t, svc.CreateJobAndAudit(ctx, CreateJobParams{
		Job: store.InsertJobInput{
			JobID: jobID, Queue: "q", Type: "t", Payload: []byte(`{}`),
			MaxAttempts: 3, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
		},
		Source: "test",
	}))

	base := NewJobLifecycle(pool, store.NewPostgresJobStore(), store.NewPostgresAuditStore())
	_, err := base.DispatchEligibleJobs(ctx, DispatchOptions{
		LeaderAllowed: true,
		AsOf:          now.Add(time.Second),
		Limit:         10,
		AssignLease: func(job store.Job) (string, time.Time, error) {
			return "w1", now.Add(10 * time.Minute), nil
		},
		Now: now.Add(time.Second),
	})
	require.NoError(t, err)
	require.NoError(t, base.HandleWorkerStarted(ctx, WorkerStartedParams{
		JobID: jobID, WorkerID: "w1", Source: "worker", Occurred: now.Add(2 * time.Second),
	}))

	err = svc.HandleWorkerSucceeded(ctx, WorkerSucceededParams{
		JobID: jobID, WorkerID: "w1", Source: "worker", Occurred: now.Add(3 * time.Second),
	})
	require.ErrorIs(t, err, errInjectedFault)

	j, err := store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.Equal(t, store.JobStatusRunning, j.Status)
	require.Equal(t, 0, auditCount(ctx, pool, jobID, "worker_succeeded"))
}

func TestInvariant_StaleFailedAfterSucceeded_noRegression(t *testing.T) {
	pool := integrationtest.Pool(t)
	ctx := context.Background()
	svc := newTestLifecycle(t, pool)

	jobID := "inv-stalef-" + time.Now().Format("150405.000000000")
	now := time.Now().UTC().Truncate(time.Millisecond)

	require.NoError(t, svc.CreateJobAndAudit(ctx, CreateJobParams{
		Job: store.InsertJobInput{
			JobID: jobID, Queue: "q", Type: "t", Payload: []byte(`{}`),
			MaxAttempts: 3, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
		},
		Source: "test",
	}))
	_, err := svc.DispatchEligibleJobs(ctx, DispatchOptions{
		LeaderAllowed: true,
		AsOf:          now.Add(time.Second), Limit: 10,
		AssignLease: func(job store.Job) (string, time.Time, error) {
			return "w1", now.Add(10 * time.Minute), nil
		},
		Now: now.Add(time.Second),
	})
	require.NoError(t, err)
	require.NoError(t, svc.HandleWorkerStarted(ctx, WorkerStartedParams{
		JobID: jobID, WorkerID: "w1", Source: "worker", Occurred: now.Add(2 * time.Second),
	}))
	require.NoError(t, svc.HandleWorkerSucceeded(ctx, WorkerSucceededParams{
		JobID: jobID, WorkerID: "w1", Source: "worker", Occurred: now.Add(3 * time.Second),
	}))

	require.NoError(t, svc.HandleWorkerFailed(ctx, WorkerFailedParams{
		JobID: jobID, WorkerID: "w1", Source: "worker",
		ErrorMessage: "late", ErrorCode: "late",
		Retryable: true, NextRetryAt: now.Add(time.Hour), Occurred: now.Add(4 * time.Second),
	}))

	j, err := store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.Equal(t, store.JobStatusSucceeded, j.Status)
}

func TestInvariant_StaleStartedAfterSucceeded_noop(t *testing.T) {
	pool := integrationtest.Pool(t)
	ctx := context.Background()
	svc := newTestLifecycle(t, pool)

	jobID := "inv-stales-" + time.Now().Format("150405.000000000")
	now := time.Now().UTC().Truncate(time.Millisecond)

	require.NoError(t, svc.CreateJobAndAudit(ctx, CreateJobParams{
		Job: store.InsertJobInput{
			JobID: jobID, Queue: "q", Type: "t", Payload: []byte(`{}`),
			MaxAttempts: 3, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
		},
		Source: "test",
	}))
	_, err := svc.DispatchEligibleJobs(ctx, DispatchOptions{
		LeaderAllowed: true,
		AsOf:          now.Add(time.Second), Limit: 10,
		AssignLease: func(job store.Job) (string, time.Time, error) {
			return "w1", now.Add(10 * time.Minute), nil
		},
		Now: now.Add(time.Second),
	})
	require.NoError(t, err)
	require.NoError(t, svc.HandleWorkerStarted(ctx, WorkerStartedParams{
		JobID: jobID, WorkerID: "w1", Source: "worker", Occurred: now.Add(2 * time.Second),
	}))
	require.NoError(t, svc.HandleWorkerSucceeded(ctx, WorkerSucceededParams{
		JobID: jobID, WorkerID: "w1", Source: "worker", Occurred: now.Add(3 * time.Second),
	}))
	vBefore := mustJob(t, ctx, pool, jobID).Version

	require.NoError(t, svc.HandleWorkerStarted(ctx, WorkerStartedParams{
		JobID: jobID, WorkerID: "w1", Source: "worker", Occurred: now.Add(5 * time.Second),
	}))

	j := mustJob(t, ctx, pool, jobID)
	require.Equal(t, store.JobStatusSucceeded, j.Status)
	require.Equal(t, vBefore, j.Version)
}

func TestInvariant_StaleStartedWrongLeaseAfterRedispatch_noop(t *testing.T) {
	pool := integrationtest.Pool(t)
	ctx := context.Background()
	svc := newTestLifecycle(t, pool)

	jobID := "inv-redisp-" + time.Now().Format("150405.000000000")
	now := time.Now().UTC().Truncate(time.Millisecond)

	require.NoError(t, svc.CreateJobAndAudit(ctx, CreateJobParams{
		Job: store.InsertJobInput{
			JobID: jobID, Queue: "q", Type: "t", Payload: []byte(`{}`),
			MaxAttempts: 5, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
		},
		Source: "test",
	}))
	_, err := svc.DispatchEligibleJobs(ctx, DispatchOptions{
		LeaderAllowed: true,
		AsOf:          now.Add(time.Second), Limit: 10,
		AssignLease: func(job store.Job) (string, time.Time, error) {
			return "w1", now.Add(10 * time.Minute), nil
		},
		Now: now.Add(time.Second),
	})
	require.NoError(t, err)
	require.NoError(t, svc.HandleWorkerStarted(ctx, WorkerStartedParams{
		JobID: jobID, WorkerID: "w1", Source: "worker", Occurred: now.Add(2 * time.Second),
	}))
	next := now.Add(time.Minute)
	require.NoError(t, svc.HandleWorkerFailed(ctx, WorkerFailedParams{
		JobID: jobID, WorkerID: "w1", Source: "worker",
		ErrorMessage: "e", ErrorCode: "e", Retryable: true, NextRetryAt: next, Occurred: now.Add(3 * time.Second),
	}))

	_, err = svc.DispatchEligibleJobs(ctx, DispatchOptions{
		LeaderAllowed: true,
		AsOf:          next,
		Limit:         10,
		AssignLease: func(job store.Job) (string, time.Time, error) {
			return "w2", now.Add(20 * time.Minute), nil
		},
		Now: next,
	})
	require.NoError(t, err)

	j := mustJob(t, ctx, pool, jobID)
	require.Equal(t, store.JobStatusLeased, j.Status)
	require.NotNil(t, j.LeaseOwnerWorkerID)
	require.Equal(t, "w2", *j.LeaseOwnerWorkerID)

	require.NoError(t, svc.HandleWorkerStarted(ctx, WorkerStartedParams{
		JobID: jobID, WorkerID: "w1", Source: "worker", Occurred: next.Add(time.Second),
	}))

	j = mustJob(t, ctx, pool, jobID)
	require.Equal(t, store.JobStatusLeased, j.Status)
	require.Equal(t, "w2", *j.LeaseOwnerWorkerID)
}

func TestInvariant_StaleSucceededWhenFailedVisible_noop(t *testing.T) {
	pool := integrationtest.Pool(t)
	ctx := context.Background()
	svc := newTestLifecycle(t, pool)

	jobID := "inv-failv-" + time.Now().Format("150405.000000000")
	now := time.Now().UTC().Truncate(time.Millisecond)

	require.NoError(t, svc.CreateJobAndAudit(ctx, CreateJobParams{
		Job: store.InsertJobInput{
			JobID: jobID, Queue: "q", Type: "t", Payload: []byte(`{}`),
			MaxAttempts: 3, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
		},
		Source: "test",
	}))
	_, err := svc.DispatchEligibleJobs(ctx, DispatchOptions{
		LeaderAllowed: true,
		AsOf:          now.Add(time.Second), Limit: 10,
		AssignLease: func(job store.Job) (string, time.Time, error) {
			return "w1", now.Add(10 * time.Minute), nil
		},
		Now: now.Add(time.Second),
	})
	require.NoError(t, err)
	require.NoError(t, svc.HandleWorkerStarted(ctx, WorkerStartedParams{
		JobID: jobID, WorkerID: "w1", Source: "worker", Occurred: now.Add(2 * time.Second),
	}))
	next := now.Add(time.Hour)
	require.NoError(t, svc.HandleWorkerFailed(ctx, WorkerFailedParams{
		JobID: jobID, WorkerID: "w1", Source: "worker",
		ErrorMessage: "e", ErrorCode: "e", Retryable: true, NextRetryAt: next, Occurred: now.Add(3 * time.Second),
	}))

	vBefore := mustJob(t, ctx, pool, jobID).Version
	require.NoError(t, svc.HandleWorkerSucceeded(ctx, WorkerSucceededParams{
		JobID: jobID, WorkerID: "w1", Source: "worker", Occurred: now.Add(4 * time.Second),
	}))

	j := mustJob(t, ctx, pool, jobID)
	require.Equal(t, store.JobStatusFailed, j.Status)
	require.Equal(t, vBefore, j.Version)
}

func TestInvariant_FollowerCannotRequeueFailedJob(t *testing.T) {
	pool := integrationtest.Pool(t)
	ctx := context.Background()
	svc := newTestLifecycle(t, pool)

	jobID := "inv-noreq-" + time.Now().Format("150405.000000000")
	now := time.Now().UTC().Truncate(time.Millisecond)
	past := now.Add(-time.Second)

	require.NoError(t, svc.CreateJobAndAudit(ctx, CreateJobParams{
		Job: store.InsertJobInput{
			JobID: jobID, Queue: "q", Type: "t", Payload: []byte(`{}`),
			MaxAttempts: 3, ScheduledAt: now, CreatedAt: now, UpdatedAt: now,
		},
		Source: "test",
	}))
	j, err := store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	_, err = svc.DispatchEligibleJobs(ctx, DispatchOptions{
		LeaderAllowed: true,
		AsOf:          now.Add(time.Second), Limit: 10,
		AssignLease: func(job store.Job) (string, time.Time, error) {
			return "w1", now.Add(10 * time.Minute), nil
		},
		Now: now.Add(time.Second),
	})
	require.NoError(t, err)
	j, err = store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	require.NoError(t, svc.HandleWorkerStarted(ctx, WorkerStartedParams{
		JobID: jobID, WorkerID: "w1", Source: "worker", Occurred: now.Add(2 * time.Second),
	}))
	require.NoError(t, svc.HandleWorkerFailed(ctx, WorkerFailedParams{
		JobID: jobID, WorkerID: "w1", Source: "worker",
		ErrorMessage: "e", ErrorCode: "e", Retryable: true, NextRetryAt: past, Occurred: now.Add(3 * time.Second),
	}))

	j = mustJob(t, ctx, pool, jobID)
	require.Equal(t, store.JobStatusFailed, j.Status)
	vBefore := j.Version

	_, err = svc.DispatchEligibleJobs(ctx, DispatchOptions{
		LeaderAllowed: false,
		AsOf:          now.Add(time.Hour),
		Limit:         10,
		AssignLease: func(job store.Job) (string, time.Time, error) {
			return "w2", now.Add(10 * time.Minute), nil
		},
		Now: now.Add(time.Hour),
	})
	require.ErrorIs(t, err, ErrNotSchedulingLeader)

	j = mustJob(t, ctx, pool, jobID)
	require.Equal(t, store.JobStatusFailed, j.Status)
	require.Equal(t, vBefore, j.Version)
}

func mustJob(t *testing.T, ctx context.Context, pool *pgxpool.Pool, jobID string) *store.Job {
	t.Helper()
	j, err := store.NewPostgresJobStore().GetJobByID(ctx, pool, jobID)
	require.NoError(t, err)
	return j
}
