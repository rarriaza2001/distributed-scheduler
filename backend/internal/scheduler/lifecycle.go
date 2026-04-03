package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"scheduler/internal/observability"
	"scheduler/internal/store"
)

// ErrNotSchedulingLeader means DispatchEligibleJobs was called without leader allowance.
var ErrNotSchedulingLeader = errors.New("scheduler: dispatch requires active scheduling leadership")

// JobLifecycle applies authoritative DB lifecycle policy using the shared pgx pool
// and plain store methods. Each transition runs in a transaction: job row update(s)
// plus execution_audit append. Pass the same pgx.Tx to store methods within a tx.
type JobLifecycle struct {
	pool  *pgxpool.Pool
	jobs  store.JobStore
	audit store.AuditStore

	metrics *observability.Metrics
	log     *slog.Logger
	failed  *observability.FailedRecentStore
}

// NewJobLifecycle constructs a lifecycle service. The pool must be the process-wide
// pool from db.Open; repositories must not create their own pools.
func NewJobLifecycle(pool *pgxpool.Pool, jobs store.JobStore, audit store.AuditStore) *JobLifecycle {
	return &JobLifecycle{pool: pool, jobs: jobs, audit: audit}
}

// SetObservability wires Phase 4 metrics, structured logs, and optional failed-job ring (terminal dead only).
func (s *JobLifecycle) SetObservability(m *observability.Metrics, log *slog.Logger, failed *observability.FailedRecentStore) {
	s.metrics = m
	s.log = log
	s.failed = failed
}

// CreateJobParams carries producer insert + audit metadata for job_created.
type CreateJobParams struct {
	Job         store.InsertJobInput
	Source      string // e.g. "producer"
	SchedulerID *string
}

// CreateJobAndAudit inserts a queued job and records job_created in one transaction.
func (s *JobLifecycle) CreateJobAndAudit(ctx context.Context, p CreateJobParams) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackUnlessCommitted(ctx, tx)

	if err := s.jobs.InsertJob(ctx, tx, p.Job); err != nil {
		return err
	}

	eventID := uuid.NewString()
	jobID := p.Job.JobID
	occurred := p.Job.CreatedAt
	if occurred.IsZero() {
		occurred = time.Now().UTC()
	}
	details, err := json.Marshal(map[string]any{"job_id": p.Job.JobID, "queue": p.Job.Queue})
	if err != nil {
		return err
	}
	if err := s.audit.AppendEvent(ctx, tx, store.AuditEventInput{
		EventID:     eventID,
		JobID:       &jobID,
		SchedulerID: p.SchedulerID,
		Source:      p.Source,
		Type:        "job_created",
		OccurredAt:  occurred,
		Details:     details,
	}); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// DispatchOptions configures leader-gated dispatch: queued jobs, and failed jobs that are being
// re-offered. For failed rows, MarkRequeued then MarkLeased run in one transaction (durable
// outcome failed→leased; see package doc transition table).
type DispatchOptions struct {
	// LeaderAllowed must be true only when this process holds scheduling leadership.
	LeaderAllowed bool
	AsOf          time.Time
	// Limit caps merged candidates from queued and failed-retry lists.
	Limit int
	// AssignLease chooses worker id and lease expiry for each job.
	AssignLease func(job store.Job) (workerID string, leaseUntil time.Time, err error)
	SchedulerID *string
	Now         time.Time
	// AfterLeaseCommitted runs after the dispatch transaction commits successfully.
	// Use for transport enqueue (e.g. Redis); failures do not roll back the DB lease.
	AfterLeaseCommitted func(ctx context.Context, job store.Job, workerID string, leaseUntil time.Time) error
}

// DispatchEligibleJobs moves schedulable work to leased under DB transaction(s),
// one job per transaction. Failed jobs are first MarkRequeued in the same tx
// before MarkLeased. Non-leader callers receive ErrNotSchedulingLeader.
func (s *JobLifecycle) DispatchEligibleJobs(ctx context.Context, opts DispatchOptions) (dispatched int, err error) {
	if !opts.LeaderAllowed {
		return 0, ErrNotSchedulingLeader
	}
	if opts.AssignLease == nil {
		return 0, fmt.Errorf("scheduler: AssignLease is required")
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	asOf := opts.AsOf
	if asOf.IsZero() {
		asOf = now
	}

	queued, err := s.jobs.ListSchedulableJobs(ctx, s.pool, asOf, limit)
	if err != nil {
		return 0, err
	}
	failedRetry, err := s.jobs.ListFailedAwaitingRequeue(ctx, s.pool, asOf, limit)
	if err != nil {
		return 0, err
	}

	candidates := mergeDispatchCandidates(queued, failedRetry, limit)

	for _, job := range candidates {
		workerID, leaseUntil, err := opts.AssignLease(job)
		if err != nil {
			return dispatched, err
		}
		n, err := s.dispatchOne(ctx, job, workerID, leaseUntil, now, opts)
		if err != nil {
			return dispatched, err
		}
		dispatched += n
	}
	return dispatched, nil
}

// dispatchOne runs a single job dispatch in its own transaction. Returns 1 if leased, 0 if skipped (race).
func (s *JobLifecycle) dispatchOne(ctx context.Context, snapshot store.Job, workerID string, leaseUntil, now time.Time, opts DispatchOptions) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer rollbackUnlessCommitted(ctx, tx)

	cur, err := s.jobs.GetJobByID(ctx, tx, snapshot.JobID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return 0, tx.Commit(ctx)
		}
		return 0, err
	}

	// Stale list entry (another scheduler instance won).
	if cur.Version != snapshot.Version || (cur.Status != store.JobStatusQueued && cur.Status != store.JobStatusFailed) {
		return 0, tx.Commit(ctx)
	}

	if cur.Status == store.JobStatusFailed {
		if err := s.jobs.MarkRequeued(ctx, tx, cur.JobID, cur.Version, now); err != nil {
			if errors.Is(err, store.ErrPreconditionFailed) {
				return 0, tx.Commit(ctx)
			}
			return 0, err
		}
		cur, err = s.jobs.GetJobByID(ctx, tx, cur.JobID)
		if err != nil {
			return 0, err
		}
	}

	if cur.Status != store.JobStatusQueued {
		return 0, tx.Commit(ctx)
	}

	if err := s.jobs.MarkLeased(ctx, tx, cur.JobID, cur.Version, workerID, leaseUntil, now); err != nil {
		if errors.Is(err, store.ErrPreconditionFailed) {
			return 0, tx.Commit(ctx)
		}
		return 0, err
	}

	cur, err = s.jobs.GetJobByID(ctx, tx, cur.JobID)
	if err != nil {
		return 0, err
	}

	eventID := uuid.NewString()
	jid := cur.JobID
	details, err := json.Marshal(map[string]any{
		"worker_id":   workerID,
		"version":     cur.Version,
		"lease_until": leaseUntil,
	})
	if err != nil {
		return 0, err
	}
	if err := s.audit.AppendEvent(ctx, tx, store.AuditEventInput{
		EventID:     eventID,
		JobID:       &jid,
		SchedulerID: opts.SchedulerID,
		Source:      "scheduler",
		Type:        "job_dispatched",
		OccurredAt:  now,
		Details:     details,
	}); err != nil {
		return 0, err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}

	if opts.AfterLeaseCommitted != nil {
		if err := opts.AfterLeaseCommitted(ctx, *cur, workerID, leaseUntil); err != nil {
			return 0, err
		}
	}
	return 1, nil
}

// WorkerStartedParams reports a worker has begun execution (leased -> running).
type WorkerStartedParams struct {
	JobID    string
	WorkerID string
	Source   string
	Occurred time.Time
	Details  map[string]any
}

// HandleWorkerStarted enforces leased -> running for the lease owner; duplicate/stale reports no-op.
func (s *JobLifecycle) HandleWorkerStarted(ctx context.Context, p WorkerStartedParams) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackUnlessCommitted(ctx, tx)

	j, err := s.jobs.GetJobByID(ctx, tx, p.JobID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return tx.Commit(ctx)
		}
		return err
	}

	if j.Status == store.JobStatusRunning {
		return tx.Commit(ctx)
	}
	if j.Status != store.JobStatusLeased {
		return tx.Commit(ctx)
	}
	if j.LeaseOwnerWorkerID == nil || *j.LeaseOwnerWorkerID != p.WorkerID {
		return tx.Commit(ctx)
	}

	occurred := p.Occurred
	if occurred.IsZero() {
		occurred = time.Now().UTC()
	}
	if err := s.jobs.MarkRunning(ctx, tx, j.JobID, j.Version, occurred); err != nil {
		if errors.Is(err, store.ErrPreconditionFailed) {
			return tx.Commit(ctx)
		}
		return err
	}

	j2, err := s.jobs.GetJobByID(ctx, tx, j.JobID)
	if err != nil {
		return err
	}

	details := p.Details
	if details == nil {
		details = map[string]any{}
	}
	details["worker_id"] = p.WorkerID
	details["version"] = j2.Version
	b, err := json.Marshal(details)
	if err != nil {
		return err
	}
	wid := p.WorkerID
	eventID := uuid.NewString()
	jid := j.JobID
	if err := s.audit.AppendEvent(ctx, tx, store.AuditEventInput{
		EventID:    eventID,
		JobID:      &jid,
		WorkerID:   &wid,
		Source:     p.Source,
		Type:       "worker_started",
		OccurredAt: occurred,
		Details:    b,
	}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	s.observeQueueWait(j, occurred)
	return nil
}

// WorkerSucceededParams reports terminal success.
type WorkerSucceededParams struct {
	JobID    string
	WorkerID string
	Source   string
	Occurred time.Time
	Details  map[string]any
	// WorkerStartedAt is the worker-local start time (aligned with HandleWorkerStarted.Occurred).
	WorkerStartedAt time.Time
}

// HandleWorkerSucceeded moves running -> succeeded; never overwrites terminal success/dead.
func (s *JobLifecycle) HandleWorkerSucceeded(ctx context.Context, p WorkerSucceededParams) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackUnlessCommitted(ctx, tx)

	j, err := s.jobs.GetJobByID(ctx, tx, p.JobID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return tx.Commit(ctx)
		}
		return err
	}

	if j.Status == store.JobStatusSucceeded || j.Status == store.JobStatusDead {
		return tx.Commit(ctx)
	}
	if j.Status != store.JobStatusRunning {
		return tx.Commit(ctx)
	}

	occurred := p.Occurred
	if occurred.IsZero() {
		occurred = time.Now().UTC()
	}
	if err := s.jobs.MarkSucceeded(ctx, tx, j.JobID, j.Version, occurred); err != nil {
		if errors.Is(err, store.ErrPreconditionFailed) {
			return tx.Commit(ctx)
		}
		return err
	}

	j2, err := s.jobs.GetJobByID(ctx, tx, j.JobID)
	if err != nil {
		return err
	}

	details := p.Details
	if details == nil {
		details = map[string]any{}
	}
	details["worker_id"] = p.WorkerID
	details["version"] = j2.Version
	b, err := json.Marshal(details)
	if err != nil {
		return err
	}
	wid := p.WorkerID
	eventID := uuid.NewString()
	jid := j.JobID
	if err := s.audit.AppendEvent(ctx, tx, store.AuditEventInput{
		EventID:    eventID,
		JobID:      &jid,
		WorkerID:   &wid,
		Source:     p.Source,
		Type:       "worker_succeeded",
		OccurredAt: occurred,
		Details:    b,
	}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	s.observeSuccessLatencies(j, p)
	return nil
}

// WorkerFailedParams reports execution failure; caller supplies retry policy inputs.
type WorkerFailedParams struct {
	JobID        string
	WorkerID     string
	Source       string
	ErrorMessage string
	ErrorCode    string
	Retryable    bool
	NextRetryAt  time.Time // used when Retryable and not dead-lettered
	Occurred     time.Time
	Details      map[string]any
	// WorkerStartedAt is the worker-local start time (aligned with HandleWorkerStarted.Occurred).
	WorkerStartedAt time.Time
}

// HandleWorkerFailed moves running -> failed (retry scheduled) or -> dead. Ignores stale/duplicate reports
// when already terminal, and does not regress succeeded/dead.
func (s *JobLifecycle) HandleWorkerFailed(ctx context.Context, p WorkerFailedParams) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackUnlessCommitted(ctx, tx)

	j, err := s.jobs.GetJobByID(ctx, tx, p.JobID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return tx.Commit(ctx)
		}
		return err
	}

	if j.Status == store.JobStatusSucceeded || j.Status == store.JobStatusDead {
		return tx.Commit(ctx)
	}
	if j.Status == store.JobStatusFailed {
		// Visible failed state: tolerate duplicate failure delivery.
		return tx.Commit(ctx)
	}
	if j.Status != store.JobStatusRunning {
		return tx.Commit(ctx)
	}

	occurred := p.Occurred
	if occurred.IsZero() {
		occurred = time.Now().UTC()
	}

	newAttempts := j.AttemptsMade + 1
	errMsg := p.ErrorMessage
	errCode := p.ErrorCode
	retryable := p.Retryable
	dead := !p.Retryable || newAttempts >= j.MaxAttempts

	details := p.Details
	if details == nil {
		details = map[string]any{}
	}
	details["worker_id"] = p.WorkerID
	details["retryable"] = p.Retryable
	details["dead"] = dead

	if dead {
		if err := s.jobs.MarkDead(ctx, tx, j.JobID, j.Version, newAttempts, &errMsg, &errCode, &retryable, &occurred, occurred); err != nil {
			if errors.Is(err, store.ErrPreconditionFailed) {
				return tx.Commit(ctx)
			}
			return err
		}
		details["outcome"] = "dead"
	} else {
		if err := s.jobs.MarkFailedAndScheduleRetry(ctx, tx, j.JobID, j.Version, newAttempts, &errMsg, &errCode, &retryable, occurred, p.NextRetryAt, occurred); err != nil {
			if errors.Is(err, store.ErrPreconditionFailed) {
				return tx.Commit(ctx)
			}
			return err
		}
		details["outcome"] = "failed_retry_scheduled"
		details["next_retry_at"] = p.NextRetryAt
	}

	j2, err := s.jobs.GetJobByID(ctx, tx, j.JobID)
	if err != nil {
		return err
	}
	details["version"] = j2.Version

	b, err := json.Marshal(details)
	if err != nil {
		return err
	}
	wid := p.WorkerID
	eventID := uuid.NewString()
	jid := j.JobID
	if err := s.audit.AppendEvent(ctx, tx, store.AuditEventInput{
		EventID:    eventID,
		JobID:      &jid,
		WorkerID:   &wid,
		Source:     p.Source,
		Type:       "worker_failed",
		OccurredAt: occurred,
		Details:    b,
	}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	s.observeFailureLatencies(ctx, j, p, dead, newAttempts, errMsg, errCode, retryable)
	return nil
}

func rollbackUnlessCommitted(ctx context.Context, tx pgx.Tx) {
	_ = tx.Rollback(ctx)
}

func mergeDispatchCandidates(queued, failed []store.Job, limit int) []store.Job {
	if limit <= 0 {
		limit = 100
	}
	i, j := 0, 0
	out := make([]store.Job, 0, limit)
	less := func(a, b store.Job) bool {
		if a.ScheduledAt.Equal(b.ScheduledAt) {
			if a.Priority == b.Priority {
				return a.JobID < b.JobID
			}
			return a.Priority > b.Priority
		}
		return a.ScheduledAt.Before(b.ScheduledAt)
	}
	for len(out) < limit && (i < len(queued) || j < len(failed)) {
		if i >= len(queued) {
			out = append(out, failed[j])
			j++
			continue
		}
		if j >= len(failed) {
			out = append(out, queued[i])
			i++
			continue
		}
		if less(queued[i], failed[j]) {
			out = append(out, queued[i])
			i++
		} else {
			out = append(out, failed[j])
			j++
		}
	}
	return out
}
