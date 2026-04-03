package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"scheduler/internal/store"
)

// RetryCoordination is optional Redis-backed retry bookkeeping (never authoritative vs DB).
type RetryCoordination interface {
	IsScheduled(ctx context.Context, jobID string) (bool, error)
	ScheduleRetry(ctx context.Context, jobID string, at time.Time) error
	RemoveRetry(ctx context.Context, jobID string) error
	ListScheduledJobIDs(ctx context.Context, limit int64) ([]string, error)
}

// JobLeaseProbe inspects Redis (or other) per-job execution lease liveness.
type JobLeaseProbe interface {
	IsActive(ctx context.Context, jobID string) (bool, error)
}

// LeaseProbeFunc adapts a callback to JobLeaseProbe (e.g. redisLeaseStore.IsActive).
type LeaseProbeFunc func(ctx context.Context, jobID string) (bool, error)

func (f LeaseProbeFunc) IsActive(ctx context.Context, jobID string) (bool, error) {
	return f(ctx, jobID)
}

// ReconcileLimits caps work per Reconcile pass (conservative batches).
type ReconcileLimits struct {
	StaleRedisScan int64
	LeasedExpired  int
	RunningProbe   int
	FailedRetry    int
}

// ReconcileOptions configures a single reconciliation pass (leader must allow).
type ReconcileOptions struct {
	LeaderAllowed bool
	Now           time.Time
	Limits        ReconcileLimits
	SchedulerID   *string
	Source        string // audit source label, e.g. "scheduler_recovery"

	// RetryCoord is optional; when nil, retry ZSET restore / stale scan steps are skipped.
	RetryCoord RetryCoordination
	// JobLease is optional; when nil, running abandonment checks are skipped.
	JobLease JobLeaseProbe
}

// Reconciler performs leader-only startup/periodic reconciliation: DB truth, Redis coordination.
type Reconciler struct {
	pool  *pgxpool.Pool
	jobs  store.JobStore
	audit store.AuditStore
}

// NewReconciler builds a reconciler. Use the same pool as JobLifecycle.
func NewReconciler(pool *pgxpool.Pool, jobs store.JobStore, audit store.AuditStore) *Reconciler {
	return &Reconciler{pool: pool, jobs: jobs, audit: audit}
}

// Reconcile runs one bounded pass. Must only be called when the scheduler instance is leader.
//
// Ordering: stale Redis entries (cheap DB reads + ZREM), leased expiry, running abandonment,
// then restore missing retry ZSET rows from failed/retryable DB state.
// Redis mutations after DB commits are best-effort (no cross-store transactions).
func (r *Reconciler) Reconcile(ctx context.Context, opts ReconcileOptions) error {
	if !opts.LeaderAllowed {
		return ErrNotSchedulingLeader
	}
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	src := opts.Source
	if src == "" {
		src = "scheduler_recovery"
	}
	lim := opts.Limits
	if lim.StaleRedisScan <= 0 {
		lim.StaleRedisScan = 200
	}
	if lim.LeasedExpired <= 0 {
		lim.LeasedExpired = 50
	}
	if lim.RunningProbe <= 0 {
		lim.RunningProbe = 50
	}
	if lim.FailedRetry <= 0 {
		lim.FailedRetry = 100
	}

	if opts.RetryCoord != nil {
		if err := r.reconcileStaleRedisRetries(ctx, opts.RetryCoord, lim.StaleRedisScan, now, src, opts.SchedulerID); err != nil {
			return err
		}
	}
	if err := r.reconcileLeasedExpired(ctx, now, lim.LeasedExpired, src, opts.SchedulerID); err != nil {
		return err
	}
	if opts.JobLease != nil {
		if err := r.reconcileRunningAbandoned(ctx, opts.JobLease, now, lim.RunningProbe, src, opts.SchedulerID); err != nil {
			return err
		}
	}
	if opts.RetryCoord != nil {
		if err := r.reconcileRestoreRetryCoord(ctx, opts.RetryCoord, lim.FailedRetry, now, src, opts.SchedulerID); err != nil {
			return err
		}
	}
	return nil
}

// reconcileStaleRedisRetries removes ZSET members whose DB jobs are terminal (succeeded/dead)
// or missing. Conservative: only removes succeeded/dead/missing; leaves other states untouched.
func (r *Reconciler) reconcileStaleRedisRetries(ctx context.Context, coord RetryCoordination, limit int64, now time.Time, source string, schedID *string) error {
	ids, err := coord.ListScheduledJobIDs(ctx, limit)
	if err != nil {
		return err
	}
	for _, jobID := range ids {
		j, err := r.jobs.GetJobByID(ctx, r.pool, jobID)
		if errors.Is(err, store.ErrNotFound) {
			_ = coord.RemoveRetry(ctx, jobID)
			continue
		}
		if err != nil {
			return err
		}
		if j.Status == store.JobStatusSucceeded || j.Status == store.JobStatusDead {
			if err := coord.RemoveRetry(ctx, jobID); err != nil {
				return err
			}
			if err := r.appendAuditOnly(ctx, schedID, source, "recovery_stale_retry_removed", jobID, map[string]any{
				"reason": "db_terminal_or_ineligible",
				"status": j.Status,
			}, now); err != nil {
				return err
			}
		}
	}
	return nil
}

// reconcileLeasedExpired returns scheduler leases that exceeded lease_expires_at to queued.
//
// Assumption: DB lease expiry means the scheduler's dispatch lease to a worker was not
// progressed (e.g. process crash) before expiry; re-queuing is safe and does not increment attempts.
func (r *Reconciler) reconcileLeasedExpired(ctx context.Context, now time.Time, limit int, source string, schedID *string) error {
	candidates, err := r.jobs.ListLeasedPastExpiry(ctx, r.pool, now, limit)
	if err != nil {
		return err
	}
	for _, j := range candidates {
		tx, err := r.pool.Begin(ctx)
		if err != nil {
			return err
		}
		if err := r.jobs.MarkLeasedExpiredToQueued(ctx, tx, j.JobID, j.Version, now, now); err != nil {
			_ = tx.Rollback(ctx)
			if errors.Is(err, store.ErrPreconditionFailed) {
				continue
			}
			return err
		}
		j2, err := r.jobs.GetJobByID(ctx, tx, j.JobID)
		if err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := r.appendRecoveryAudit(ctx, tx, schedID, source, "recovery_lease_expired_requeued", j.JobID, map[string]any{
			"version": j2.Version,
		}, now); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

// reconcileRunningAbandoned moves rows stuck in running when Redis per-job lease is inactive.
//
// Assumption: workers renew the Redis execution lease while executing; if the key is gone, we
// treat execution as abandoned. This may mis-read during brief Redis outages—preferring
// conservative recovery, a false positive only re-queues without incrementing attempts.
func (r *Reconciler) reconcileRunningAbandoned(ctx context.Context, probe JobLeaseProbe, now time.Time, limit int, source string, schedID *string) error {
	running, err := r.jobs.ListRunningJobs(ctx, r.pool, limit)
	if err != nil {
		return err
	}
	for _, j := range running {
		active, err := probe.IsActive(ctx, j.JobID)
		if err != nil {
			return err
		}
		if active {
			continue
		}
		tx, err := r.pool.Begin(ctx)
		if err != nil {
			return err
		}
		scheduled := now
		if err := r.jobs.MarkRunningAbandonedToQueued(ctx, tx, j.JobID, j.Version, scheduled, now); err != nil {
			_ = tx.Rollback(ctx)
			if errors.Is(err, store.ErrPreconditionFailed) {
				continue
			}
			return err
		}
		j2, err := r.jobs.GetJobByID(ctx, tx, j.JobID)
		if err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := r.appendRecoveryAudit(ctx, tx, schedID, source, "recovery_running_abandoned_requeued", j.JobID, map[string]any{
			"version":    j2.Version,
			"redis_lease": "inactive",
		}, now); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

// reconcileRestoreRetryCoord ensures Redis retry ZSET has entries for DB failed/retryable jobs.
// Redis absence does not imply “no retry”; we re-add using DB scheduled_at as the score.
func (r *Reconciler) reconcileRestoreRetryCoord(ctx context.Context, coord RetryCoordination, limit int, now time.Time, source string, schedID *string) error {
	jobs, err := r.jobs.ListFailedRetryableForCoordination(ctx, r.pool, limit)
	if err != nil {
		return err
	}
	for _, j := range jobs {
		ok, err := coord.IsScheduled(ctx, j.JobID)
		if err != nil {
			return err
		}
		if ok {
			continue
		}
		retryAt := j.ScheduledAt
		if retryAt.IsZero() {
			retryAt = now
		}
		if err := coord.ScheduleRetry(ctx, j.JobID, retryAt); err != nil {
			return err
		}
		if err := r.appendAuditOnly(ctx, schedID, source, "recovery_retry_coord_restored", j.JobID, map[string]any{
			"retry_at": retryAt,
			"version":  j.Version,
		}, now); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reconciler) appendRecoveryAudit(ctx context.Context, tx store.Querier, schedID *string, source, typ, jobID string, details map[string]any, occurred time.Time) error {
	b, err := json.Marshal(details)
	if err != nil {
		return err
	}
	ev := uuid.NewString()
	jid := jobID
	return r.audit.AppendEvent(ctx, tx, store.AuditEventInput{
		EventID:     ev,
		JobID:       &jid,
		SchedulerID: schedID,
		Source:      source,
		Type:        typ,
		OccurredAt:  occurred,
		Details:     b,
	})
}

func (r *Reconciler) appendAuditOnly(ctx context.Context, schedID *string, source, typ, jobID string, details map[string]any, occurred time.Time) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := r.appendRecoveryAudit(ctx, tx, schedID, source, typ, jobID, details, occurred); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
