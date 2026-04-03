package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

const jobSelectColumns = `job_id, queue, type, payload, priority, status,
	attempts_made, max_attempts, created_at, updated_at, scheduled_at,
	lease_owner_worker_id, lease_expires_at,
	last_error_message, last_error_code, last_error_retryable, last_failed_at,
	idempotency_key, version`

// JobStore persists job rows. Methods are plain SQL; lifecycle policy belongs
// in the scheduler service. Pass the process pool or an active transaction as Querier.
type JobStore interface {
	InsertJob(ctx context.Context, q Querier, in InsertJobInput) error
	GetJobByID(ctx context.Context, q Querier, jobID string) (*Job, error)
	ListSchedulableJobs(ctx context.Context, q Querier, asOf time.Time, limit int) ([]Job, error)
	// ListFailedAwaitingRequeue lists failed jobs eligible to be moved back to queued (retry path).
	ListFailedAwaitingRequeue(ctx context.Context, q Querier, asOf time.Time, limit int) ([]Job, error)
	MarkRequeued(ctx context.Context, q Querier, jobID string, expectedVersion int, updatedAt time.Time) error
	MarkLeased(ctx context.Context, q Querier, jobID string, expectedVersion int, workerID string, leaseExpiresAt, updatedAt time.Time) error
	MarkRunning(ctx context.Context, q Querier, jobID string, expectedVersion int, updatedAt time.Time) error
	MarkSucceeded(ctx context.Context, q Querier, jobID string, expectedVersion int, updatedAt time.Time) error
	MarkFailedAndScheduleRetry(ctx context.Context, q Querier, jobID string, expectedVersion int,
		newAttemptsMade int, lastErrorMessage, lastErrorCode *string, lastErrorRetryable *bool,
		lastFailedAt time.Time, nextScheduledAt, updatedAt time.Time) error
	MarkDead(ctx context.Context, q Querier, jobID string, expectedVersion int,
		newAttemptsMade int, lastErrorMessage, lastErrorCode *string, lastErrorRetryable *bool,
		lastFailedAt *time.Time, updatedAt time.Time) error
	ClearLease(ctx context.Context, q Querier, jobID string, expectedVersion int, updatedAt time.Time) error

	// ListLeasedPastExpiry lists leased rows whose DB lease_expires_at is before asOf.
	ListLeasedPastExpiry(ctx context.Context, q Querier, asOf time.Time, limit int) ([]Job, error)
	// MarkLeasedExpiredToQueued moves leased -> queued when lease_expires_at < asOf (scheduler recovery).
	MarkLeasedExpiredToQueued(ctx context.Context, q Querier, jobID string, expectedVersion int, asOf time.Time, updatedAt time.Time) error
	ListRunningJobs(ctx context.Context, q Querier, limit int) ([]Job, error)
	// MarkRunningAbandonedToQueued moves running -> queued without incrementing attempts_made
	// (abandonment detected via coordination signals, not a worker failure report).
	MarkRunningAbandonedToQueued(ctx context.Context, q Querier, jobID string, expectedVersion int, scheduledAt, updatedAt time.Time) error
	// ListFailedRetryableForCoordination lists failed retryable jobs still under max_attempts (any scheduled_at).
	ListFailedRetryableForCoordination(ctx context.Context, q Querier, limit int) ([]Job, error)
}

// PostgresJobStore implements JobStore.
type PostgresJobStore struct{}

// NewPostgresJobStore returns a JobStore backed by Postgres.
func NewPostgresJobStore() *PostgresJobStore {
	return &PostgresJobStore{}
}

var _ JobStore = (*PostgresJobStore)(nil)

// InsertJob inserts a new queued job. ErrIdempotencyConflict on unique idempotency_key.
func (PostgresJobStore) InsertJob(ctx context.Context, q Querier, in InsertJobInput) error {
	_, err := q.Exec(ctx, `
INSERT INTO jobs (
	job_id, queue, type, payload, priority, status,
	attempts_made, max_attempts,
	created_at, updated_at, scheduled_at,
	idempotency_key, version
) VALUES (
	$1, $2, $3, $4, $5, 'queued',
	0, $6,
	$7, $8, $9,
	$10, 0
)`, in.JobID, in.Queue, in.Type, in.Payload, in.Priority,
		in.MaxAttempts, in.CreatedAt, in.UpdatedAt, in.ScheduledAt, in.IdempotencyKey)
	if err != nil {
		var pe *pgconn.PgError
		if errors.As(err, &pe) && pe.Code == "23505" {
			return ErrIdempotencyConflict
		}
		return err
	}
	return nil
}

// GetJobByID returns a job by primary key.
func (PostgresJobStore) GetJobByID(ctx context.Context, q Querier, jobID string) (*Job, error) {
	row := q.QueryRow(ctx, `
SELECT `+jobSelectColumns+`
FROM jobs
WHERE job_id = $1`, jobID)
	j, err := scanJob(row)
	if err != nil {
		return nil, mapNotFound(err)
	}
	return j, nil
}

// ListSchedulableJobs returns queued jobs with scheduled_at <= asOf.
func (PostgresJobStore) ListSchedulableJobs(ctx context.Context, q Querier, asOf time.Time, limit int) ([]Job, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := q.Query(ctx, `
SELECT `+jobSelectColumns+`
FROM jobs
WHERE status = 'queued' AND scheduled_at <= $1
ORDER BY scheduled_at ASC, priority DESC, job_id ASC
LIMIT $2`, asOf, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *j)
	}
	return out, rows.Err()
}

// ListFailedAwaitingRequeue returns failed rows past their scheduled_at retry time, still under max_attempts.
func (PostgresJobStore) ListFailedAwaitingRequeue(ctx context.Context, q Querier, asOf time.Time, limit int) ([]Job, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := q.Query(ctx, `
SELECT `+jobSelectColumns+`
FROM jobs
WHERE status = 'failed'
	AND scheduled_at <= $1
	AND attempts_made < max_attempts
ORDER BY scheduled_at ASC, priority DESC, job_id ASC
LIMIT $2`, asOf, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *j)
	}
	return out, rows.Err()
}

// MarkRequeued moves failed -> queued so DispatchEligibleJobs can lease again. Lease fields remain absent.
func (PostgresJobStore) MarkRequeued(ctx context.Context, q Querier, jobID string, expectedVersion int, updatedAt time.Time) error {
	return execOne(ctx, q, `
UPDATE jobs SET
	status = 'queued',
	updated_at = $3,
	version = version + 1
WHERE job_id = $1 AND version = $2 AND status = 'failed'`,
		jobID, expectedVersion, updatedAt)
}

// MarkLeased sets status leased and lease fields from queued.
func (PostgresJobStore) MarkLeased(ctx context.Context, q Querier, jobID string, expectedVersion int, workerID string, leaseExpiresAt, updatedAt time.Time) error {
	return execOne(ctx, q, `
UPDATE jobs SET
	status = 'leased',
	lease_owner_worker_id = $3,
	lease_expires_at = $4,
	updated_at = $5,
	version = version + 1
WHERE job_id = $1 AND version = $2 AND status = 'queued'`,
		jobID, expectedVersion, workerID, leaseExpiresAt, updatedAt)
}

// MarkRunning moves leased -> running and clears lease fields.
func (PostgresJobStore) MarkRunning(ctx context.Context, q Querier, jobID string, expectedVersion int, updatedAt time.Time) error {
	return execOne(ctx, q, `
UPDATE jobs SET
	status = 'running',
	lease_owner_worker_id = NULL,
	lease_expires_at = NULL,
	updated_at = $3,
	version = version + 1
WHERE job_id = $1 AND version = $2 AND status = 'leased'`,
		jobID, expectedVersion, updatedAt)
}

// MarkSucceeded moves running -> succeeded and clears lease fields.
func (PostgresJobStore) MarkSucceeded(ctx context.Context, q Querier, jobID string, expectedVersion int, updatedAt time.Time) error {
	return execOne(ctx, q, `
UPDATE jobs SET
	status = 'succeeded',
	lease_owner_worker_id = NULL,
	lease_expires_at = NULL,
	updated_at = $3,
	version = version + 1
WHERE job_id = $1 AND version = $2 AND status = 'running'`,
		jobID, expectedVersion, updatedAt)
}

// MarkFailedAndScheduleRetry moves running -> failed, records error metadata,
// bumps attempts_made, sets next scheduled_at, clears lease. Persistence
// semantics for retries are enforced in the scheduler service.
func (PostgresJobStore) MarkFailedAndScheduleRetry(ctx context.Context, q Querier, jobID string, expectedVersion int,
	newAttemptsMade int, lastErrorMessage, lastErrorCode *string, lastErrorRetryable *bool,
	lastFailedAt time.Time, nextScheduledAt, updatedAt time.Time) error {
	return execOne(ctx, q, `
UPDATE jobs SET
	status = 'failed',
	attempts_made = $3,
	last_error_message = $4,
	last_error_code = $5,
	last_error_retryable = $6,
	last_failed_at = $7,
	scheduled_at = $8,
	lease_owner_worker_id = NULL,
	lease_expires_at = NULL,
	updated_at = $9,
	version = version + 1
WHERE job_id = $1 AND version = $2 AND status = 'running'`,
		jobID, expectedVersion, newAttemptsMade, lastErrorMessage, lastErrorCode, lastErrorRetryable,
		lastFailedAt, nextScheduledAt, updatedAt)
}

// MarkDead moves running -> dead with terminal error metadata, clears lease.
func (PostgresJobStore) MarkDead(ctx context.Context, q Querier, jobID string, expectedVersion int,
	newAttemptsMade int, lastErrorMessage, lastErrorCode *string, lastErrorRetryable *bool,
	lastFailedAt *time.Time, updatedAt time.Time) error {
	return execOne(ctx, q, `
UPDATE jobs SET
	status = 'dead',
	attempts_made = $3,
	last_error_message = COALESCE($4, last_error_message),
	last_error_code = COALESCE($5, last_error_code),
	last_error_retryable = COALESCE($6, last_error_retryable),
	last_failed_at = COALESCE($7, last_failed_at),
	lease_owner_worker_id = NULL,
	lease_expires_at = NULL,
	updated_at = $8,
	version = version + 1
WHERE job_id = $1 AND version = $2 AND status = 'running'`,
		jobID, expectedVersion, newAttemptsMade, lastErrorMessage, lastErrorCode, lastErrorRetryable,
		lastFailedAt, updatedAt)
}

// ClearLease nulls lease columns when preconditions match (version + identity; no status guard).
func (PostgresJobStore) ClearLease(ctx context.Context, q Querier, jobID string, expectedVersion int, updatedAt time.Time) error {
	return execOne(ctx, q, `
UPDATE jobs SET
	lease_owner_worker_id = NULL,
	lease_expires_at = NULL,
	updated_at = $3,
	version = version + 1
WHERE job_id = $1 AND version = $2`,
		jobID, expectedVersion, updatedAt)
}

func (PostgresJobStore) ListLeasedPastExpiry(ctx context.Context, q Querier, asOf time.Time, limit int) ([]Job, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := q.Query(ctx, `
SELECT `+jobSelectColumns+`
FROM jobs
WHERE status = 'leased'
	AND lease_expires_at IS NOT NULL
	AND lease_expires_at < $1
ORDER BY lease_expires_at ASC, job_id ASC
LIMIT $2`, asOf, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *j)
	}
	return out, rows.Err()
}

func (PostgresJobStore) MarkLeasedExpiredToQueued(ctx context.Context, q Querier, jobID string, expectedVersion int, asOf time.Time, updatedAt time.Time) error {
	return execOne(ctx, q, `
UPDATE jobs SET
	status = 'queued',
	lease_owner_worker_id = NULL,
	lease_expires_at = NULL,
	updated_at = $4,
	version = version + 1
WHERE job_id = $1 AND version = $2 AND status = 'leased'
	AND lease_expires_at IS NOT NULL AND lease_expires_at < $3`,
		jobID, expectedVersion, asOf, updatedAt)
}

func (PostgresJobStore) ListRunningJobs(ctx context.Context, q Querier, limit int) ([]Job, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := q.Query(ctx, `
SELECT `+jobSelectColumns+`
FROM jobs
WHERE status = 'running'
ORDER BY updated_at ASC, job_id ASC
LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *j)
	}
	return out, rows.Err()
}

func (PostgresJobStore) MarkRunningAbandonedToQueued(ctx context.Context, q Querier, jobID string, expectedVersion int, scheduledAt, updatedAt time.Time) error {
	return execOne(ctx, q, `
UPDATE jobs SET
	status = 'queued',
	scheduled_at = $3,
	updated_at = $4,
	version = version + 1
WHERE job_id = $1 AND version = $2 AND status = 'running'`,
		jobID, expectedVersion, scheduledAt, updatedAt)
}

func (PostgresJobStore) ListFailedRetryableForCoordination(ctx context.Context, q Querier, limit int) ([]Job, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := q.Query(ctx, `
SELECT `+jobSelectColumns+`
FROM jobs
WHERE status = 'failed'
	AND attempts_made < max_attempts
	AND last_error_retryable = true
ORDER BY scheduled_at ASC, job_id ASC
LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *j)
	}
	return out, rows.Err()
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanJob(row rowScanner) (*Job, error) {
	var j Job
	var status string
	err := row.Scan(
		&j.JobID,
		&j.Queue,
		&j.Type,
		&j.Payload,
		&j.Priority,
		&status,
		&j.AttemptsMade,
		&j.MaxAttempts,
		&j.CreatedAt,
		&j.UpdatedAt,
		&j.ScheduledAt,
		&j.LeaseOwnerWorkerID,
		&j.LeaseExpiresAt,
		&j.LastErrorMessage,
		&j.LastErrorCode,
		&j.LastErrorRetryable,
		&j.LastFailedAt,
		&j.IdempotencyKey,
		&j.Version,
	)
	if err != nil {
		return nil, err
	}
	j.Status = JobStatus(status)
	return &j, nil
}

func execOne(ctx context.Context, q Querier, sql string, args ...any) error {
	tag, err := q.Exec(ctx, sql, args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrPreconditionFailed
	}
	return nil
}
