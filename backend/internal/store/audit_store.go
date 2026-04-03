package store

import (
	"context"
)

// AuditStore appends execution_audit rows. No lifecycle policy.
type AuditStore interface {
	AppendEvent(ctx context.Context, q Querier, in AuditEventInput) error
}

// PostgresAuditStore implements AuditStore.
type PostgresAuditStore struct{}

// NewPostgresAuditStore returns an AuditStore backed by Postgres.
func NewPostgresAuditStore() *PostgresAuditStore {
	return &PostgresAuditStore{}
}

var _ AuditStore = (*PostgresAuditStore)(nil)

// AppendEvent inserts one audit event. details defaults to {} when empty.
func (PostgresAuditStore) AppendEvent(ctx context.Context, q Querier, in AuditEventInput) error {
	details := in.Details
	if len(details) == 0 {
		details = []byte("{}")
	}
	_, err := q.Exec(ctx, `
INSERT INTO execution_audit (
	event_id, job_id, worker_id, scheduler_id, source, type, occurred_at, details
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		in.EventID,
		in.JobID,
		in.WorkerID,
		in.SchedulerID,
		in.Source,
		in.Type,
		in.OccurredAt,
		details,
	)
	return err
}
