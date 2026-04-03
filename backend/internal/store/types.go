package store

import (
	"time"
)

// JobStatus is the persisted jobs.status value.
type JobStatus string

const (
	JobStatusQueued    JobStatus = "queued"
	JobStatusLeased    JobStatus = "leased"
	JobStatusRunning   JobStatus = "running"
	JobStatusSucceeded JobStatus = "succeeded"
	JobStatusFailed    JobStatus = "failed"
	JobStatusDead      JobStatus = "dead"
)

// Job is the full jobs row. Optional fields use pointers for SQL NULL.
type Job struct {
	JobID                string
	Queue                string
	Type                 string
	Payload              []byte
	Priority             int
	Status               JobStatus
	AttemptsMade         int
	MaxAttempts          int
	CreatedAt            time.Time
	UpdatedAt            time.Time
	ScheduledAt          time.Time
	LeaseOwnerWorkerID   *string
	LeaseExpiresAt       *time.Time
	LastErrorMessage     *string
	LastErrorCode        *string
	LastErrorRetryable   *bool
	LastFailedAt         *time.Time
	IdempotencyKey       *string
	Version              int
}

// InsertJobInput is an initial insert for a producer-created job (status queued).
type InsertJobInput struct {
	JobID          string
	Queue          string
	Type           string
	Payload        []byte
	Priority       int
	MaxAttempts    int
	ScheduledAt    time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
	IdempotencyKey *string
}

// AuditEventInput is one row in execution_audit.
type AuditEventInput struct {
	EventID      string
	JobID        *string
	WorkerID     *string
	SchedulerID  *string
	Source       string
	Type         string
	OccurredAt   time.Time
	Details      []byte // JSON object; nil or empty -> '{}'
}
