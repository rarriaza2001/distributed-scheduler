package queue

import (
	"context"
	"time"
)

// LeaseStore tracks per-job ownership freshness for active processing. A worker
// may be alive while a specific job's lease is stale—heartbeat alone is not enough.
//
// Abandonment (Phase 2): the delivery is still pending/unacked in Redis and IsActive(jobID) is false.
type LeaseStore interface {
	Acquire(ctx context.Context, jobID string, workerID string, ttl time.Duration) error
	Renew(ctx context.Context, jobID string, workerID string, ttl time.Duration) error
	Release(ctx context.Context, jobID string, workerID string) error
	IsActive(ctx context.Context, jobID string) (bool, error)
}
