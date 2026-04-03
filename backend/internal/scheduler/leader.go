package scheduler

import "context"

// LeaderLease represents a TTL-based leadership claim for the scheduler control
// plane. It is intentionally a soft lease and does not provide consensus.
//
// Semantics:
//   - Acquire: best-effort attempt to become leader for a fixed TTL.
//   - Renew: extend TTL only if this scheduler still owns the lease.
//   - Release: best-effort ownership-safe delete; correctness relies on TTL.
type LeaderLease interface {
	Acquire(ctx context.Context, id string, ttlSeconds int) (bool, error)
	Renew(ctx context.Context, id string, ttlSeconds int) (bool, error)
	Release(ctx context.Context, id string) (bool, error)
}
