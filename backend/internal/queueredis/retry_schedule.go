package queueredis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RetrySchedule is non-authoritative Redis bookkeeping for delayed retries (sorted set).
// The DB jobs row remains the source of truth; this set is reconciled against DB.
type RetrySchedule struct {
	client redis.UniversalClient
	key    string
}

// NewRetrySchedule builds a coordinator. If key is empty, uses scheduler:retry:due.
func NewRetrySchedule(client redis.UniversalClient, key string) *RetrySchedule {
	if key == "" {
		key = "scheduler:retry:due"
	}
	return &RetrySchedule{client: client, key: key}
}

// ScheduleRetry records jobID with score = retry instant (ms). Overwrites prior score.
func (r *RetrySchedule) ScheduleRetry(ctx context.Context, jobID string, at time.Time) error {
	return r.client.ZAdd(ctx, r.key, redis.Z{
		Score:  float64(at.UnixMilli()),
		Member: jobID,
	}).Err()
}

// IsScheduled reports whether jobID currently has an entry in the set.
func (r *RetrySchedule) IsScheduled(ctx context.Context, jobID string) (bool, error) {
	_, err := r.client.ZScore(ctx, r.key, jobID).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// RemoveRetry drops jobID from the set (safe if absent).
func (r *RetrySchedule) RemoveRetry(ctx context.Context, jobID string) error {
	return r.client.ZRem(ctx, r.key, jobID).Err()
}

// ListScheduledJobIDs returns up to limit members by ascending score (earliest retry first).
func (r *RetrySchedule) ListScheduledJobIDs(ctx context.Context, limit int64) ([]string, error) {
	if limit <= 0 {
		limit = 200
	}
	// ZRANGE by rank: lowest scores first for delayed retry semantics.
	return r.client.ZRange(ctx, r.key, 0, limit-1).Result()
}
