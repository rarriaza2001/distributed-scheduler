package queue

import (
	"context"
	"time"
)

// HeartbeatStore records worker process liveness only. It is not job progress
// and does not by itself protect a claimed delivery from reassignment.
type HeartbeatStore interface {
	Beat(ctx context.Context, workerID string, ttl time.Duration) error
}
