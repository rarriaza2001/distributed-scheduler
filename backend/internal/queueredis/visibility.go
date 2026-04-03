package queueredis

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// StreamVisibility exposes cheap Redis-backed gauges for dashboards. All values are
// non-authoritative hints; Postgres remains source of truth for job state.

// ApproximateQueueDepth returns XLEN on the stream.
//
// This counts all messages retained in the stream (including pending and already-delivered
// history depending on trimming policy). It is not identical to "jobs waiting to be claimed"
// or unacked PEL depth. Use alongside PendingGroupCount for pressure signals.
func ApproximateQueueDepth(ctx context.Context, client redis.UniversalClient, stream string) (int64, error) {
	return client.XLen(ctx, stream).Result()
}

// PendingGroupCount returns the total number of pending messages for the consumer group
// (XPENDING summary). This is the PEL size — deliveries read but not yet ACKed.
func PendingGroupCount(ctx context.Context, client redis.UniversalClient, stream, group string) (int64, error) {
	p, err := client.XPending(ctx, stream, group).Result()
	if err != nil {
		return 0, err
	}
	return int64(p.Count), nil
}

// LeaderValue returns the raw scheduler leader lease value if present (debug/visibility only).
func LeaderValue(ctx context.Context, client redis.UniversalClient) (string, error) {
	return client.Get(ctx, LeaderLeaseKey).Result()
}

// LeaderTTLSeconds returns approximate remaining TTL of the leader key, or -1 if missing.
func LeaderTTLSeconds(ctx context.Context, client redis.UniversalClient) (float64, error) {
	d, err := client.TTL(ctx, LeaderLeaseKey).Result()
	if err != nil {
		return 0, err
	}
	return d.Seconds(), nil
}
