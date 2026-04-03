package queueredis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// LeaderLeaseKey is the Redis key used for scheduler leader election integration tests
// and administrative cleanup.
const LeaderLeaseKey = "scheduler:leader"

// RedisLeaderLease implements scheduler.LeaderLease using a single Redis key and TTL.
// This is a soft leadership mechanism, not consensus: brief split-brain windows
// remain possible under faults, and correctness still relies on idempotent job
// handling and at-least-once semantics.
type RedisLeaderLease struct {
	client redis.UniversalClient
}

func NewRedisLeaderLease(client redis.UniversalClient) *RedisLeaderLease {
	return &RedisLeaderLease{client: client}
}

// Acquire tries to become leader using SET NX PX. ttlSeconds should be a small
// number of seconds (for example, 5); it is converted to milliseconds for Redis.
func (l *RedisLeaderLease) Acquire(ctx context.Context, id string, ttlSeconds int) (bool, error) {
	if ttlSeconds <= 0 {
		ttlSeconds = 5
	}
	ttl := time.Duration(ttlSeconds) * time.Second

	ok, err := l.client.SetNX(ctx, LeaderLeaseKey, id, ttl).Result()
	if err != nil {
		return false, err
	}
	return ok, nil
}

// Renew extends the TTL only if this scheduler still owns the lease. It uses a
// small Lua script for an atomic compare-and-pexpire.
func (l *RedisLeaderLease) Renew(ctx context.Context, id string, ttlSeconds int) (bool, error) {
	if ttlSeconds <= 0 {
		ttlSeconds = 5
	}
	ttlMs := ttlSeconds * 1000

	script := redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("PEXPIRE", KEYS[1], ARGV[2])
		else
			return 0
		end
	`)

	res, err := script.Run(ctx, l.client, []string{LeaderLeaseKey}, id, ttlMs).Result()
	if err != nil {
		return false, err
	}

	n, ok := res.(int64)
	if !ok {
		return false, nil
	}
	return n == 1, nil
}

// Release deletes the leadership key only if this scheduler still owns it.
// This is an optimization only; correctness relies on TTL expiry.
func (l *RedisLeaderLease) Release(ctx context.Context, id string) (bool, error) {
	script := redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`)

	res, err := script.Run(ctx, l.client, []string{LeaderLeaseKey}, id).Result()
	if err != nil {
		return false, err
	}

	n, ok := res.(int64)
	if !ok {
		return false, nil
	}
	return n == 1, nil
}
