package queueredis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// WorkersAliveZSetKey holds worker_id members scored by last heartbeat unix ms.
// Used for O(1)-ish aggregate alive counts without SCAN on per-worker keys.
const WorkersAliveZSetKey = "scheduler:workers:alive"

type RedisHeartbeatStore struct {
	client redis.UniversalClient
	prefix string
}

func NewRedisHeartbeatStore(client redis.UniversalClient) *RedisHeartbeatStore {
	return &RedisHeartbeatStore{
		client: client,
		prefix: "worker",
	}
}

func (s *RedisHeartbeatStore) Beat(ctx context.Context, workerID string, ttl time.Duration) error {
	key := fmt.Sprintf("%s:%s:heartbeat", s.prefix, workerID)
	nowMs := time.Now().UnixMilli()
	value := strconv.FormatInt(nowMs, 10)
	if err := s.client.Set(ctx, key, value, ttl).Err(); err != nil {
		return err
	}
	z := redis.Z{Score: float64(nowMs), Member: workerID}
	return s.client.ZAdd(ctx, WorkersAliveZSetKey, z).Err()
}

// CountAliveAfterPrune removes ZSET members with heartbeat score older than maxStale and returns ZCARD.
func (s *RedisHeartbeatStore) CountAliveAfterPrune(ctx context.Context, maxStale time.Duration) (int64, error) {
	if maxStale <= 0 {
		maxStale = 30 * time.Second
	}
	cutoff := float64(time.Now().Add(-maxStale).UnixMilli())
	max := "(" + strconv.FormatFloat(cutoff, 'f', -1, 64)
	if err := s.client.ZRemRangeByScore(ctx, WorkersAliveZSetKey, "-inf", max).Err(); err != nil {
		return 0, err
	}
	return s.client.ZCard(ctx, WorkersAliveZSetKey).Result()
}
