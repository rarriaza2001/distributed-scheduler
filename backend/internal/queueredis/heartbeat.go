package queueredis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

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
	value := strconv.FormatInt(time.Now().UnixMilli(), 10)
	return s.client.Set(ctx, key, value, ttl).Err()
}
