package queueredis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisLeaseStore struct {
	client redis.UniversalClient
	prefix string
}

func NewRedisLeaseStore(client redis.UniversalClient) *RedisLeaseStore {
	return &RedisLeaseStore{
		client: client,
		prefix: "job",
	}
}

func (s *RedisLeaseStore) Acquire(ctx context.Context, jobID string, workerID string, ttl time.Duration) error {
	key := fmt.Sprintf("%s:%s:lease", s.prefix, jobID)
	return s.client.Set(ctx, key, workerID, ttl).Err()
}

func (s *RedisLeaseStore) Renew(ctx context.Context, jobID string, workerID string, ttl time.Duration) error {
	key := fmt.Sprintf("%s:%s:lease", s.prefix, jobID)
	return s.client.Set(ctx, key, workerID, ttl).Err()
}

func (s *RedisLeaseStore) Release(ctx context.Context, jobID string, workerID string) error {
	_ = workerID
	key := fmt.Sprintf("%s:%s:lease", s.prefix, jobID)
	return s.client.Del(ctx, key).Err()
}

func (s *RedisLeaseStore) IsActive(ctx context.Context, jobID string) (bool, error) {
	key := fmt.Sprintf("%s:%s:lease", s.prefix, jobID)
	n, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}
