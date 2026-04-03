package queueredis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrLeaseHeldByOther is returned when Acquire or Renew cannot claim because another worker owns the lease.
var ErrLeaseHeldByOther = errors.New("redis: job lease held by another worker")

type RedisLeaseStore struct {
	client redis.UniversalClient
	prefix string

	acquireScript *redis.Script
	renewScript   *redis.Script
	releaseScript *redis.Script
}

func NewRedisLeaseStore(client redis.UniversalClient) *RedisLeaseStore {
	s := &RedisLeaseStore{
		client: client,
		prefix: "job",
	}
	// KEYS[1] = lease key, ARGV[1] = workerID, ARGV[2] = ttl milliseconds
	s.acquireScript = redis.NewScript(`
		local v = redis.call("GET", KEYS[1])
		if v == false then
			redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
			return 1
		end
		if v == ARGV[1] then
			redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
			return 1
		end
		return 0
	`)
	s.renewScript = redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("PEXPIRE", KEYS[1], ARGV[2])
		else
			return 0
		end
	`)
	s.releaseScript = redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`)
	return s
}

func (s *RedisLeaseStore) leaseKey(jobID string) string {
	return fmt.Sprintf("%s:%s:lease", s.prefix, jobID)
}

// Acquire sets the lease only if missing or already owned by workerID (idempotent for same worker).
func (s *RedisLeaseStore) Acquire(ctx context.Context, jobID string, workerID string, ttl time.Duration) error {
	key := s.leaseKey(jobID)
	ms := ttl.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	res, err := s.acquireScript.Run(ctx, s.client, []string{key}, workerID, ms).Result()
	if err != nil {
		return err
	}
	n, ok := res.(int64)
	if !ok {
		return fmt.Errorf("redis lease acquire: unexpected result %v", res)
	}
	if n != 1 {
		return ErrLeaseHeldByOther
	}
	return nil
}

// Renew extends TTL only if the lease value matches workerID.
func (s *RedisLeaseStore) Renew(ctx context.Context, jobID string, workerID string, ttl time.Duration) error {
	key := s.leaseKey(jobID)
	ms := ttl.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	res, err := s.renewScript.Run(ctx, s.client, []string{key}, workerID, ms).Result()
	if err != nil {
		return err
	}
	n, ok := res.(int64)
	if !ok {
		return fmt.Errorf("redis lease renew: unexpected result %v", res)
	}
	if n != 1 {
		return ErrLeaseHeldByOther
	}
	return nil
}

// Release deletes the lease key only if it matches workerID.
func (s *RedisLeaseStore) Release(ctx context.Context, jobID string, workerID string) error {
	key := s.leaseKey(jobID)
	_, err := s.releaseScript.Run(ctx, s.client, []string{key}, workerID).Result()
	return err
}

func (s *RedisLeaseStore) IsActive(ctx context.Context, jobID string) (bool, error) {
	key := s.leaseKey(jobID)
	n, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}
