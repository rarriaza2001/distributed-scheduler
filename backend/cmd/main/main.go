// This entrypoint runs the Redis-only benchmark. Full DB + Redis job flow lives in
// internal/runtime (Coordinator + scheduler.JobLifecycle) and should be wired from
// db.Open, queueredis.NewRedisStreamQueue, and scheduler.SchedulerLeader.DispatchOnTick.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"

	"scheduler/internal/queue"
	"scheduler/internal/queueredis"
)

func NewRedisQueueForDemo(ctx context.Context, client redis.UniversalClient) (*queueredis.RedisStreamQueue, error) {
	q, err := queueredis.NewRedisStreamQueue(ctx, client, "test:jobs", "test:workers")
	if err != nil {
		return nil, err
	}
	return q, nil
}

func main() {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	q, err := queueredis.NewRedisStreamQueue(ctx, client, "bench:jobs", "bench:workers")
	if err != nil {
		fmt.Printf("queue init failed: %v", err)
	}

	cfg := queue.BenchmarkConfig{
		JobCount:    10000,
		WorkerCount: 4,
		ClaimBatch:  10,
		HandlerMode: queue.NoOp,
		FixedDelay:  2 * time.Millisecond,
	}

	if err := queue.RunBenchmark(ctx, q, cfg); err != nil {
		log.Fatal(err)
	}
}