// Demo worker (non-production): runs demo job payloads with the same worker/lease/heartbeat model as production.
// Use WORKER_ID=dispatcher-placeholder to match default service dispatch lease owner.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"scheduler/internal/db"
	"scheduler/internal/demo"
	"scheduler/internal/queue"
	"scheduler/internal/queueredis"
	"scheduler/internal/runtime"
	"scheduler/internal/scheduler"
	"scheduler/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("TEST_DATABASE_URL")
	}
	if dsn == "" {
		log.Error("DATABASE_URL required")
		os.Exit(1)
	}
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "127.0.0.1:6379"
	}
	stream := os.Getenv("REDIS_STREAM")
	if stream == "" {
		stream = "scheduler:jobs"
	}
	group := os.Getenv("REDIS_GROUP")
	if group == "" {
		group = "workers"
	}
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		workerID = "dispatcher-placeholder"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Open(ctx, db.Config{DataSourceName: dsn})
	if err != nil {
		log.Error("db", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Error("redis", "err", err)
		os.Exit(1)
	}
	defer rdb.Close()

	q, err := queueredis.NewRedisStreamQueue(ctx, rdb, stream, group)
	if err != nil {
		log.Error("queue", "err", err)
		os.Exit(1)
	}

	jobs := store.NewPostgresJobStore()
	life := scheduler.NewJobLifecycle(pool, jobs, store.NewPostgresAuditStore())
	coord := runtime.NewCoordinator(q, life)

	hb := queueredis.NewRedisHeartbeatStore(rdb)
	lease := queueredis.NewRedisLeaseStore(rdb)
	w := queue.NewWorker(q, workerID, 2, hb, time.Second, 3*time.Second, lease, 400*time.Millisecond, 2*time.Second)
	w.SetObservability(log, nil)
	w.Hooks = coord.WorkerLifecycleHooks("demo-worker", nil)

	go w.StartHeartbeat(ctx)

	handler := func(ctx context.Context, msg queue.JobMessage) error {
		if msg.Type != demo.JobTypeDemo {
			return nil
		}
		return demo.ExecuteDemoJob(ctx, pool, jobs, rdb, msg)
	}

	log.Info("demo-worker started", "worker_id", workerID, "stream", stream, "group", group)

	for ctx.Err() == nil {
		msgs, err := w.Claim(ctx, 1, 2*time.Second)
		if err != nil {
			log.Error("claim", "err", err)
			time.Sleep(time.Second)
			continue
		}
		for _, m := range msgs {
			if err := w.ExecuteOne(ctx, m, handler); err != nil {
				log.Debug("execute", "job_id", m.Job.JobID, "err", err)
			}
		}
	}
}
