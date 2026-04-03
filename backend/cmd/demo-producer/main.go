// Demo producer CLI: inserts seeded demo jobs via Coordinator (non-production).
// Usage: go run ./cmd/demo-producer -preset=mixed-demo
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"

	"github.com/redis/go-redis/v9"

	"scheduler/internal/db"
	"scheduler/internal/demo"
	"scheduler/internal/queueredis"
	"scheduler/internal/runtime"
	"scheduler/internal/scheduler"
	"scheduler/internal/store"
)

func main() {
	presetFlag := flag.String("preset", "small-demo", "one of: small-demo, retry-demo, abandonment-demo, mixed-demo")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	p := demo.PresetFromString(*presetFlag)
	if p == demo.PresetUnknown {
		log.Error("unknown preset", "value", *presetFlag)
		os.Exit(1)
	}

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

	ctx := context.Background()
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

	life := scheduler.NewJobLifecycle(pool, store.NewPostgresJobStore(), store.NewPostgresAuditStore())
	coord := runtime.NewCoordinator(q, life)

	n, err := demo.Seed(ctx, coord, p)
	if err != nil {
		log.Error("seed", "err", err)
		os.Exit(1)
	}
	log.Info("seeded", "preset", *presetFlag, "jobs", n)
}
