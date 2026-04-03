// Service entrypoint: HTTP metrics/health/debug + optional scheduler leader loop (Phase 4).
// Does not replace cmd/main benchmark; workers are typically separate processes using the same libraries.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"scheduler/internal/db"
	"scheduler/internal/observability"
	"scheduler/internal/queueredis"
	"scheduler/internal/runtime"
	"scheduler/internal/scheduler"
	"scheduler/internal/store"
)

func main() {
	ctx := context.Background()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("TEST_DATABASE_URL")
	}
	if dsn == "" {
		log.Error("DATABASE_URL is required")
		os.Exit(1)
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "127.0.0.1:6379"
	}
	schedID := os.Getenv("SCHEDULER_ID")
	if schedID == "" {
		schedID = "scheduler-1"
	}
	stream := os.Getenv("REDIS_STREAM")
	if stream == "" {
		stream = "scheduler:jobs"
	}
	group := os.Getenv("REDIS_GROUP")
	if group == "" {
		group = "workers"
	}
	httpAddr := os.Getenv("HTTP_ADDR")
	if httpAddr == "" {
		httpAddr = ":8080"
	}

	pool, err := db.Open(ctx, db.Config{DataSourceName: dsn})
	if err != nil {
		log.Error("db open", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Error("redis ping", "err", err)
		os.Exit(1)
	}
	defer rdb.Close()

	reg := prometheus.NewRegistry()
	m := observability.NewMetrics(reg)

	q, err := queueredis.NewRedisStreamQueue(ctx, rdb, stream, group)
	if err != nil {
		log.Error("queue init", "err", err)
		os.Exit(1)
	}

	jobs := store.NewPostgresJobStore()
	audit := store.NewPostgresAuditStore()
	life := scheduler.NewJobLifecycle(pool, jobs, audit)
	failed := observability.NewFailedRecentStore(rdb, observability.DefaultFailedRecentMax)
	life.SetObservability(m, log, failed)

	coord := runtime.NewCoordinator(q, life)
	sid := schedID
	coord.SchedulerID = &sid

	retryCoord := queueredis.NewRetrySchedule(rdb, "")
	jobLease := queueredis.NewRedisLeaseStore(rdb)
	rec := scheduler.NewReconciler(pool, jobs, audit)
	rec.SetObservability(m, log)

	leaderLease := queueredis.NewRedisLeaderLease(rdb)
	leaderCfg := scheduler.SchedulerLeaderConfig{
		LeaseTTLSeconds:  5,
		RenewInterval:    2 * time.Second,
		AcquireInterval:  time.Second,
		ScanInterval:     250 * time.Millisecond,
		ScanLimitPerTick: 100,
	}
	leader := scheduler.NewSchedulerLeader(schedID, leaderLease, nil, leaderCfg)
	leader.SetObservability(m, log)

	leader.ReconcileOnTick = func(ctx context.Context) error {
		now := time.Now().UTC()
		return rec.Reconcile(ctx, scheduler.ReconcileOptions{
			LeaderAllowed: true,
			Now:           now,
			RetryCoord:    retryCoord,
			JobLease:      scheduler.LeaseProbeFunc(jobLease.IsActive),
			SchedulerID:   &sid,
			Source:        "scheduler_recovery",
		})
	}
	leader.DispatchOnTick = func(ctx context.Context) error {
		now := time.Now().UTC()
		_, err := coord.Dispatch(ctx, runtime.CoordinatorDispatchOptions{
			LeaderAllowed: true,
			AsOf:          now,
			Limit:         50,
			Now:           now,
			AssignLease: func(job store.Job) (string, time.Time, error) {
				_ = job
				return "dispatcher-placeholder", now.Add(10 * time.Minute), nil
			},
		})
		return err
	}

	hb := queueredis.NewRedisHeartbeatStore(rdb)
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				d, err := queueredis.ApproximateQueueDepth(ctx, rdb, stream)
				if err == nil {
					m.SetQueueDepth(float64(d))
				}
				n, err := hb.CountAliveAfterPrune(ctx, 45*time.Second)
				if err == nil {
					m.SetWorkersAlive(float64(n))
				}
			}
		}
	}()

	go leader.Start(context.Background())

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/debug/failed-jobs/recent", func(w http.ResponseWriter, r *http.Request) {
		limit := 50
		if s := r.URL.Query().Get("limit"); s != "" {
			if n, err := strconv.Atoi(s); err == nil && n > 0 {
				limit = n
			}
		}
		list, err := failed.ListRecent(r.Context(), limit)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(list)
	})

	log.Info("service listening", "addr", httpAddr)
	if err := http.ListenAndServe(httpAddr, mux); err != nil {
		log.Error("http server", "err", err)
		os.Exit(1)
	}
}
