package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"scheduler/internal/db"
	"scheduler/internal/observability"
	"scheduler/internal/queueredis"
	"scheduler/internal/runtime"
	"scheduler/internal/scheduler"
	"scheduler/internal/store"
)

// Bootstrap connects to Postgres and Redis, builds queues, lifecycle, reconciler, and scheduler leader.
// Caller must Close pool and Redis on shutdown.
func Bootstrap(ctx context.Context, cfg ServiceConfig, log *slog.Logger) (*Deps, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	pool, err := db.Open(ctx, db.Config{DataSourceName: cfg.DatabaseURL})
	if err != nil {
		return nil, fmt.Errorf("bootstrap db: %w", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		pool.Close()
		return nil, fmt.Errorf("bootstrap redis: %w", err)
	}

	reg := prometheus.NewRegistry()
	m := observability.NewMetrics(reg)

	q, err := queueredis.NewRedisStreamQueue(ctx, rdb, cfg.RedisStream, cfg.RedisGroup)
	if err != nil {
		rdb.Close()
		pool.Close()
		return nil, fmt.Errorf("bootstrap queue: %w", err)
	}

	jobs := store.NewPostgresJobStore()
	audit := store.NewPostgresAuditStore()
	life := scheduler.NewJobLifecycle(pool, jobs, audit)
	failed := observability.NewFailedRecentStore(rdb, cfg.FailedRecentMax)
	life.SetObservability(m, log, failed)

	coord := runtime.NewCoordinator(q, life)
	sid := cfg.SchedulerID
	coord.SchedulerID = &sid

	retryCoord := queueredis.NewRetrySchedule(rdb, "")
	jobLease := queueredis.NewRedisLeaseStore(rdb)
	rec := scheduler.NewReconciler(pool, jobs, audit)
	rec.SetObservability(m, log)

	leaderLease := queueredis.NewRedisLeaderLease(rdb)
	leaderCfg := scheduler.SchedulerLeaderConfig{
		LeaseTTLSeconds:  cfg.LeaderLeaseTTLSeconds,
		RenewInterval:    2 * time.Second,
		AcquireInterval:  time.Second,
		ScanInterval:     250 * time.Millisecond,
		ScanLimitPerTick: 100,
	}
	leader := scheduler.NewSchedulerLeader(cfg.SchedulerID, leaderLease, nil, leaderCfg)
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

	return &Deps{
		Config:         cfg,
		Log:            log,
		Pool:           pool,
		Redis:          rdb,
		Reg:            reg,
		Metric:         m,
		Queue:          q,
		FailedRecent:   failed,
		Lifecycle:      life,
		Coordinator:    coord,
		Reconciler:     rec,
		Leader:         leader,
		RetrySchedule:  retryCoord,
		JobLease:       jobLease,
		LeaderLease:    leaderLease,
		HeartbeatStore: hb,
		SchedulerID:    sid,
		Stream:         cfg.RedisStream,
		Group:          cfg.RedisGroup,
	}, nil
}
