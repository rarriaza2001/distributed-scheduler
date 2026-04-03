package app

import (
	"log/slog"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"scheduler/internal/observability"
	"scheduler/internal/queueredis"
	"scheduler/internal/runtime"
	"scheduler/internal/scheduler"
)

// Deps holds wired runtime dependencies for the service process.
type Deps struct {
	Config ServiceConfig
	Log    *slog.Logger

	Pool   *pgxpool.Pool
	Redis  *redis.Client
	Reg    *prometheus.Registry
	Metric *observability.Metrics

	Queue          *queueredis.RedisStreamQueue
	FailedRecent   *observability.FailedRecentStore
	Lifecycle      *scheduler.JobLifecycle
	Coordinator    *runtime.Coordinator
	Reconciler     *scheduler.Reconciler
	Leader         *scheduler.SchedulerLeader
	RetrySchedule  *queueredis.RetrySchedule
	JobLease       *queueredis.RedisLeaseStore
	LeaderLease    *queueredis.RedisLeaderLease
	HeartbeatStore *queueredis.RedisHeartbeatStore

	SchedulerID string
	Stream      string
	Group       string
}

// HTTPServer wraps the HTTP server for graceful shutdown.
type HTTPServer struct {
	Inner *http.Server
	Mux   *http.ServeMux
}
