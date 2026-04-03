package app

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// ServiceConfig holds env-driven settings for the Phase 4 service. Validate before use.
type ServiceConfig struct {
	DatabaseURL string
	RedisAddr   string
	RedisStream string
	RedisGroup  string
	HTTPAddr    string
	SchedulerID string

	// QueueAlivePollInterval updates scheduler_queue_depth and scheduler_workers_alive gauges.
	QueueAlivePollInterval time.Duration
	// WorkersAliveStale is the max age for heartbeat scores before prune (see heartbeat store).
	WorkersAliveStale time.Duration
	// FailedRecentMax caps Redis list scheduler:failed:recent (1..2000).
	FailedRecentMax int
	// HTTPShutdownTimeout bounds graceful server shutdown.
	HTTPShutdownTimeout time.Duration
	// LeaderLeaseTTLSeconds is passed to SchedulerLeaderConfig.
	LeaderLeaseTTLSeconds int
	// AllowDemoEndpoints enables POST /debug/demo/seed (non-production).
	AllowDemoEndpoints bool
}

const (
	defaultRedisStream       = "scheduler:jobs"
	defaultRedisGroup        = "workers"
	defaultHTTPAddr          = ":8080"
	defaultSchedulerID       = "scheduler-1"
	defaultQueueAlivePoll    = 10 * time.Second
	defaultWorkersAliveStale = 45 * time.Second
	defaultFailedRecentMax   = 300
	defaultHTTPShutdown      = 15 * time.Second
	defaultLeaderLeaseTTL    = 5
)

// LoadConfigFromEnv reads configuration from environment variables with documented defaults.
func LoadConfigFromEnv() ServiceConfig {
	c := ServiceConfig{
		DatabaseURL:           firstNonEmpty(os.Getenv("DATABASE_URL"), os.Getenv("TEST_DATABASE_URL")),
		RedisAddr:             os.Getenv("REDIS_ADDR"),
		RedisStream:           os.Getenv("REDIS_STREAM"),
		RedisGroup:            os.Getenv("REDIS_GROUP"),
		HTTPAddr:              os.Getenv("HTTP_ADDR"),
		SchedulerID:           os.Getenv("SCHEDULER_ID"),
		QueueAlivePollInterval: durationEnv("QUEUE_ALIVE_POLL_INTERVAL", defaultQueueAlivePoll),
		WorkersAliveStale:     durationEnv("WORKERS_ALIVE_STALE", defaultWorkersAliveStale),
		FailedRecentMax:       intEnv("FAILED_RECENT_MAX", defaultFailedRecentMax),
		HTTPShutdownTimeout:   durationEnv("HTTP_SHUTDOWN_TIMEOUT", defaultHTTPShutdown),
		LeaderLeaseTTLSeconds: intEnv("LEADER_LEASE_TTL_SECONDS", defaultLeaderLeaseTTL),
		AllowDemoEndpoints:    os.Getenv("ALLOW_DEMO_ENDPOINTS") == "1" || os.Getenv("ALLOW_DEMO_ENDPOINTS") == "true",
	}
	if c.RedisAddr == "" {
		c.RedisAddr = "127.0.0.1:6379"
	}
	if c.RedisStream == "" {
		c.RedisStream = defaultRedisStream
	}
	if c.RedisGroup == "" {
		c.RedisGroup = defaultRedisGroup
	}
	if c.HTTPAddr == "" {
		c.HTTPAddr = defaultHTTPAddr
	}
	if c.SchedulerID == "" {
		c.SchedulerID = defaultSchedulerID
	}
	return c
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func durationEnv(key string, def time.Duration) time.Duration {
	s := os.Getenv(key)
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return def
	}
	return d
}

func intEnv(key string, def int) int {
	s := os.Getenv(key)
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

// Validate returns an error if configuration is unusable.
func (c ServiceConfig) Validate() error {
	if c.DatabaseURL == "" {
		return errors.New("app: DATABASE_URL (or TEST_DATABASE_URL) is required")
	}
	if c.RedisAddr == "" {
		return errors.New("app: REDIS_ADDR is invalid")
	}
	if c.HTTPAddr == "" {
		return errors.New("app: HTTP_ADDR is invalid")
	}
	if c.QueueAlivePollInterval <= 0 {
		return fmt.Errorf("app: QUEUE_ALIVE_POLL_INTERVAL must be positive, got %v", c.QueueAlivePollInterval)
	}
	if c.WorkersAliveStale <= 0 {
		return fmt.Errorf("app: WORKERS_ALIVE_STALE must be positive, got %v", c.WorkersAliveStale)
	}
	if c.FailedRecentMax < 1 || c.FailedRecentMax > 2000 {
		return fmt.Errorf("app: FAILED_RECENT_MAX must be in [1,2000], got %d", c.FailedRecentMax)
	}
	if c.HTTPShutdownTimeout <= 0 {
		return fmt.Errorf("app: HTTP_SHUTDOWN_TIMEOUT must be positive, got %v", c.HTTPShutdownTimeout)
	}
	if c.LeaderLeaseTTLSeconds <= 0 {
		return fmt.Errorf("app: LEADER_LEASE_TTL_SECONDS must be positive, got %d", c.LeaderLeaseTTLSeconds)
	}
	return nil
}
