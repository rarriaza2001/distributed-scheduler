package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Open builds a single shared pool from cfg, verifies connectivity with Ping,
// and returns it for the process lifetime. Call Close on shutdown.
//
// Uses pgxpool.ParseConfig + pgxpool.NewWithConfig; does not create per-query pools.
func Open(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	if cfg.DataSourceName == "" {
		return nil, fmt.Errorf("db: DataSourceName is required")
	}

	pc, err := pgxpool.ParseConfig(cfg.DataSourceName)
	if err != nil {
		return nil, fmt.Errorf("db: parse config: %w", err)
	}

	if cfg.MaxConns > 0 {
		pc.MaxConns = cfg.MaxConns
	}
	if cfg.MinConns > 0 {
		pc.MinConns = cfg.MinConns
	}
	if cfg.MaxConnLifetime > 0 {
		pc.MaxConnLifetime = cfg.MaxConnLifetime
	}
	if cfg.MaxConnIdleTime > 0 {
		pc.MaxConnIdleTime = cfg.MaxConnIdleTime
	}
	if cfg.HealthCheckPeriod > 0 {
		pc.HealthCheckPeriod = cfg.HealthCheckPeriod
	}

	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("db: new pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db: ping: %w", err)
	}

	return pool, nil
}
