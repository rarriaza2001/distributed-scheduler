package db

import "time"

// Config holds pgxpool settings. DataSourceName is a Postgres connection string
// suitable for pgxpool.ParseConfig (URI or DSN keyword/value form).
type Config struct {
	DataSourceName    string
	MaxConns          int32
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
}
