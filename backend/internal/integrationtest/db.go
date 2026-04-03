//go:build integration

// Package integrationtest provides shared Postgres setup for integration tests.
// Tests across packages must use Pool() so schema setup and truncation are serialized.
package integrationtest

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"scheduler/internal/db"
)

var (
	dbMu   sync.Mutex
	shared *pgxpool.Pool
)

// DatabaseURL returns TEST_DATABASE_URL, DATABASE_URL, or a local default.
func DatabaseURL() string {
	u := os.Getenv("TEST_DATABASE_URL")
	if u == "" {
		u = os.Getenv("DATABASE_URL")
	}
	if u == "" {
		u = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	}
	return u
}

// Pool returns a shared pgx pool with migration 000001 applied. Serializes access
// and truncates job tables before each test for isolation.
func Pool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dbMu.Lock()
	defer dbMu.Unlock()

	ctx := context.Background()
	if shared == nil {
		p, err := db.Open(ctx, db.Config{DataSourceName: DatabaseURL()})
		if err != nil {
			t.Skipf("postgres not available: %v", err)
			return nil
		}
		applyMigration0001(t, p)
		shared = p
	}

	truncateTables(t, shared)
	return shared
}

func truncateTables(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx, `TRUNCATE TABLE execution_audit, jobs RESTART IDENTITY CASCADE`)
	require.NoError(t, err)
}

// applyMigration0001 ensures migration 000001 DDL is applied once across parallel test
// processes (go test -p N): pg_advisory_lock serializes init; other processes skip DDL if
// both tables already exist.
func applyMigration0001(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	dir := filepath.Dir(thisFile)
	path := filepath.Join(dir, "..", "..", "migrations", "000001_jobs_and_audit.up.sql")
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	ctx := context.Background()

	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()

	const lockK1 int32 = 842001
	const lockK2 int32 = 1
	_, err = conn.Exec(ctx, `SELECT pg_advisory_lock($1, $2)`, lockK1, lockK2)
	require.NoError(t, err)
	defer func() {
		_, _ = conn.Exec(ctx, `SELECT pg_advisory_unlock($1, $2)`, lockK1, lockK2)
	}()

	var jobsOK, auditOK bool
	err = conn.QueryRow(ctx, `
SELECT
  EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'jobs'),
  EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'execution_audit')`).Scan(&jobsOK, &auditOK)
	require.NoError(t, err)

	if !jobsOK || !auditOK {
		_, err = conn.Exec(ctx, `DROP TABLE IF EXISTS execution_audit CASCADE`)
		require.NoError(t, err)
		_, err = conn.Exec(ctx, `DROP TABLE IF EXISTS jobs CASCADE`)
		require.NoError(t, err)

		var sb strings.Builder
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "--") {
				continue
			}
			sb.WriteString(line)
			sb.WriteByte('\n')
		}
		for _, part := range strings.Split(sb.String(), ";") {
			stmt := strings.TrimSpace(part)
			if stmt == "" {
				continue
			}
			_, err = conn.Exec(ctx, stmt)
			require.NoError(t, err)
		}
	}
}
