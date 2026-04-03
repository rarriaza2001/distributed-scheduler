// Package queueredis provides Redis-backed implementations for queue transport, leadership
// heartbeats, per-job leases, and optional retry coordination (sorted sets).
//
// Nothing in this package interprets job lifecycle policy. Retry ZSET and stream contents are
// non-authoritative; the scheduler reconciles them against Postgres (see internal/scheduler).
package queueredis
