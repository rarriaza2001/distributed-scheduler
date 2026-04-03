# Demo runbook (60–90 seconds)

**Non-production.** Demo jobs use `type=demo` and JSON payloads interpreted only by `cmd/demo-worker`.

## Prerequisites

- Postgres reachable (`DATABASE_URL`).
- Redis reachable (`REDIS_ADDR`).
- Migrations applied (jobs table).
- Same `REDIS_STREAM` / `REDIS_GROUP` for service, producer, and worker (defaults: `scheduler:jobs` / `workers`).

## 1. Start the service (scheduler + HTTP)

From `backend/`:

```bash
set DATABASE_URL=postgres://...
set REDIS_ADDR=127.0.0.1:6379
set ALLOW_DEMO_ENDPOINTS=1
go run ./cmd/service
```

Open:

- `http://localhost:8080/metrics`
- `http://localhost:8080/healthz`

## 2. Start one or more demo workers

The service dispatches leased work to worker id **`dispatcher-placeholder`** by default. Run the demo worker with that id:

```bash
set DATABASE_URL=postgres://...
set REDIS_ADDR=127.0.0.1:6379
set WORKER_ID=dispatcher-placeholder
go run ./cmd/demo-worker
```

For a second consumer identity, change **both** service dispatch `AssignLease` (production wiring) and `WORKER_ID` — the stock service still uses the placeholder id for the demo.

## 3. Seed jobs

**Option A — CLI**

```bash
go run ./cmd/demo-producer -preset=mixed-demo
```

Presets:

| Preset | Behavior |
|--------|----------|
| `small-demo` | ~12s sleep job. |
| `retry-demo` | Fails once (`demo_retryable_failure`) then succeeds on retry. |
| `abandonment-demo` | After ~1.5s deletes Redis per-job lease; recovery may requeue (watch `scheduler_jobs_abandoned_total` / reconciler). |
| `mixed-demo` | Several jobs: fast, slow, retry, abandon-style. |

**Option B — HTTP** (requires `ALLOW_DEMO_ENDPOINTS=1`)

```bash
curl -X POST http://localhost:8080/debug/demo/seed -H "Content-Type: application/json" -d "{\"preset\":\"mixed-demo\"}"
```

## 4. What to watch (timeline)

Within **~30–60s** you should see:

1. **Queue depth** (`scheduler_queue_depth`) move as jobs enqueue and drain.
2. **Completed** (`rate(scheduler_jobs_completed_total[1m])`) increase as demo sleeps finish.
3. **Retries** (`scheduler_job_retries_total{reason="execution_failure"}`) on `retry-demo` / mixed retry job after first failure.
4. **Heartbeats** (`scheduler_worker_heartbeat_total`) if worker is running (demo-worker ticks heartbeats).
5. **Workers alive** (`scheduler_workers_alive`) ≥ 1 while demo-worker is up.
6. **Abandonment** (abandon preset): lease deleted mid-run → eventual recovery activity (metrics/logs; exact timing depends on reconciler tick).

## 5. Stopping

Send `Ctrl+C` to the service: HTTP shuts down gracefully, leader loop and poller stop, Redis/DB close.

## Assumptions

- Demo payloads are **not** validated for production invariants beyond normal job rows.
- Abandonment visibility depends on reconciler + Redis lease probe timing; allow **up to a few tick intervals** after the scripted lease delete.
