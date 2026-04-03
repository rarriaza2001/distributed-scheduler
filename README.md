```txt
distributed-scheduler
│
├── README.md
├── architecture
│   ├── architecture-diagram.png
│   └── system-design.md
│
├── backend
│   ├── internal
│   │   ├── app
│   │   ├── db
│   │   ├── demo
│   │   ├── integration tests
│   │   ├── observability
│   │   ├── queue
│   │   ├── scheduler
│   │   ├── store
│   │   ├── runtime
│   │   └── queueredis
│   └── cmd
│       ├── main          (Redis benchmark entrypoint)
│       ├── service
│       ├── demo-worker
│       └── demo-producer
│
├── infrastructure
│   ├── docker
│   ├── deployment
│   └── monitoring
│       └── grafana       (dashboard JSON templates)
│
├── scripts
│   ├── benchmark         (run.ps1 / run.sh → backend benchmark)
│   └── local-dev
│
├── docs
│   ├── design-decisions.md
│   ├── failure-modes.md
│   └── scaling-considerations.md
│
└── docker-compose.yml
```

## Using the project

The Go code lives under **`backend/`**. You need **Postgres** (jobs are authoritative) and **Redis** (queue + coordination). Apply DB migrations for your environment, then set `DATABASE_URL` and `REDIS_ADDR`.

| Command | Purpose |
|--------|---------|
| `go run ./cmd/service` | **service**: HTTP (`/metrics`, `/healthz`, `/debug/failed-jobs/recent`), scheduler leader loop, gauge polling. Run from **`backend/`**. |
| From repo root: `scripts/benchmark/run.ps1` or `scripts/benchmark/run.sh` | Redis **benchmark** only (no full DB lifecycle); or `go run ./cmd/main` from **`backend/`**. |

**Service** env: `DATABASE_URL`, `REDIS_ADDR`, optional `HTTP_ADDR` (default `:8080`), `SCHEDULER_ID`, `REDIS_STREAM` / `REDIS_GROUP`. Set `ALLOW_DEMO_ENDPOINTS=1` only if you want `POST /debug/demo/seed` in non-production.

**Tests:** from `backend/`, run `go test ./...`. Integration tests: `go test -tags=integration ./...` (needs Postgres + Redis). If the editor hides integration files, configure gopls with `-tags=integration` (see `.vscode/settings.json`).

## Demo (non-production)

Demo jobs use `type=demo` and JSON payloads in **`backend/internal/demo/`**. For live walkthroughs only, not production.

1. Start the **service** (`go run ./cmd/service` from `backend/`) with DB and Redis available.
2. Start a **demo worker**: `go run ./cmd/demo-worker` — set **`WORKER_ID=dispatcher-placeholder`** (default) so it matches the lease owner the stock service assigns to dispatched jobs.
3. **Seed jobs** with either:
   - **CLI:** `go run ./cmd/demo-producer -preset=mixed-demo` (also `small-demo`, `retry-demo`, `abandonment-demo`), or
   - **HTTP** (if `ALLOW_DEMO_ENDPOINTS=1`): `POST /debug/demo/seed` with body `{"preset":"mixed-demo"}`.

Inspect **`/metrics`** and import Grafana dashboards from **`infrastructure/monitoring/grafana/`**. Timeline and what to expect: **`docs/demo-runbook.md`**.
