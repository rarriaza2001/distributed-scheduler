# Redis queue benchmark

The benchmark implementation lives in the Go module under **`backend/`** (`cmd/main` + `internal/queue/benchmark.go`). This folder holds **runner scripts** so you can start the benchmark from the repo root without remembering paths.

**Requires:** Redis reachable (default `localhost:6379`).

- **Windows (PowerShell):** `./run.ps1` or `.\run.ps1`
- **Unix:** `./run.sh` (make executable: `chmod +x run.sh`)

Or manually:

```bash
cd backend
go run ./cmd/main
```

Optional: set `REDIS_ADDR` before running (scripts pass it through where supported).
