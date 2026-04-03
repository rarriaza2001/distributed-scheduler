# System Design — Distributed Job Scheduler

## 1. Overview

This project is a **distributed job scheduler** designed to reliably process background jobs across multiple workers while preserving correctness under failure.

The system is built around a simple principle:

- **Postgres is the source of truth**
- **Redis is used for transport and coordination**
- **Schedulers control lifecycle**
- **Workers execute jobs**

The architecture is intentionally designed for:

- at-least-once delivery
- bounded coordination
- failure recovery
- operational observability
- horizontal worker scaling

It does **not** attempt to provide exactly-once execution or consensus-grade coordination.

---

## 2. Goals

### Primary goals
- reliably enqueue and execute jobs
- recover from worker crashes and stuck execution
- support multiple workers concurrently
- support multiple scheduler processes with leader failover
- preserve durable lifecycle state
- allow safe retries and reassignment
- provide strong observability for production debugging

### Non-goals
- exactly-once delivery
- consensus / linearizable coordination
- global strict mutual exclusion
- fully user-facing frontend product
- cross-system distributed transactions

---

## 3. High-Level Architecture

The system has four major parts:

### 1. API / Producer Layer
Responsible for creating jobs and persisting them in Postgres.

### 2. Scheduler Control Plane
Responsible for:
- dispatching eligible jobs
- leader election
- reconciliation
- retry scheduling
- abandonment recovery

### 3. Worker Execution Plane
Responsible for:
- claiming jobs from Redis Streams
- executing handlers
- renewing per-job leases
- reporting lifecycle results

### 4. Data / Coordination Layer
Consists of:
- **Postgres** for durable job truth
- **Redis** for transport, leases, heartbeats, and ephemeral observability helpers

---

## 4. Core Components

## 4.1 Postgres

Postgres is the authoritative system of record.

### Stores
- `jobs`
- `execution_audit`

### Responsibilities
- persistent job state
- lifecycle transitions
- retry metadata
- lease metadata
- scheduling metadata
- append-only audit trail

### Why Postgres
The project needs:
- transactional lifecycle updates
- recovery-safe state
- reliable inspection
- durable audit history

---

## 4.2 Redis Streams

Redis Streams are used as the job transport layer.

### Responsibilities
- queueing jobs
- consumer groups
- pending entry tracking
- worker claims
- acknowledgements

### Why Redis Streams
They provide:
- high-throughput distribution
- consumer coordination
- replay / pending semantics
- operational simplicity

Redis is intentionally **not authoritative**.

---

## 4.3 Scheduler

The scheduler is the **control plane**.

### Responsibilities
- leader election
- dispatching eligible jobs
- retrying eligible jobs
- detecting abandoned jobs
- reconciling Redis coordination state against DB truth

### Important rule
Only the **current leader** may perform leader-only work.

### Multiple schedulers
Many scheduler instances may run, but only one should actively control lifecycle at a time.

---

## 4.4 Workers

Workers are the **execution plane**.

### Responsibilities
- consume jobs from Redis Streams
- execute job handlers
- heartbeat process liveness
- renew per-job execution leases
- report outcomes back through lifecycle hooks
- ACK successful Redis deliveries

### Important rule
Workers do **not** directly mutate authoritative lifecycle rows outside the defined lifecycle service path.

---

## 5. Data Flow

## 5.1 Job Creation
1. Producer/API creates a job in Postgres
2. Job is persisted with initial lifecycle state
3. Scheduler later determines the job is eligible
4. Leader dispatches job to Redis Stream

---

## 5.2 Job Dispatch
1. Leader queries eligible jobs from Postgres
2. Leader updates job state in Postgres within a transaction
3. Audit event is written in the same transaction
4. After successful commit, job is enqueued to Redis Stream

### Important note
DB commit and Redis enqueue are **not** atomic together.

That means:
- DB may say queued while Redis enqueue fails
- recovery/reconciliation must repair that gap

---

## 5.3 Job Execution
1. Worker reads from Redis consumer group
2. Worker claims delivery
3. Worker acquires or renews per-job lease
4. Worker reports job started through lifecycle path
5. Worker executes handler
6. On success:
   - lifecycle success is recorded
   - audit event is written
   - Redis entry is ACKed
7. On failure:
   - lifecycle failure/retry state is recorded
   - audit event is written
   - future retry is scheduled if policy allows

---

## 5.4 Recovery / Reconciliation
1. Leader scans for expired leases / abandoned work
2. Leader reconciles DB truth against Redis coordination state
3. Eligible jobs are restored to safe queued/retryable states
4. Jobs are re-dispatched as appropriate

Redis never overrides Postgres truth.

---

## 6. Job Lifecycle Model

The lifecycle is DB-authoritative.

Typical lifecycle progression:

- created
- queued / dispatched
- running
- succeeded
- failed
- retry scheduled
- dead / terminal failure

Each transition must:
- update the job row
- append an audit record
- commit atomically in one Postgres transaction

This ensures:
- durable lifecycle truth
- replayable history
- easier debugging

---

## 7. Delivery Semantics

## At-Least-Once Delivery
The system intentionally supports **at-least-once execution**.

### What that means
A job may execute:
- once
- more than once
- again after reassignment or retry

### Why chosen
Exactly-once would require much more complex coordination, typically involving:
- distributed transactions
- stronger consensus assumptions
- much higher implementation complexity

### Required application contract
All job handlers must be **idempotent**.

That is a hard requirement.

---

## 8. Leadership Design

Leadership is implemented through a Redis lease.

### Mechanism
- key: `scheduler:leader`
- value: `schedulerID`
- TTL-based ownership

### Operations
- acquire using `SET NX PX`
- renew only if value still matches owner
- release only if value still matches owner

### Why this works
It provides:
- simple active leader selection
- failover after leader death
- bounded control-plane coordination

### Known limitation
This is **not consensus**.

Possible failure cases include:
- brief split-brain
- GC pause causing lease expiry
- network partition / delayed response
- Redis hiccups

### Safety rule
If a scheduler cannot prove it still owns the lease, it must step down immediately.

---

## 9. Lease Model for Workers

Each running job has a per-job lease.

### Purpose
- establish temporary execution ownership
- detect stale / abandoned execution
- allow safe reassignment after lease expiry

### Properties
- ownership is tied to worker ID
- renew is ownership-safe
- release is ownership-safe

### Why leases matter
A dead or stalled worker must not hold work forever.

Leases create **time-bounded ownership**, not permanent locks.

---

## 10. Heartbeats

Workers emit process heartbeats independently from per-job leases.

### Heartbeat ansThe projectrs
- is the worker process alive?

### Lease answers
- is this worker still the valid owner of this job execution?

These are intentionally separate because a worker can be:
- alive
- but blocked, wedged, or not progressing

This distinction is operationally critical.

---

## 11. Failure Recovery Strategy

The system is built around recovery, not perfect prevention.

### Examples
- worker crashes mid-execution
- scheduler crashes
- Redis enqueue fails after DB commit
- stale pending entries remain in Redis
- leader lease expires unexpectedly
- duplicate dispatch occurs during split-brain window

### Recovery tools
- Postgres lifecycle truth
- audit log
- reconciliation loop
- retries
- abandonment detection
- lease expiration
- idempotent handlers

This allows the system to converge back to safe state after partial failure.

---

## 12. Observability Design

Observability is a first-class system feature.

### Metrics
The system tracks:
- queue depth
- throughput
- retries
- abandoned jobs
- terminal failures
- worker heartbeats
- workers alive
- leadership state
- leadership changes
- queue wait latency
- execution latency
- end-to-end latency

### Logging
Structured logs are used for:
- leadership transitions
- dispatch errors
- recovery errors
- worker failures
- lease errors
- ACK failures

### Failed-job inspection
A lightweight bounded recent-failures view exists for debugging:
- metadata only
- bounded retention
- not a source of truth

### Dashboards
Grafana dashboards are used to inspect:
- system health
- queue pressure
- worker health
- retry/failure trends
- latency/throughput
- leadership stability

---

## 13. Key Tradeoffs

## 13.1 Postgres Truth vs Redis Truth
I chose Postgres truth because correctness and recovery matter more than pure speed.

## 13.2 At-Least-Once vs Exactly-Once
I chose at-least-once because exactly-once is far more complex and often not worth it for background jobs.

## 13.3 Lease-Based Leadership vs Consensus
I chose Redis leases because they are simpler and operationally lighter, while idempotency makes bounded split-brain acceptable.

## 13.4 Minimal Metrics vs Exhaustive Metrics
I chose minimal core metrics to avoid noise, high cardinality, and dashboard clutter.

## 13.5 No DB↔Redis Distributed Transactions
I accepted temporary mismatch betIen DB and Redis and solved it with reconciliation instead of distributed commits.

---

## 14. Scaling Model

## Horizontal scaling
The system scales primarily by adding more workers.

### Worker scaling
More workers allow:
- higher concurrent throughput
- faster queue drain
- better backlog handling

### Scheduler scaling
Multiple schedulers may run for availability, but only one leader actively controls lifecycle.

### Likely bottlenecks
As load grows, limits are likely to show up in:
- Postgres write contention
- Redis stream throughput
- handler execution speed
- retry storms under failure

---

## 15. Security / Safety Boundaries

The project’s operational safety depends on:
- idempotent job handlers
- ownership-safe lease renew/release
- scheduler step-down on uncertain leadership
- DB-authoritative lifecycle transitions
- bounded observability cardinality
- metadata-only recent failure storage

These are not optional details; they are part of system correctness.

---

## 16. Why This Architecture Fits the Project

This architecture was chosen because it balances:

- practical implementation effort
- strong recovery behavior
- observability
- horizontal execution scaling
- realistic distributed systems tradeoffs

It does not pretend to eliminate all distributed systems failure modes.

Instead, it is designed so that:
- failures are visible
- duplicates are safe
- recovery is possible
- operations remain understandable

That is the core design philosophy of the system.

---

## 17. Final Summary

This distributed scheduler uses:

- **Postgres** for durable lifecycle truth
- **Redis Streams** for job transport
- **Redis leases** for leadership and execution ownership
- **Schedulers** for lifecycle control and recovery
- **Workers** for execution
- **Observability** for production operability

The system deliberately accepts:
- at-least-once delivery
- duplicate execution
- bounded coordination gaps

and relies on:
- idempotency
- reconciliation
- leases
- auditability
- observability

to maintain correctness under failure.