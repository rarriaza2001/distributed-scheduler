# Design Decisions — Distributed Job Scheduler

## 1. Core Philosophy

This system prioritizes:
- **Correctness under failure > strict consistency**
- **Operational simplicity > theoretical guarantees**
- **At-least-once delivery > exactly-once complexity**
- **Explicit observability > implicit behavior**

We intentionally avoid over-engineering (e.g., consensus systems) and instead rely on:
- idempotency
- bounded coordination
- strong lifecycle authority in Postgres

---

## 2. Source of Truth: Postgres

### Decision
Postgres is the **authoritative source of truth** for:
- job state
- scheduling
- lifecycle transitions

### Tradeoffs

**Pros**
- Strong consistency guarantees
- Transactional lifecycle + audit
- Easier debugging and replay
- Durable recovery model

**Cons**
- Higher latency than in-memory systems
- Requires careful indexing
- Not optimized for high-frequency coordination

### Why chosen
We prioritized **correct recovery and auditability** over raw speed.

---

## 3. Redis as Transport + Coordination Only

### Decision
Redis is used for:
- Streams (job transport)
- Consumer groups (distribution)
- Leases (worker ownership + scheduler leadership)
- Ephemeral state (heartbeats, recent failures)

Redis is **NOT** a source of truth.

### Tradeoffs

**Pros**
- Fast, scalable distribution
- Built-in pending tracking (PEL)
- Lightweight coordination primitives

**Cons**
- Not strongly consistent
- Can lose state during failover
- Requires reconciliation with DB

### Why chosen
Redis gives **efficient distribution**, while Postgres handles correctness.

---

## 4. At-Least-Once Delivery Model

### Decision
System guarantees:
- jobs may execute more than once
- idempotency is required

### Tradeoffs

**Pros**
- Simpler design
- Works well with Redis Streams
- Robust under failure and retries

**Cons**
- Duplicate execution possible
- Requires idempotent job handlers

### Why chosen
Exactly-once semantics introduce:
- distributed transactions
- consensus requirements

We chose **practical reliability over theoretical guarantees**.

---

## 5. Scheduler Leadership via Redis Lease

### Decision
Single active scheduler enforced via:
- Redis key with TTL
- ownership-safe renew
- immediate step-down on uncertainty

### Tradeoffs

**Pros**
- Simple failover
- No consensus system required
- Easy to reason about

**Cons**
- Split-brain possible (bounded)
- Not linearizable
- Depends on timing assumptions

### Why chosen
We explicitly accept:
- **bounded inconsistency**
- **temporary split-brain**

because idempotency protects correctness.

---

## 6. No DB ↔ Redis Transactions

### Decision
We do NOT attempt atomic operations across:
- Postgres
- Redis

### Tradeoffs

**Pros**
- Simpler architecture
- Avoids distributed transaction complexity
- More resilient to partial failures

**Cons**
- Temporary inconsistencies possible
- Requires reconciliation logic

### Why chosen
Distributed transactions are:
- complex
- slow
- fragile

We rely instead on:
- reconciliation
- leases
- retries

---

## 7. Lease-Based Execution Model

### Decision
Each job has:
- leaseOwnerWorkerId
- leaseExpiresAt

Workers must:
- renew lease periodically
- lose ownership on expiration

### Tradeoffs

**Pros**
- Enables recovery of stuck jobs
- Prevents permanent lock by dead worker
- Simple coordination model

**Cons**
- Requires tuning TTL and renewal intervals
- Can cause duplicate execution

### Why chosen
Leases provide **time-bounded ownership**, which is critical for recovery.

---

## 8. Observability-First Design (Phase 4)

### Decision
We explicitly defined:
- metrics
- dashboards
- failure inspection
- logging structure

### Tradeoffs

**Pros**
- Faster debugging
- Clear operational model
- Better production readiness

**Cons**
- Additional implementation complexity
- Requires discipline in metric design

### Why chosen
Distributed systems without observability are **not operable**.

---

## 9. Minimal Metrics Strategy

### Decision
We chose:
- **minimal core metrics**
- low-cardinality labels

### Tradeoffs

**Pros**
- High signal-to-noise ratio
- Lower cost
- Easier dashboards

**Cons**
- Less forensic detail
- More reliance on logs

### Why chosen
Avoid:
- metric explosion
- useless dashboards

---

## 10. Lightweight Failed Job Inspection

### Decision
We added:
- bounded Redis store
- metadata-only failed job view

### Tradeoffs

**Pros**
- fast debugging
- immediate visibility

**Cons**
- not durable
- not full forensic store

### Why chosen
We needed:
- quick inspection
- without building a full analytics system

---

## 11. No Frontend UI (Intentional)

### Decision
No dedicated UI layer; rely on:
- Grafana dashboards
- debug endpoints
- demo tooling

### Tradeoffs

**Pros**
- faster backend focus
- lower scope

**Cons**
- less user-friendly
- demo requires setup

### Why chosen
This project prioritizes:
- backend system design
- not product UX

---

## 12. Demo Workload Strategy

### Decision
Introduce:
- controlled demo jobs
- staged execution with delays
- intentional failures and retries

### Tradeoffs

**Pros**
- observable system behavior
- strong demo capability

**Cons**
- non-production code paths

### Why chosen
A distributed system must be:
- **demonstrable**
- not just theoretically correct

---

## Summary

This system intentionally favors:
- simplicity over perfection
- observability over opacity
- recovery over prevention
- correctness under failure over strict guarantees