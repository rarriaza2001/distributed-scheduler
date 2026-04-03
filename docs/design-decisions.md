# Design Decisions

## System Shape
- Single scheduler leader + many workers
- Scheduler = control plane + state authority
- Workers = execution only (no DB writes)

## Language
- Go
- Reason:
  - Native concurrency (goroutines)
  - Independent control loops
  - Strong backend/distributed systems signal

## Queue Backbone
- Redis Streams (consumer groups)
- Provides:
  - At-least-once delivery
  - Pending entries tracking (PEL)
  - Consumer coordination

## Leader Election
- Redis lease-based leader election
- Single active scheduler enforced via expiring lock
- Lease must be renewed periodically
- Expiry enables failover

## Persistence Model

### DB (Durable Truth)
- Job (canonical lifecycle state)
- ExecutionAudit (append-only event log)

### Redis (Ephemeral Coordination)
- Job transport (Streams)
- WorkerRegistration
- Heartbeat (TTL-based)
- LeadershipState (lease key)
- Retry schedule (delayed execution)

## Retry Model
- Split responsibility:
  - DB → retry truth (attempts, failure, eligibility)
  - Redis → retry timing (delayed scheduling)

## Contracts

### Job (DB)
- Canonical lifecycle record
- Tracks:
  - status
  - attempts
  - lease ownership
  - retry timing

### WorkerRegistration (Redis)
- Worker identity + capabilities

### Heartbeat (Redis)
- Liveness + capacity snapshot

### LeadershipState (Redis)
- Current scheduler leader + lease

### ExecutionAudit (DB)
- Append-only event log
- Stores:
  - job lifecycle events
  - worker events
  - leadership changes

## Commands vs Events
- Commands = scheduler intent (not persisted)
- Events = facts (persisted as ExecutionAudit)
- No event sourcing

## Key Rules

### Scheduler Owns Truth
- Only scheduler writes to DB
- Workers never mutate job state

### DB vs Redis
- DB = long-term truth
- Redis = live coordination

### Delivery Semantics
- At-least-once
- Idempotency required

### Lease-Based Execution
- Jobs assigned with expiration (lease)
- Recovery happens after lease expiry

### Validation Rule
- Redis signals must always be validated against DB