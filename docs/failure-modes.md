# Failure Modes

## Worker Crash

### Failure
- Worker dies mid-execution
- Stops sending heartbeats
- Job may remain leased or running

### Detection
- Heartbeat timeout in Redis

### Recovery
- Scheduler marks worker as timed out
- Jobs recovered after leaseExpiresAt
- Jobs requeued or dead-lettered

### Risk
- Duplicate execution possible

---

## Scheduler Crash

### Failure
- Active scheduler stops
- No dispatch, retries, or monitoring

### Detection
- Leader lease expires in Redis

### Recovery
- New scheduler acquires leadership
- Restarts:
  - dispatch loop
  - retry loop
  - recovery logic

---

## Redis Down

### Failure
- No job transport
- No heartbeats
- No leader election

### Impact
- System pauses coordination
- Workers cannot receive jobs

### Recovery
- Redis restored
- Workers re-register
- Scheduler re-establishes leadership
- State rebuilt from DB

---

## Duplicate Delivery

### Causes
- Worker crash after side effects
- Lease expiry + recovery
- Message reclaiming
- Late success/failure reports

### Handling
- At-least-once semantics
- Idempotency required

---

## Stuck Job

### Failure
- Job remains leased/running indefinitely

### Detection
- leaseExpiresAt <= now

### Recovery
- Scheduler reclaims job
- Requeue or dead-letter

---

## Heartbeat Timeout

### Failure
- Worker stops heartbeating

### Detection
- TTL expiration or stale timestamp

### Recovery
- Worker marked unavailable
- No new jobs assigned
- Existing jobs handled via lease expiry

---

## Message Reordering / Delayed Retry

### Failure
- Late or out-of-order events

### Examples
- Success after retry scheduled
- Retry after job already dead

### Handling
- Always validate against DB
- Ignore stale transitions

---

## Key Principles

- DB is source of truth
- Redis is coordination only
- Recovery happens via leases, not assumptions
- System must tolerate duplicates