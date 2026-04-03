# Failure Modes — Distributed Job Scheduler

## 1. Worker Dies Mid-Execution

### What happens
- Lease expires
- Scheduler detects abandonment
- Job is resubmitted

### Result
- Duplicate execution possible

### Mitigation
- Idempotency
- Lease expiration

---

## 2. Worker Alive but Stuck

### What happens
- Heartbeats continue
- No progress (no ACK)

### Signal
- queue depth increases
- throughput stagnates

### Mitigation
- Abandonment detection
- lease expiration

---

## 3. Scheduler Crash

### What happens
- Leadership lease expires
- New scheduler takes over

### Result
- Recovery resumes

---

## 4. Split-Brain Scheduler

### Cause
- lease expiration + delayed detection

### What happens
- two schedulers act temporarily

### Result
- duplicate resubmissions

### Mitigation
- idempotency
- immediate step-down

---

## 5. Redis Failure / Restart

### What happens
- stream or ephemeral state lost
- pending jobs may disappear

### Mitigation
- DB reconciliation
- scheduler recovery

---

## 6. DB Commit Succeeds, Redis Enqueue Fails

### What happens
- job marked queued but not in stream

### Mitigation
- reconciliation pass re-enqueues

---

## 7. Redis Enqueue Succeeds, DB Commit Fails

### What happens
- worker receives job without DB truth

### Mitigation
- worker must validate against DB

---

## 8. Retry Storm

### Cause
- repeated failures
- misconfigured retry policy

### Signal
- retry metric spikes

### Mitigation
- backoff strategy
- retry limits

---

## 9. Leadership Flapping

### Cause
- TTL misconfiguration
- GC pauses
- network jitter

### Signal
- leader_changes_total spikes

---

## 10. High Queue Backpressure

### Cause
- insufficient workers

### Signal
- queue depth rises
- queue wait latency rises

---

## 11. Metric Cardinality Explosion (Design Risk)

### Cause
- labels like job_id or raw errors

### Impact
- Prometheus instability

---

## Summary

All failure modes rely on:
- idempotency
- reconciliation
- observability

System correctness is preserved even under:
- duplication
- partial failures