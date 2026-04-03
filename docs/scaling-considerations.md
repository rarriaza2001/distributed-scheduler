# Scaling Considerations — Distributed Job Scheduler

## 1. Horizontal Worker Scaling

### Strategy
- Add more workers to increase throughput

### Limits
- DB contention
- Redis stream throughput

---

## 2. Scheduler Scaling

### Strategy
- Multiple schedulers (leader election)

### Constraint
- Only one active leader

---

## 3. Redis Scaling

### Strategy
- vertical scaling first
- clustering if needed

### Risks
- cross-slot complexity
- stream partitioning complexity

---

## 4. Postgres Scaling

### Strategy
- indexing
- read replicas
- partitioning (future)

### Risk
- write contention on jobs table

---

## 5. Queue Throughput

### Bottlenecks
- Redis XREADGROUP latency
- worker processing speed

---

## 6. Job Duration Impact

### Long jobs
- require frequent lease renewal
- increase recovery complexity

---

## 7. Retry Scaling

### Risk
- exponential retries amplify load

### Mitigation
- backoff
- retry limits

---

## 8. Observability Scaling

### Risk
- too many metrics

### Strategy
- keep low cardinality
- aggregate signals

---

## 9. Failure Recovery Scaling

### Recovery loops must:
- be bounded
- avoid scanning entire DB frequently

---

## 10. Future Scaling Paths

- multiple queues
- priority queues
- sharded schedulers
- region-based deployment

---

## Summary

Scaling is primarily:
- worker-driven
- DB-limited
- coordination-light

System scales well horizontally as long as:
- DB contention is controlled
- metrics remain bounded