# Scaling Considerations

## Current Architecture Limits

### Single Scheduler Leader
- Only one scheduler performs:
  - dispatch
  - retries
  - recovery
- Bottleneck under high load

---

## Worker Scaling

### Horizontal Scaling
- Add more workers
- Workers are stateless
- Scales independently of scheduler

### Limits
- Scheduler must keep up with:
  - dispatch decisions
  - heartbeat processing

---

## Redis Scaling

### Current Role
- Queue transport (Streams)
- Coordination (heartbeats, leadership, retry)

### Risks
- Single Redis instance = SPOF
- High throughput may saturate Redis

### Future Options
- Redis clustering
- Sharded queues

---

## DB Scaling

### Current Role
- Job lifecycle
- ExecutionAudit

### Risks
- High write volume (audit + job updates)
- Retry-heavy workloads increase load

### Future Options
- Read replicas
- Partitioning jobs by queue
- Archiving old audit data

---

## Scheduler Bottleneck

### Problem
- Single leader handles all logic

### Future Improvements
- Partition queues across multiple schedulers
- Shard by queue or job type
- Move toward partial decentralization

---

## Retry Scaling

### Current Model
- Redis sorted set for delayed retries

### Risks
- Large retry sets → slow scans

### Future Improvements
- Bucketed retry queues
- Time-window partitioning

---

## Observability Scaling

### ExecutionAudit Growth
- Append-only → unbounded growth

### Future Improvements
- Retention policies
- Aggregation pipelines
- Metrics extraction

---

## Throughput Considerations

### Key Constraints
- Scheduler decision rate
- Redis throughput
- DB write throughput

---

## Future Evolution Path

1. Scale workers horizontally
2. Optimize Redis usage
3. Partition queues
4. Introduce scheduler sharding
5. Add observability + metrics pipelines

---

## Key Principle

Scale by:
- separating coordination from execution
- minimizing shared bottlenecks
- keeping scheduler logic efficient