package queue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type HandlerMode int

const (
	NoOp HandlerMode = iota
	FixedCost
)

type BenchmarkConfig struct {
	JobCount    int
	WorkerCount int
	ClaimBatch  int
	HandlerMode HandlerMode
	FixedDelay  time.Duration
}

func RunBenchmark(ctx context.Context, q Queue, cfg BenchmarkConfig) error {
	if cfg.JobCount <= 0 {
		return fmt.Errorf("job count must be > 0")
	}
	if cfg.WorkerCount <= 0 {
		return fmt.Errorf("worker count must be > 0")
	}

	// Phase 2 contract:
	// one in-flight job per worker, so benchmark must claim one at a time.
	if cfg.ClaimBatch <= 0 {
		cfg.ClaimBatch = 1
	}
	if cfg.ClaimBatch > 1 {
		cfg.ClaimBatch = 1
	}

	submitter := NewSubmitter(q)

	for i := 0; i < cfg.JobCount; i++ {
		msg := JobMessage{
			JobID:          fmt.Sprintf("job-%d", i),
			Queue:          "bench",
			Type:           "bench_job",
			Payload:        []byte(`{}`),
			Priority:       1,
			ScheduledAt:    time.Now(),
			IdempotencyKey: fmt.Sprintf("bench-%d", i),
		}

		if err := submitter.Submit(ctx, msg); err != nil {
			return err
		}
	}

	var completed int64
	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < cfg.WorkerCount; i++ {
		wg.Add(1)

		go func(n int) {
			defer wg.Done()

			workerID := fmt.Sprintf("worker-%d", n)
			// heartbeatInterval, heartbeatTTL, leaseInterval, leaseTTL (nil stores → no heartbeat/lease I/O)
			worker := NewWorker(q, workerID, 1, nil, time.Second, time.Second, nil, time.Second, time.Second)
			worker.StartHeartbeat(ctx)
			for {
				if atomic.LoadInt64(&completed) >= int64(cfg.JobCount) {
					return
				}

				claimed, err := worker.Claim(ctx, cfg.ClaimBatch, 250*time.Millisecond)
				if err != nil {
					continue
				}
				if len(claimed) == 0 {
					continue
				}

				for _, msg := range claimed {
					err := worker.ExecuteOne(ctx, msg, func(jobCtx context.Context, job JobMessage) error {
						handle(job, cfg)
						return nil
					})
					if err == nil {
						atomic.AddInt64(&completed, 1)
					}
				}
			}
		}(i)
	}

	wg.Wait()

	elapsed := time.Since(start)
	throughput := float64(cfg.JobCount) / elapsed.Seconds()

	fmt.Printf(
		"jobs=%d workers=%d batch=%d elapsed=%s throughput=%.2f jobs/sec\n",
		cfg.JobCount,
		cfg.WorkerCount,
		cfg.ClaimBatch,
		elapsed,
		throughput,
	)

	return nil
}

func handle(job JobMessage, cfg BenchmarkConfig) {
	switch cfg.HandlerMode {
	case NoOp:
		return
	case FixedCost:
		time.Sleep(cfg.FixedDelay)
	}
}
