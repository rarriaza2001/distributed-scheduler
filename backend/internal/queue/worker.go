package queue

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"scheduler/internal/observability"
)

// JobHooks optionally reports lifecycle signals to the scheduler service (DB).
// Workers must not write the jobs table directly; hooks call into JobLifecycle.
type JobHooks struct {
	BeforeExecute func(ctx context.Context, workerID string, msg ClaimedMessage) error
	AfterSuccess  func(ctx context.Context, workerID string, msg ClaimedMessage) error
	AfterFailure  func(ctx context.Context, workerID string, msg ClaimedMessage, runErr error) error
}

// Worker executes jobs only (Phase 2). Heartbeat reflects process liveness only;
// per-job lease reflects ownership freshness for a specific job.
type Worker struct {
	queue             Queue
	workerID          string
	maxInFlight       int
	inFlight          int64
	heartbeatStore    HeartbeatStore
	heartbeatInterval time.Duration
	heartbeatTTL      time.Duration
	leaseStore        LeaseStore
	leaseInterval     time.Duration
	leaseTTL          time.Duration
	Hooks             *JobHooks

	log     *slog.Logger
	metrics *observability.Metrics
}

func NewWorker(
	jobQueue Queue,
	workerID string,
	maxInFlight int,
	heartbeatStore HeartbeatStore,
	heartbeatInterval, heartbeatTTL time.Duration,
	leaseStore LeaseStore,
	leaseInterval, leaseTTL time.Duration,
) *Worker {
	if maxInFlight <= 0 {
		maxInFlight = 1
	}
	if heartbeatInterval <= 0 {
		heartbeatInterval = time.Second
	}
	if heartbeatTTL <= heartbeatInterval {
		heartbeatTTL = heartbeatInterval * 3
	}
	if leaseInterval <= 0 {
		leaseInterval = time.Second
	}
	if leaseTTL <= leaseInterval {
		leaseTTL = leaseInterval * 3
	}
	return &Worker{
		queue:             jobQueue,
		workerID:          workerID,
		maxInFlight:       maxInFlight,
		inFlight:          0,
		heartbeatStore:    heartbeatStore,
		heartbeatInterval: heartbeatInterval,
		heartbeatTTL:      heartbeatTTL,
		leaseStore:        leaseStore,
		leaseInterval:     leaseInterval,
		leaseTTL:          leaseTTL,
		log:               nil,
		metrics:           nil,
	}
}

// SetObservability attaches optional structured logging and metrics (heartbeat, lease errors are logged).
func (w *Worker) SetObservability(log *slog.Logger, m *observability.Metrics) {
	w.log = log
	w.metrics = m
}

func (w *Worker) Claim(ctx context.Context, count int, block time.Duration) ([]ClaimedMessage, error) {
	for {
		current := atomic.LoadInt64(&w.inFlight)
		if current >= int64(w.maxInFlight) {
			time.Sleep(block)
			continue
		}

		if atomic.CompareAndSwapInt64(&w.inFlight, current, current+1) {
			break
		}
	}

	if count > w.maxInFlight {
		count = w.maxInFlight
	}
	if count <= 0 {
		count = 1
	}

	messages, err := w.queue.Claim(ctx, w.workerID, count, block)
	if err != nil {
		atomic.AddInt64(&w.inFlight, -1)
		return nil, err
	}

	if len(messages) == 0 {
		atomic.AddInt64(&w.inFlight, -1)
		return nil, nil
	}

	return messages, nil
}

// releaseInFlight is the single place that decrements the Claim reservation counter
// for successfully reserved slots (one increment per successful Claim that returned work).
func (w *Worker) releaseInFlight(n int) {
	if n <= 0 {
		return
	}
	atomic.AddInt64(&w.inFlight, -int64(n))
}

// ackDelivery performs queue Ack and per-job lease Release only; it does not change inFlight.
func (w *Worker) ackDelivery(ctx context.Context, messages ...ClaimedMessage) error {
	err := w.queue.Ack(ctx, w.workerID, messages...)
	if err != nil {
		if w.log != nil {
			w.log.Error(observability.EventAckFailure, "event", observability.EventAckFailure, "worker_id", w.workerID, "err", err)
		}
		return err
	}
	if w.leaseStore != nil {
		for _, m := range messages {
			if rerr := w.leaseStore.Release(ctx, m.Job.JobID, w.workerID); rerr != nil && w.log != nil {
				w.log.Error(observability.EventLeaseReleaseError, "event", observability.EventLeaseReleaseError, "worker_id", w.workerID, "job_id", m.Job.JobID, "err", rerr)
			}
		}
	}
	return nil
}

func (w *Worker) Ack(ctx context.Context, messages ...ClaimedMessage) error {
	if err := w.ackDelivery(ctx, messages...); err != nil {
		return err
	}
	w.releaseInFlight(len(messages))
	return nil
}

// ExecuteOne runs a single claimed message: job-scoped context, optional lease renewal loop,
// handler, then transport ack on success. Abandonment for reassignment is pending in Redis + inactive lease.
//
// The Claim() reservation is always released exactly once via defer releaseInFlight(1).
// On handler error, the delivery is not acked; on ackDelivery error, the slot is still released
// but the message may remain pending in Redis.
func (w *Worker) ExecuteOne(ctx context.Context, msg ClaimedMessage, handler func(context.Context, JobMessage) error) error {
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	startedAt := time.Now().UTC()
	msg.StartedAt = startedAt

	if w.Hooks != nil && w.Hooks.BeforeExecute != nil {
		if err := w.Hooks.BeforeExecute(ctx, w.workerID, msg); err != nil {
			w.releaseInFlight(1)
			return err
		}
	}

	if w.leaseStore != nil {
		w.StartLeaseLoop(jobCtx, msg)
	}

	defer w.releaseInFlight(1)

	if err := handler(jobCtx, msg.Job); err != nil {
		if w.Hooks != nil && w.Hooks.AfterFailure != nil {
			_ = w.Hooks.AfterFailure(ctx, w.workerID, msg, err)
		}
		if w.log != nil {
			w.log.Error(observability.EventWorkerExecFailure, "event", observability.EventWorkerExecFailure, "worker_id", w.workerID, "job_id", msg.Job.JobID, "err", err)
		}
		return err
	}

	if w.Hooks != nil && w.Hooks.AfterSuccess != nil {
		if err := w.Hooks.AfterSuccess(ctx, w.workerID, msg); err != nil {
			return err
		}
	}

	return w.ackDelivery(ctx, msg)
}

func (w *Worker) StartHeartbeat(ctx context.Context) {
	if w.heartbeatStore == nil {
		return
	}
	err := w.heartbeatStore.Beat(ctx, w.workerID, w.heartbeatTTL)
	if err != nil {
		if w.log != nil {
			w.log.Error(observability.EventHeartbeatError, "event", observability.EventHeartbeatError, "worker_id", w.workerID, "err", err)
		}
		return
	}
	if w.metrics != nil {
		w.metrics.IncWorkerHeartbeat()
	}

	ticker := time.NewTicker(w.heartbeatInterval)
	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if berr := w.heartbeatStore.Beat(ctx, w.workerID, w.heartbeatTTL); berr != nil {
					if w.log != nil {
						w.log.Error(observability.EventHeartbeatError, "event", observability.EventHeartbeatError, "worker_id", w.workerID, "err", berr)
					}
					continue
				}
				if w.metrics != nil {
					w.metrics.IncWorkerHeartbeat()
				}
			}
		}
	}()
}

// StartLeaseLoop renews the per-job lease until ctx is canceled. ctx must be
// scoped to this job only (e.g. jobCtx, cancel := context.WithCancel(runLoopCtx);
// defer cancel()), not the worker process lifetime—otherwise the lease outlives the job.
func (w *Worker) StartLeaseLoop(ctx context.Context, msg ClaimedMessage) {
	if w.leaseStore == nil {
		return
	}

	if aerr := w.leaseStore.Acquire(ctx, msg.Job.JobID, w.workerID, w.leaseTTL); aerr != nil && w.log != nil {
		w.log.Error(observability.EventLeaseAcquireError, "event", observability.EventLeaseAcquireError, "worker_id", w.workerID, "job_id", msg.Job.JobID, "err", aerr)
	}

	ticker := time.NewTicker(w.leaseInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if rerr := w.leaseStore.Renew(ctx, msg.Job.JobID, w.workerID, w.leaseTTL); rerr != nil && w.log != nil {
					w.log.Error(observability.EventLeaseRenewError, "event", observability.EventLeaseRenewError, "worker_id", w.workerID, "job_id", msg.Job.JobID, "err", rerr)
				}
			}
		}
	}()
}
