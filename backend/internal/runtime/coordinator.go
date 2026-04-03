package runtime

import (
	"context"
	"time"

	"scheduler/internal/queue"
	"scheduler/internal/scheduler"
	"scheduler/internal/store"
)

// Coordinator wires Postgres lifecycle (truth) to Redis queue transport (at-least-once).
//
// Redis enqueue runs only after a successful dispatch transaction (AfterLeaseCommitted);
// there is no atomic DB+Redis transaction. For authoritative rules, see package scheduler.
//
// Example Phase 4 leader tick (split reconcile vs dispatch; both only run while Redis leadership is held):
//
//	leader.ReconcileOnTick = func(ctx context.Context) error {
//	  now := time.Now().UTC()
//	  return reconciler.Reconcile(ctx, scheduler.ReconcileOptions{
//	    LeaderAllowed: true, Now: now, RetryCoord: redisRetrySchedule, JobLease: scheduler.LeaseProbeFunc(redisJobLease.IsActive),
//	  })
//	}
//	leader.DispatchOnTick = func(ctx context.Context) error {
//	  now := time.Now().UTC()
//	  _, err := coord.Dispatch(ctx, CoordinatorDispatchOptions{
//	    LeaderAllowed: true, AsOf: now, Limit: 50, Now: now,
//	    AssignLease: assign,
//	  })
//	  return err
//	}
//
// Authoritative mutations always go through JobLifecycle; workers never touch JobStore directly.
type Coordinator struct {
	Life        *scheduler.JobLifecycle
	Queue       queue.Queue
	submitter   *queue.Submitter
	SchedulerID *string
	// AfterLeaseCommittedOverride, when non-nil, runs instead of Submitter.Submit after a successful
	// dispatch DB commit (tests, simulated Redis faults). Default behavior enqueues transport.
	AfterLeaseCommittedOverride func(ctx context.Context, job store.Job, workerID string, leaseUntil time.Time) error
}

// NewCoordinator returns a coordinator. The pgx pool is owned by JobLifecycle.
func NewCoordinator(q queue.Queue, life *scheduler.JobLifecycle) *Coordinator {
	return &Coordinator{
		Life:      life,
		Queue:     q,
		submitter: queue.NewSubmitter(q),
	}
}

// CreateJob persists a queued job and audit event only (no Redis). The leader dispatches to transport.
func (c *Coordinator) CreateJob(ctx context.Context, in store.InsertJobInput, source string) error {
	return c.Life.CreateJobAndAudit(ctx, scheduler.CreateJobParams{
		Job:         in,
		Source:      source,
		SchedulerID: c.SchedulerID,
	})
}

// CoordinatorDispatchOptions controls leader-gated dispatch and lease assignment.
type CoordinatorDispatchOptions struct {
	LeaderAllowed bool
	AsOf          time.Time
	Limit         int
	Now           time.Time
	AssignLease   func(job store.Job) (workerID string, leaseUntil time.Time, err error)
}

// Dispatch runs DispatchEligibleJobs and enqueues leased jobs to Redis after each DB commit.
func (c *Coordinator) Dispatch(ctx context.Context, opts CoordinatorDispatchOptions) (dispatched int, err error) {
	return c.Life.DispatchEligibleJobs(ctx, scheduler.DispatchOptions{
		LeaderAllowed: opts.LeaderAllowed,
		AsOf:          opts.AsOf,
		Limit:         opts.Limit,
		AssignLease:   opts.AssignLease,
		SchedulerID:   c.SchedulerID,
		Now:           opts.Now,
		AfterLeaseCommitted: func(ctx context.Context, job store.Job, workerID string, leaseUntil time.Time) error {
			if c.AfterLeaseCommittedOverride != nil {
				return c.AfterLeaseCommittedOverride(ctx, job, workerID, leaseUntil)
			}
			_ = workerID
			_ = leaseUntil
			return c.submitter.Submit(ctx, StoreJobToQueueMessage(job))
		},
	})
}

// FailedPolicy classifies worker execution errors for HandleWorkerFailed.
type FailedPolicy func(msg queue.ClaimedMessage, runErr error) (retryable bool, nextRetryAt time.Time, errMsg string, errCode string)

// WorkerLifecycleHooks connects queue.JobHooks to JobLifecycle worker report handlers.
func (c *Coordinator) WorkerLifecycleHooks(workerSource string, policy FailedPolicy) *queue.JobHooks {
	if policy == nil {
		policy = defaultRetryPolicy
	}
	return &queue.JobHooks{
		BeforeExecute: func(ctx context.Context, workerID string, msg queue.ClaimedMessage) error {
			occ := msg.StartedAt
			if occ.IsZero() {
				occ = time.Now().UTC()
			}
			return c.Life.HandleWorkerStarted(ctx, scheduler.WorkerStartedParams{
				JobID:    msg.Job.JobID,
				WorkerID: workerID,
				Source:   workerSource,
				Occurred: occ,
			})
		},
		AfterSuccess: func(ctx context.Context, workerID string, msg queue.ClaimedMessage) error {
			return c.Life.HandleWorkerSucceeded(ctx, scheduler.WorkerSucceededParams{
				JobID:           msg.Job.JobID,
				WorkerID:        workerID,
				Source:          workerSource,
				Occurred:        time.Now().UTC(),
				WorkerStartedAt: msg.StartedAt,
			})
		},
		AfterFailure: func(ctx context.Context, workerID string, msg queue.ClaimedMessage, runErr error) error {
			retryable, next, errMsg, errCode := policy(msg, runErr)
			return c.Life.HandleWorkerFailed(ctx, scheduler.WorkerFailedParams{
				JobID:           msg.Job.JobID,
				WorkerID:        workerID,
				Source:          workerSource,
				ErrorMessage:    errMsg,
				ErrorCode:       errCode,
				Retryable:       retryable,
				NextRetryAt:     next,
				Occurred:        time.Now().UTC(),
				WorkerStartedAt: msg.StartedAt,
			})
		},
	}
}

func defaultRetryPolicy(msg queue.ClaimedMessage, runErr error) (bool, time.Time, string, string) {
	_ = msg
	return true, time.Now().UTC().Add(time.Minute), runErr.Error(), "worker_error"
}

// StoreJobToQueueMessage maps an authoritative job row to transport envelope fields.
func StoreJobToQueueMessage(j store.Job) queue.JobMessage {
	idem := ""
	if j.IdempotencyKey != nil {
		idem = *j.IdempotencyKey
	}
	return queue.JobMessage{
		JobID:          j.JobID,
		Queue:          j.Queue,
		Type:           j.Type,
		Payload:        j.Payload,
		Priority:       j.Priority,
		ScheduledAt:    j.ScheduledAt,
		CreatedAt:      j.CreatedAt,
		IdempotencyKey: idem,
	}
}

// InsertJobInputToQueueMessage maps producer insert input to transport shape (for tests / tooling).
func InsertJobInputToQueueMessage(in store.InsertJobInput) queue.JobMessage {
	idem := ""
	if in.IdempotencyKey != nil {
		idem = *in.IdempotencyKey
	}
	return queue.JobMessage{
		JobID:          in.JobID,
		Queue:          in.Queue,
		Type:           in.Type,
		Payload:        in.Payload,
		Priority:       in.Priority,
		ScheduledAt:    in.ScheduledAt,
		CreatedAt:      in.CreatedAt,
		IdempotencyKey: idem,
	}
}
