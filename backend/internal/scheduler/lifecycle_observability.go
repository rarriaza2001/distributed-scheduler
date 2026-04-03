package scheduler

import (
	"context"
	"time"

	"scheduler/internal/observability"
	"scheduler/internal/store"
)

// eligibleTime is max(CreatedAt, ScheduledAt) for queue-wait: time until the job was eligible to run.
func eligibleTime(j store.Job) time.Time {
	if j.CreatedAt.After(j.ScheduledAt) {
		return j.CreatedAt
	}
	return j.ScheduledAt
}

func (s *JobLifecycle) observeQueueWait(j *store.Job, workerStart time.Time) {
	if s.metrics == nil || workerStart.IsZero() || j == nil {
		return
	}
	eligible := eligibleTime(*j)
	if workerStart.Before(eligible) {
		return
	}
	s.metrics.ObserveQueueWait(workerStart.Sub(eligible))
}

func (s *JobLifecycle) observeSuccessLatencies(j *store.Job, p WorkerSucceededParams) {
	if s.metrics == nil || j == nil {
		return
	}
	s.metrics.IncJobsCompleted()
	started := p.WorkerStartedAt
	if started.IsZero() {
		started = p.Occurred
	}
	occurred := p.Occurred
	if !started.IsZero() && occurred.After(started) {
		s.metrics.ObserveExecution(occurred.Sub(started))
	}
	if !j.CreatedAt.IsZero() && occurred.After(j.CreatedAt) {
		s.metrics.ObserveEndToEnd(occurred.Sub(j.CreatedAt))
	}
}

func (s *JobLifecycle) observeFailureLatencies(
	ctx context.Context,
	j *store.Job,
	p WorkerFailedParams,
	dead bool,
	newAttempts int,
	errMsg, errCode string,
	retryable bool,
) {
	if s.metrics == nil || j == nil {
		return
	}
	started := p.WorkerStartedAt
	if started.IsZero() {
		started = p.Occurred
	}
	occurred := p.Occurred

	if dead {
		s.metrics.IncJobsFailed()
	} else {
		s.metrics.IncRetry(observability.RetryReasonExecutionFailure)
	}

	if !started.IsZero() && occurred.After(started) {
		s.metrics.ObserveExecution(occurred.Sub(started))
	}
	if !j.CreatedAt.IsZero() && occurred.After(j.CreatedAt) {
		s.metrics.ObserveEndToEnd(occurred.Sub(j.CreatedAt))
	}

	if dead && s.failed != nil {
		meta := observability.FailedJobMeta{
			JobID:        p.JobID,
			JobType:      j.Type,
			Queue:        j.Queue,
			AttemptsMade: newAttempts,
			MaxAttempts:  j.MaxAttempts,
			Retryable:    retryable,
			Terminal:     true,
			ErrorCode:    errCode,
			ShortError:   observability.TruncateShortError(errMsg, 256),
			WorkerID:     p.WorkerID,
			FailedAt:     occurred,
		}
		if err := s.failed.Push(ctx, meta); err != nil && s.log != nil {
			s.log.Error(observability.EventFailedJobStoreError, "event", observability.EventFailedJobStoreError,
				"job_id", p.JobID, "err", err)
		}
	}
}
