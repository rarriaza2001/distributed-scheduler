package scheduler

import (
	"context"
	"log"
	"time"

	"scheduler/internal/queue"
)

// Reassigner (Phase 2): abandonment = delivery still pending in Redis + per-job lease inactive.
// Resubmits a fresh JobMessage only; original stale PEL entry is left in place (no scheduler XACK).
// Repeated scans may resubmit the same abandoned delivery multiple times until the PEL is cleared elsewhere.
type Reassigner struct {
	submitter        *queue.Submitter
	leaseStore       queue.LeaseStore
	pendingInspector queue.PendingInspector
	messageLookup    queue.MessageLookup
}

func NewReassigner(submitter *queue.Submitter, leaseStore queue.LeaseStore, pendingInspector queue.PendingInspector, messageLookup queue.MessageLookup) *Reassigner {
	return &Reassigner{
		submitter:        submitter,
		leaseStore:       leaseStore,
		pendingInspector: pendingInspector,
		messageLookup:    messageLookup,
	}
}

func (r *Reassigner) ScanAndReassign(ctx context.Context, limit int) error {
	pending, err := r.pendingInspector.ListPending(ctx, limit)
	if err != nil {
		return err
	}

	for _, p := range pending {
		msg, errLookup := r.messageLookup.GetClaimedMessage(ctx, p.StreamMessageID)
		if errLookup != nil {
			return errLookup
		}
		if msg == nil {
			continue
		}

		active, errLease := r.leaseStore.IsActive(ctx, msg.Job.JobID)
		if errLease != nil {
			return errLease
		}
		if active {
			continue
		}

		detectedAt := time.Now().Format(time.RFC3339Nano)
		log.Printf(
			"reassigning_abandoned_delivery job_id=%s stream_message_id=%s previous_worker_id=%s reason=%s detected_at=%s",
			msg.Job.JobID,
			p.StreamMessageID,
			p.ConsumerName,
			"lease_expired",
			detectedAt,
		)

		if errSubmit := r.submitter.Submit(ctx, msg.Job); errSubmit != nil {
			return errSubmit
		}
	}

	return nil
}

func (r *Reassigner) Start(ctx context.Context, interval time.Duration, limit int) {
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}

	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = r.ScanAndReassign(ctx, limit)
			}
		}
	}()
}
