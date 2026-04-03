package scheduler

import (
	"context"
	"log/slog"
	"time"

	"scheduler/internal/queue"
)

// Reassigner (legacy Phase 2): abandonment = delivery still pending in Redis + per-job lease inactive.
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
	if r == nil || r.pendingInspector == nil || r.messageLookup == nil || r.submitter == nil {
		return nil
	}
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
		slog.Default().Info("reassigning_abandoned_delivery",
			"job_id", msg.Job.JobID,
			"stream_message_id", p.StreamMessageID,
			"previous_worker_id", p.ConsumerName,
			"reason", "lease_expired",
			"detected_at", detectedAt,
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
