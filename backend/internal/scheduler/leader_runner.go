package scheduler

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"scheduler/internal/observability"
)

// SchedulerLeader coordinates leadership acquisition and renewal for the
// scheduler control plane. It gates optional legacy PEL reassignment so that only
// the current leader runs it when configured.
//
// This is lease-based leadership on top of Redis, not consensus: brief
// split-brain windows remain possible under faults, so duplicate scheduler
// actions are still accepted and correctness relies on idempotent job
// handling and at-least-once delivery.
//
// Default Phase 4 tick order (when wired): ReconcileOnTick (DB recovery), then
// DispatchOnTick (DB dispatch + enqueue), then optional legacy Reassigner scan.
type SchedulerLeader struct {
	id              string
	lease           LeaderLease
	reassigner      *Reassigner // legacy Phase 2; nil disables PEL resubmission on the hot path
	leaseTTLSeconds int

	renewInterval    time.Duration
	acquireInterval  time.Duration
	scanInterval     time.Duration
	scanLimitPerTick int

	// ReconcileOnTick is optional DB-authoritative recovery (e.g. Reconciler.Reconcile).
	ReconcileOnTick func(ctx context.Context) error
	// DispatchOnTick is optional leader-gated dispatch + transport enqueue.
	DispatchOnTick func(ctx context.Context) error

	metrics *observability.Metrics
	log     *slog.Logger

	isLeader atomic.Bool
}

type SchedulerLeaderConfig struct {
	LeaseTTLSeconds  int
	RenewInterval    time.Duration
	AcquireInterval  time.Duration
	ScanInterval     time.Duration
	ScanLimitPerTick int
}

// NewSchedulerLeader constructs a leader loop. reassigner may be nil to disable legacy PEL reassignment.
func NewSchedulerLeader(id string, lease LeaderLease, reassigner *Reassigner, cfg SchedulerLeaderConfig) *SchedulerLeader {
	if cfg.LeaseTTLSeconds <= 0 {
		cfg.LeaseTTLSeconds = 5
	}
	if cfg.RenewInterval <= 0 {
		cfg.RenewInterval = 2 * time.Second
	}
	if cfg.AcquireInterval <= 0 {
		cfg.AcquireInterval = time.Second
	}
	if cfg.ScanInterval <= 0 {
		cfg.ScanInterval = 250 * time.Millisecond
	}
	if cfg.ScanLimitPerTick <= 0 {
		cfg.ScanLimitPerTick = 100
	}

	return &SchedulerLeader{
		id:               id,
		lease:            lease,
		reassigner:       reassigner,
		leaseTTLSeconds:  cfg.LeaseTTLSeconds,
		renewInterval:    cfg.RenewInterval,
		acquireInterval:  cfg.AcquireInterval,
		scanInterval:     cfg.ScanInterval,
		scanLimitPerTick: cfg.ScanLimitPerTick,
	}
}

// SetObservability attaches metrics and structured logging for leadership transitions and tick errors.
func (s *SchedulerLeader) SetObservability(m *observability.Metrics, log *slog.Logger) {
	s.metrics = m
	s.log = log
}

// IsLeader returns whether this instance believes it is the active leader.
// Used to gate scheduling-related DB work; it may briefly disagree with peers under faults.
func (s *SchedulerLeader) IsLeader() bool {
	return s.isLeader.Load()
}

// Start runs the leader state machine until ctx is canceled.
func (s *SchedulerLeader) Start(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		if !s.isLeader.Load() {
			ok, err := s.lease.Acquire(ctx, s.id, s.leaseTTLSeconds)
			if err == nil && ok {
				if s.metrics != nil {
					s.metrics.SetLeader(s.id, 1)
					s.metrics.IncLeaderChange(s.id)
				}
				if s.log != nil {
					s.log.Info(observability.EventLeaderAcquired, "event", observability.EventLeaderAcquired, "scheduler_id", s.id)
				}
				s.runLeader(ctx)
				continue
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(s.acquireInterval):
			}
			continue
		}
	}
}

func (s *SchedulerLeader) runLeader(parent context.Context) {
	leaderCtx, cancel := context.WithCancel(parent)
	defer cancel()

	s.isLeader.Store(true)
	defer func() {
		s.isLeader.Store(false)
		if s.metrics != nil {
			s.metrics.SetLeader(s.id, 0)
		}
		if s.log != nil {
			s.log.Info(observability.EventLeaderLost, "event", observability.EventLeaderLost, "scheduler_id", s.id)
		}
	}()

	go func() {
		ticker := time.NewTicker(s.renewInterval)
		defer ticker.Stop()

		for {
			select {
			case <-leaderCtx.Done():
				return
			case <-ticker.C:
				ok, err := s.lease.Renew(leaderCtx, s.id, s.leaseTTLSeconds)
				if err != nil || !ok {
					s.isLeader.Store(false)
					if s.metrics != nil {
						s.metrics.SetLeader(s.id, 0)
						s.metrics.IncLeaderChange(s.id)
					}
					if s.log != nil {
						s.log.Warn(observability.EventLeaderRenewFailed, "event", observability.EventLeaderRenewFailed,
							"scheduler_id", s.id, "ok", ok, "err", err)
						s.log.Info(observability.EventLeaderStepDown, "event", observability.EventLeaderStepDown, "scheduler_id", s.id)
					}
					cancel()
					return
				}
			}
		}
	}()

	ticker := time.NewTicker(s.scanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-leaderCtx.Done():
			return
		case <-ticker.C:
			if !s.isLeader.Load() {
				continue
			}
			if s.ReconcileOnTick != nil {
				if err := s.ReconcileOnTick(leaderCtx); err != nil {
					if s.metrics != nil {
						s.metrics.IncRecoveryError()
					}
					if s.log != nil {
						s.log.Error(observability.EventRecoveryTickError, "event", observability.EventRecoveryTickError, "scheduler_id", s.id, "err", err)
					}
				}
			}
			if s.DispatchOnTick != nil {
				if err := s.DispatchOnTick(leaderCtx); err != nil {
					if s.metrics != nil {
						s.metrics.IncDispatchError()
					}
					if s.log != nil {
						s.log.Error(observability.EventDispatchTickError, "event", observability.EventDispatchTickError, "scheduler_id", s.id, "err", err)
					}
				}
			}
			if s.reassigner != nil {
				_ = s.reassigner.ScanAndReassign(leaderCtx, s.scanLimitPerTick)
			}
		}
	}
}
