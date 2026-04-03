package scheduler

import (
	"context"
	"sync/atomic"
	"time"
)

// SchedulerLeader coordinates leadership acquisition and renewal for the
// scheduler control plane. It gates the reassignment scan loop so that only
// the current leader performs abandonment detection and resubmission.
//
// This is lease-based leadership on top of Redis, not consensus: brief
// split-brain windows remain possible under faults, so duplicate scheduler
// actions are still accepted and correctness relies on idempotent job
// handling and at-least-once delivery.
type SchedulerLeader struct {
	id              string
	lease           LeaderLease
	reassigner      *Reassigner
	leaseTTLSeconds int

	renewInterval    time.Duration
	acquireInterval  time.Duration
	scanInterval     time.Duration
	scanLimitPerTick int

	// DispatchOnTick is optional DB dispatch / transport enqueue run while leader (before PEL reassignment).
	DispatchOnTick func(ctx context.Context) error

	isLeader atomic.Bool
}

type SchedulerLeaderConfig struct {
	LeaseTTLSeconds  int
	RenewInterval    time.Duration
	AcquireInterval  time.Duration
	ScanInterval     time.Duration
	ScanLimitPerTick int
}

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
	defer s.isLeader.Store(false)

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
			if s.DispatchOnTick != nil {
				_ = s.DispatchOnTick(leaderCtx)
			}
			_ = s.reassigner.ScanAndReassign(leaderCtx, s.scanLimitPerTick)
		}
	}
}
