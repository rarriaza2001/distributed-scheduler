package app

import (
	"context"
	"log/slog"
	"time"

	"scheduler/internal/queueredis"
)

// RunQueueAlivePoller updates queue depth and workers-alive gauges until ctx is canceled.
func RunQueueAlivePoller(ctx context.Context, d *Deps, log *slog.Logger) {
	t := d.Config.QueueAlivePollInterval
	if t <= 0 {
		return
	}
	ticker := time.NewTicker(t)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			depth, err := queueredis.ApproximateQueueDepth(ctx, d.Redis, d.Stream)
			if err == nil {
				d.Metric.SetQueueDepth(float64(depth))
			} else if log != nil {
				log.Debug("queue depth poll", "err", err)
			}
			n, err := d.HeartbeatStore.CountAliveAfterPrune(ctx, d.Config.WorkersAliveStale)
			if err == nil {
				d.Metric.SetWorkersAlive(float64(n))
			} else if log != nil {
				log.Debug("alive poll", "err", err)
			}
		}
	}
}
