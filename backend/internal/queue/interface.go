package queue

import (
	"context"
	"time"
)

// Queue is the boundary for queue transport operations (submit / claim / ack).
type Queue interface {
	Submit(ctx context.Context, msg JobMessage) error
	Claim(ctx context.Context, workerID string, count int, block time.Duration) ([]ClaimedMessage, error)
	Ack(ctx context.Context, workerID string, messages ...ClaimedMessage) error
}

type RetryScheduler interface {
	ScheduleRetry(ctx context.Context, req RetryRequest) error
}
