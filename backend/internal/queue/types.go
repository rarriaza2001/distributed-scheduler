package queue

import (
	"time"
)

type JobMessage struct {
	JobID          string
	Queue          string
	Type           string
	Payload        []byte
	Priority       int
	ScheduledAt    time.Time
	CreatedAt      time.Time // job row creation time (Unix ms on wire); zero if legacy message
	IdempotencyKey string
}

type ClaimedMessage struct {
	Stream          string
	StreamMessageID string
	Job             JobMessage
	ConsumerGroup   string
	ConsumerName    string
	// StartedAt is set by Worker.ExecuteOne when the handler begins (worker-local clock).
	StartedAt time.Time
}

type RetryRequest struct {
	JobID     string
	RetryAt   time.Time
	Attempt   int
	LastError string
	Retryable bool
}

type PendingMessage struct {
	Stream          string
	StreamMessageID string
	ConsumerGroup   string
	ConsumerName    string
	JobID           string
}
