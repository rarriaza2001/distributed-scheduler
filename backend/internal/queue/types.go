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
	IdempotencyKey string
}

type ClaimedMessage struct {
	Stream          string
	StreamMessageID string
	Job             JobMessage
	ConsumerGroup   string
	ConsumerName    string
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
