package queue

import "context"

// PendingInspector lists deliveries still in the consumer group's pending entries list (PEL).
type PendingInspector interface {
	ListPending(ctx context.Context, count int) ([]PendingMessage, error)
}
