package queue

import "context"

type MessageLookup interface {
	GetClaimedMessage(ctx context.Context, streamMessageID string) (*ClaimedMessage, error)
}
