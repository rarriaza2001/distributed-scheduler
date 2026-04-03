package queueredis

import (
	"context"

	"github.com/redis/go-redis/v9"

	"scheduler/internal/queue"
)

type RedisPendingInspector struct {
	client        redis.UniversalClient
	stream        string
	consumerGroup string
}

func NewRedisPendingInspector(client redis.UniversalClient, stream, consumerGroup string) *RedisPendingInspector {
	return &RedisPendingInspector{
		client:        client,
		stream:        stream,
		consumerGroup: consumerGroup,
	}
}

func (r *RedisPendingInspector) ListPending(ctx context.Context, count int) ([]queue.PendingMessage, error) {
	if count <= 0 {
		count = 100
	}

	result, err := r.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: r.stream,
		Group:  r.consumerGroup,
		Start:  "-",
		End:    "+",
		Count:  int64(count),
	}).Result()
	if err != nil {
		return nil, err
	}

	out := make([]queue.PendingMessage, 0, len(result))
	for _, p := range result {
		out = append(out, queue.PendingMessage{
			Stream:          r.stream,
			StreamMessageID: p.ID,
			ConsumerGroup:   r.consumerGroup,
			ConsumerName:    p.Consumer,
			JobID:           "",
		})
	}

	return out, nil
}
