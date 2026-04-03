package queueredis

import (
	"context"

	"github.com/redis/go-redis/v9"

	"scheduler/internal/queue"
)

type RedisMessageLookup struct {
	client        redis.UniversalClient
	stream        string
	consumerGroup string
}

func NewRedisMessageLookup(client redis.UniversalClient, stream, consumerGroup string) *RedisMessageLookup {
	return &RedisMessageLookup{
		client:        client,
		stream:        stream,
		consumerGroup: consumerGroup,
	}
}

func (r *RedisMessageLookup) GetClaimedMessage(ctx context.Context, streamMessageID string) (*queue.ClaimedMessage, error) {
	result, err := r.client.XRangeN(ctx, r.stream, streamMessageID, streamMessageID, 1).Result()
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, nil
	}

	msg := result[0]
	job, err := decodeJobMessage(msg.Values)
	if err != nil {
		return nil, err
	}

	return &queue.ClaimedMessage{
		Stream:          r.stream,
		StreamMessageID: msg.ID,
		ConsumerGroup:   r.consumerGroup,
		ConsumerName:    "",
		Job:             job,
	}, nil
}
