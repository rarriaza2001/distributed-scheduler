package queueredis

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"scheduler/internal/queue"
)

var ErrInvalidMessageField = errors.New("invalid message field type")

type RedisStreamQueue struct {
	client        redis.UniversalClient
	stream        string
	consumerGroup string
}

func (q *RedisStreamQueue) Submit(ctx context.Context, msg queue.JobMessage) error {
	return q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: q.stream,
		Values: map[string]any{
			"job_id":          msg.JobID,
			"queue":           msg.Queue,
			"type":            msg.Type,
			"payload":         string(msg.Payload),
			"priority":        strconv.Itoa(msg.Priority),
			"scheduled_at":    strconv.FormatInt(msg.ScheduledAt.UnixMilli(), 10),
			"created_at":      strconv.FormatInt(msg.CreatedAt.UnixMilli(), 10),
			"idempotency_key": msg.IdempotencyKey,
		},
	}).Err()
}

func (q *RedisStreamQueue) Claim(ctx context.Context, workerID string, count int, block time.Duration) ([]queue.ClaimedMessage, error) {
	result, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    q.consumerGroup,
		Consumer: workerID,
		Streams:  []string{q.stream, ">"},
		Count:    int64(count),
		Block:    block,
	}).Result()
	if err != nil {
		return nil, err
	}
	return mapStreamMessages(result, q.consumerGroup, workerID)
}

func (q *RedisStreamQueue) Ack(ctx context.Context, workerID string, messages ...queue.ClaimedMessage) error {
	_ = workerID

	ids := make([]string, 0, len(messages))
	for _, m := range messages {
		ids = append(ids, m.StreamMessageID)
	}

	if len(ids) == 0 {
		return nil
	}

	return q.client.XAck(ctx, q.stream, q.consumerGroup, ids...).Err()
}

func mapStreamMessages(streams []redis.XStream, group string, workerID string) ([]queue.ClaimedMessage, error) {
	claimed := make([]queue.ClaimedMessage, 0)

	for _, stream := range streams {
		for _, message := range stream.Messages {
			job, err := decodeJobMessage(message.Values)
			if err != nil {
				return nil, err
			}

			claimed = append(claimed, queue.ClaimedMessage{
				Stream:          stream.Stream,
				StreamMessageID: message.ID,
				ConsumerGroup:   group,
				ConsumerName:    workerID,
				Job:             job,
			})
		}
	}

	return claimed, nil
}

func decodeJobMessage(values map[string]any) (queue.JobMessage, error) {
	jobID, err := asString(values["job_id"])
	if err != nil {
		return queue.JobMessage{}, err
	}
	queueName, err := asString(values["queue"])
	if err != nil {
		return queue.JobMessage{}, err
	}
	jobType, err := asString(values["type"])
	if err != nil {
		return queue.JobMessage{}, err
	}
	payloadStr, err := asString(values["payload"])
	if err != nil {
		return queue.JobMessage{}, err
	}
	priorityStr, err := asString(values["priority"])
	if err != nil {
		return queue.JobMessage{}, err
	}
	scheduledAtStr, err := asString(values["scheduled_at"])
	if err != nil {
		return queue.JobMessage{}, err
	}
	idempotencyKey, err := asString(values["idempotency_key"])
	if err != nil {
		return queue.JobMessage{}, err
	}

	priority, err := strconv.Atoi(priorityStr)
	if err != nil {
		return queue.JobMessage{}, err
	}
	scheduledAtMs, err := strconv.ParseInt(scheduledAtStr, 10, 64)
	if err != nil {
		return queue.JobMessage{}, err
	}

	var createdAt time.Time
	if v, ok := values["created_at"]; ok {
		createdStr, err := asString(v)
		if err == nil && createdStr != "" {
			if ms, err := strconv.ParseInt(createdStr, 10, 64); err == nil {
				createdAt = time.UnixMilli(ms)
			}
		}
	}

	return queue.JobMessage{
		JobID:          jobID,
		Queue:          queueName,
		Type:           jobType,
		Payload:        []byte(payloadStr),
		Priority:       priority,
		ScheduledAt:    time.UnixMilli(scheduledAtMs),
		CreatedAt:      createdAt,
		IdempotencyKey: idempotencyKey,
	}, nil
}

func asString(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case []byte:
		return string(t), nil
	default:
		return "", ErrInvalidMessageField
	}
}

// NewRedisStreamQueue creates a consumer group on the stream (if needed) and returns a Redis-backed queue.Queue.
func NewRedisStreamQueue(ctx context.Context, client redis.UniversalClient, stream string, consumerGroup string) (*RedisStreamQueue, error) {
	q := &RedisStreamQueue{client: client, stream: stream, consumerGroup: consumerGroup}
	err := q.client.XGroupCreateMkStream(ctx, q.stream, q.consumerGroup, "$").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return nil, err
	}
	return q, nil
}
