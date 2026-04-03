package demo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"scheduler/internal/queue"
	"scheduler/internal/store"
)

// ExecuteDemoJob runs a demo job payload (non-production). Returns nil for non-demo types.
func ExecuteDemoJob(ctx context.Context, pool *pgxpool.Pool, jobs store.JobStore, rdb redis.UniversalClient, msg queue.JobMessage) error {
	if msg.Type != JobTypeDemo {
		return fmt.Errorf("%w: type=%q", ErrNotDemo, msg.Type)
	}
	var pl PayloadV1
	if err := json.Unmarshal(msg.Payload, &pl); err != nil {
		return err
	}
	j, err := jobs.GetJobByID(ctx, pool, msg.JobID)
	if err != nil {
		return err
	}

	if pl.Meta != nil && pl.Meta.FailUntilAttempt > 0 {
		// Fail until attempts_made reaches FailUntilAttempt-1 (see DEMO_RUNBOOK).
		if j.AttemptsMade < pl.Meta.FailUntilAttempt-1 {
			return fmt.Errorf("demo_retryable_failure: attempt %d", j.AttemptsMade)
		}
	}

	if pl.Meta != nil && pl.Meta.BreakLeaseAfterMS > 0 {
		delay := time.Duration(pl.Meta.BreakLeaseAfterMS) * time.Millisecond
		key := fmt.Sprintf("job:%s:lease", msg.JobID)
		time.AfterFunc(delay, func() {
			_ = rdb.Del(context.Background(), key).Err()
		})
	}

	for _, step := range pl.Steps {
		switch step.Op {
		case "noop":
		case "sleep":
			d := time.Duration(step.MS) * time.Millisecond
			if d <= 0 {
				d = time.Second
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(d):
			}
		default:
		}
	}
	return nil
}

// ErrNotDemo indicates the message type is not a demo job (caller may run default handler).
var ErrNotDemo = errors.New("demo: not a demo job")
