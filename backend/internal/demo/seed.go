package demo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"scheduler/internal/runtime"
	"scheduler/internal/store"
)

const (
	JobTypeDemo = "demo"
	QueueDemo   = "demo"
	SourceDemo  = "demo_seed"
)

// Seed inserts demo jobs for the preset via the coordinator (DB + later dispatch by leader).
func Seed(ctx context.Context, coord *runtime.Coordinator, preset Preset) (int, error) {
	inputs, err := jobsForPreset(preset)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, in := range inputs {
		if err := coord.CreateJob(ctx, in, SourceDemo); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

func jobsForPreset(p Preset) ([]store.InsertJobInput, error) {
	now := time.Now().UTC()
	switch p {
	case PresetSmall:
		return []store.InsertJobInput{demoJob(uuid.NewString(), "sleep", []Step{{Op: "sleep", MS: 12000}}, nil, now)}, nil
	case PresetRetry:
		pl := PayloadV1{
			Version: 1,
			Plan:    "retry",
			Meta:    &PayloadMeta{FailUntilAttempt: 2},
			Steps: []Step{
				{Op: "sleep", MS: 2000},
				{Op: "noop"},
			},
		}
		return []store.InsertJobInput{demoJob(uuid.NewString(), "retry", nil, &pl, now)}, nil
	case PresetAbandonment:
		pl := PayloadV1{
			Version: 1,
			Plan:    "abandon",
			Meta:    &PayloadMeta{BreakLeaseAfterMS: 1500},
			Steps: []Step{
				{Op: "sleep", MS: 12000},
			},
		}
		return []store.InsertJobInput{demoJob(uuid.NewString(), "abandon", nil, &pl, now)}, nil
	case PresetMixed:
		plRetry := PayloadV1{Version: 1, Plan: "retry", Meta: &PayloadMeta{FailUntilAttempt: 2}, Steps: []Step{{Op: "sleep", MS: 2500}}}
		plSlow := PayloadV1{Version: 1, Plan: "slow", Steps: []Step{{Op: "sleep", MS: 20000}}}
		plFast := PayloadV1{Version: 1, Plan: "fast", Steps: []Step{{Op: "sleep", MS: 8000}}}
		plAb := PayloadV1{Version: 1, Plan: "abandon", Meta: &PayloadMeta{BreakLeaseAfterMS: 2000}, Steps: []Step{{Op: "sleep", MS: 10000}}}
		return []store.InsertJobInput{
			demoJob(uuid.NewString(), "mixed-a", nil, &plFast, now),
			demoJob(uuid.NewString(), "mixed-b", nil, &plSlow, now),
			demoJob(uuid.NewString(), "mixed-c", nil, &plRetry, now),
			demoJob(uuid.NewString(), "mixed-d", nil, &plAb, now),
		}, nil
	default:
		return nil, fmt.Errorf("demo: unknown preset")
	}
}

func demoJob(id, plan string, steps []Step, full *PayloadV1, now time.Time) store.InsertJobInput {
	var pl PayloadV1
	if full != nil {
		pl = *full
	} else {
		pl = PayloadV1{Version: 1, Plan: plan, Steps: steps}
	}
	if pl.Version == 0 {
		pl.Version = 1
	}
	b, _ := json.Marshal(pl)
	return store.InsertJobInput{
		JobID:       id,
		Queue:       QueueDemo,
		Type:        JobTypeDemo,
		Payload:     b,
		Priority:    0,
		MaxAttempts: 5,
		ScheduledAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}
