package scheduler

import (
	"testing"
	"time"

	"scheduler/internal/store"

	"github.com/stretchr/testify/require"
)

func TestMergeDispatchCandidates_ordering(t *testing.T) {
	t1 := time.Date(2026, 1, 2, 15, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)

	queued := []store.Job{
		{JobID: "a", Status: store.JobStatusQueued, ScheduledAt: t1, Priority: 0},
		{JobID: "b", Status: store.JobStatusQueued, ScheduledAt: t2, Priority: 0},
	}
	failed := []store.Job{
		{JobID: "c", Status: store.JobStatusFailed, ScheduledAt: t1.Add(30 * time.Minute), Priority: 99},
	}

	out := mergeDispatchCandidates(queued, failed, 10)
	require.Len(t, out, 3)
	require.Equal(t, "a", out[0].JobID)
	require.Equal(t, "c", out[1].JobID)
	require.Equal(t, "b", out[2].JobID)
}

func TestMergeDispatchCandidates_respectsLimit(t *testing.T) {
	t0 := time.Now().UTC()
	q := []store.Job{{JobID: "1", ScheduledAt: t0}, {JobID: "2", ScheduledAt: t0.Add(time.Minute)}}
	f := []store.Job{{JobID: "3", ScheduledAt: t0.Add(2 * time.Minute)}}
	out := mergeDispatchCandidates(q, f, 2)
	require.Len(t, out, 2)
}
