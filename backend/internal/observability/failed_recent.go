package observability

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	FailedRecentKey        = "scheduler:failed:recent"
	DefaultFailedRecentMax = 300
)

// FailedJobMeta is compact metadata for the inspection ring (no full payload).
type FailedJobMeta struct {
	JobID        string    `json:"job_id"`
	JobType      string    `json:"job_type"`
	Queue        string    `json:"queue"`
	AttemptsMade int       `json:"attempts_made"`
	MaxAttempts  int       `json:"max_attempts"`
	Retryable    bool      `json:"retryable"`
	Terminal     bool      `json:"terminal"`
	ErrorCode    string    `json:"error_code"`
	ShortError   string    `json:"short_error_summary"`
	WorkerID     string    `json:"worker_id"`
	FailedAt     time.Time `json:"failed_at"`
}

// FailedRecentStore appends terminal failure metadata to a capped Redis list (newest at head).
type FailedRecentStore struct {
	client redis.UniversalClient
	max    int
}

// NewFailedRecentStore caps the list at max entries (clamped to >= 1).
func NewFailedRecentStore(client redis.UniversalClient, max int) *FailedRecentStore {
	if max <= 0 {
		max = DefaultFailedRecentMax
	}
	if max > 2000 {
		max = 2000
	}
	return &FailedRecentStore{client: client, max: max}
}

// Push prepends JSON metadata and trims the list.
func (s *FailedRecentStore) Push(ctx context.Context, meta FailedJobMeta) error {
	if s == nil || s.client == nil {
		return nil
	}
	b, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	pipe := s.client.Pipeline()
	pipe.LPush(ctx, FailedRecentKey, string(b))
	pipe.LTrim(ctx, FailedRecentKey, 0, int64(s.max-1))
	_, err = pipe.Exec(ctx)
	return err
}

// ListRecent returns newest-first entries up to limit.
func (s *FailedRecentStore) ListRecent(ctx context.Context, limit int) ([]FailedJobMeta, error) {
	if s == nil || s.client == nil {
		return nil, errors.New("failed recent store not configured")
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > s.max {
		limit = s.max
	}
	raw, err := s.client.LRange(ctx, FailedRecentKey, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}
	out := make([]FailedJobMeta, 0, len(raw))
	for _, line := range raw {
		var m FailedJobMeta
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			continue
		}
		out = append(out, m)
	}
	return out, nil
}

// TruncateShortError caps error text for storage (metadata only).
func TruncateShortError(s string, max int) string {
	if max <= 0 {
		max = 256
	}
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	return string(runes[:max]) + "…"
}

// FormatFailedLine is a single-line debug representation (optional).
func FormatFailedLine(m FailedJobMeta) string {
	return fmt.Sprintf("%s %s queue=%s terminal=%v", m.JobID, m.ErrorCode, m.Queue, m.Terminal)
}
