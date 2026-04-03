package demo

// PayloadV1 is the JSON shape stored in jobs.payload for demo job types (metadata, not full app payload).
type PayloadV1 struct {
	Version int         `json:"v"`
	Plan    string      `json:"plan"`
	Steps   []Step      `json:"steps,omitempty"`
	Meta    *PayloadMeta `json:"meta,omitempty"`
}

// PayloadMeta carries demo-only flags interpreted by demo-worker (non-production).
type PayloadMeta struct {
	// BreakLeaseAfterMS deletes the Redis per-job lease after N ms of work to simulate abandonment recovery.
	BreakLeaseAfterMS int `json:"break_lease_after_ms,omitempty"`
	// FailUntilAttempt fails handler until attempts_made reaches this (requires DB read in worker).
	FailUntilAttempt int `json:"fail_until_attempt,omitempty"`
}

// Step is one unit of simulated work.
type Step struct {
	Op string `json:"op"` // sleep | noop
	MS int    `json:"ms,omitempty"`
}
