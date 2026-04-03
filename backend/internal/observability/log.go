package observability

import (
	"log/slog"
)

// Logger returns a default JSON-ish friendly slog logger; callers may replace with their own.
func Logger() *slog.Logger {
	return slog.Default()
}

// Attr helpers for stable structured fields (logs only — never use these as metric labels).

const (
	EventLeaderAcquired      = "leader_acquired"
	EventLeaderRenewFailed   = "leader_renew_failed"
	EventLeaderStepDown      = "leader_step_down"
	EventLeaderLost          = "leader_lost"
	EventDispatchTickError   = "dispatch_tick_error"
	EventRecoveryTickError   = "recovery_tick_error"
	EventAbandonedJob        = "abandoned_job_detected"
	EventHeartbeatError      = "worker_heartbeat_error"
	EventLeaseAcquireError   = "worker_lease_acquire_error"
	EventLeaseRenewError     = "worker_lease_renew_error"
	EventLeaseReleaseError   = "worker_lease_release_error"
	EventAckFailure          = "ack_failure"
	EventWorkerExecFailure   = "worker_execution_failure"
	EventFailedJobStoreError = "failed_job_inspection_write_error"
)
