package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// RetryReason is a bounded label value for scheduler_job_retries_total.
type RetryReason string

const (
	RetryReasonAbandoned        RetryReason = "abandoned"
	RetryReasonExecutionFailure RetryReason = "execution_failure"
	RetryReasonRecoveryRestore  RetryReason = "recovery_restore"
)

// Metrics holds Phase 4 Prometheus collectors. Labels stay low-cardinality; never use job_id or raw errors.
type Metrics struct {
	QueueDepth       prometheus.Gauge
	JobsCompleted    prometheus.Counter
	JobRetries       *prometheus.CounterVec
	WorkerHeartbeat  prometheus.Counter
	WorkersAlive     prometheus.Gauge
	JobsFailed       prometheus.Counter
	JobsAbandoned    prometheus.Counter
	IsLeader         *prometheus.GaugeVec
	LeaderChanges    *prometheus.CounterVec
	QueueWaitSeconds prometheus.Histogram
	ExecutionSeconds prometheus.Histogram
	EndToEndSeconds  prometheus.Histogram
	DispatchErrors   prometheus.Counter
	RecoveryErrors   prometheus.Counter
}

// Default histogram buckets: subsecond through 15 minutes (seconds).
var latencyBuckets = []float64{
	0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 900,
}

// NewMetrics registers all Phase 4 metrics on the given Registerer (e.g. prometheus.DefaultRegisterer).
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		QueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "scheduler_queue_depth",
			Help: "Approximate depth of the job stream (see Redis visibility docs; not strict ready-to-claim depth).",
		}),
		JobsCompleted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_jobs_completed_total",
			Help: "Jobs reached terminal success.",
		}),
		JobRetries: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "scheduler_job_retries_total",
				Help: "Retry scheduling events (bounded reason label).",
			},
			[]string{"reason"},
		),
		WorkerHeartbeat: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_worker_heartbeat_total",
			Help: "Successful worker heartbeat beats.",
		}),
		WorkersAlive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "scheduler_workers_alive",
			Help: "Workers with recent heartbeat in the aggregate alive set (after stale prune).",
		}),
		JobsFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_jobs_failed_total",
			Help: "Terminal job failures only (dead letter), not each failed attempt.",
		}),
		JobsAbandoned: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_jobs_abandoned_total",
			Help: "Jobs requeued from abandoned running state (recovery).",
		}),
		IsLeader: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "scheduler_is_leader",
				Help: "1 if this process holds the scheduler leader lease, else 0.",
			},
			[]string{"scheduler_id"},
		),
		LeaderChanges: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "scheduler_leader_changes_total",
				Help: "Leadership transitions (acquire or step-down).",
			},
			[]string{"scheduler_id"},
		),
		QueueWaitSeconds: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "scheduler_job_queue_wait_seconds",
				Help:    "Time from job eligibility to worker start (see PHASE4 docs).",
				Buckets: latencyBuckets,
			},
		),
		ExecutionSeconds: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "scheduler_job_execution_seconds",
				Help:    "Time from worker start to completion (success or retry/terminal outcome).",
				Buckets: latencyBuckets,
			},
		),
		EndToEndSeconds: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "scheduler_job_end_to_end_seconds",
				Help:    "Time from job creation to terminal or retry-scheduled outcome at worker report.",
				Buckets: latencyBuckets,
			},
		),
		DispatchErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_dispatch_tick_errors_total",
			Help: "Errors during leader dispatch tick (bounded aggregate).",
		}),
		RecoveryErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_recovery_tick_errors_total",
			Help: "Errors during leader recovery/reconcile tick (bounded aggregate).",
		}),
	}

	reg.MustRegister(
		m.QueueDepth,
		m.JobsCompleted,
		m.JobRetries,
		m.WorkerHeartbeat,
		m.WorkersAlive,
		m.JobsFailed,
		m.JobsAbandoned,
		m.IsLeader,
		m.LeaderChanges,
		m.QueueWaitSeconds,
		m.ExecutionSeconds,
		m.EndToEndSeconds,
		m.DispatchErrors,
		m.RecoveryErrors,
	)
	return m
}

// IncRetry increments retries with a bounded reason.
func (m *Metrics) IncRetry(reason RetryReason) {
	if m == nil {
		return
	}
	m.JobRetries.WithLabelValues(string(reason)).Inc()
}

// ObserveQueueWait records queue wait duration.
func (m *Metrics) ObserveQueueWait(d time.Duration) {
	if m == nil {
		return
	}
	m.QueueWaitSeconds.Observe(d.Seconds())
}

// ObserveExecution records execution segment duration.
func (m *Metrics) ObserveExecution(d time.Duration) {
	if m == nil {
		return
	}
	m.ExecutionSeconds.Observe(d.Seconds())
}

// ObserveEndToEnd records end-to-end duration.
func (m *Metrics) ObserveEndToEnd(d time.Duration) {
	if m == nil {
		return
	}
	m.EndToEndSeconds.Observe(d.Seconds())
}

// SetLeader sets leader gauge for this scheduler id (0 or 1).
func (m *Metrics) SetLeader(schedulerID string, v float64) {
	if m == nil {
		return
	}
	m.IsLeader.WithLabelValues(schedulerID).Set(v)
}

// IncLeaderChange counts a leadership transition.
func (m *Metrics) IncLeaderChange(schedulerID string) {
	if m == nil {
		return
	}
	m.LeaderChanges.WithLabelValues(schedulerID).Inc()
}

func (m *Metrics) IncJobsCompleted() {
	if m == nil {
		return
	}
	m.JobsCompleted.Inc()
}

func (m *Metrics) IncJobsFailed() {
	if m == nil {
		return
	}
	m.JobsFailed.Inc()
}

func (m *Metrics) IncJobsAbandoned() {
	if m == nil {
		return
	}
	m.JobsAbandoned.Inc()
}

func (m *Metrics) IncWorkerHeartbeat() {
	if m == nil {
		return
	}
	m.WorkerHeartbeat.Inc()
}

func (m *Metrics) SetQueueDepth(v float64) {
	if m == nil {
		return
	}
	m.QueueDepth.Set(v)
}

func (m *Metrics) SetWorkersAlive(v float64) {
	if m == nil {
		return
	}
	m.WorkersAlive.Set(v)
}

func (m *Metrics) IncDispatchError() {
	if m == nil {
		return
	}
	m.DispatchErrors.Inc()
}

func (m *Metrics) IncRecoveryError() {
	if m == nil {
		return
	}
	m.RecoveryErrors.Inc()
}
