package observability

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestNewMetrics_DoesNotPanic(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	require.NotNil(t, m)

	_, err := reg.Gather()
	require.NoError(t, err)

	m.IncJobsCompleted()
	m.IncJobsFailed()
	m.IncJobsAbandoned()
	m.IncRetry(RetryReasonExecutionFailure)
	m.IncWorkerHeartbeat()
	m.SetQueueDepth(3)
	m.SetWorkersAlive(2)
	m.SetLeader("s1", 1)
	m.IncLeaderChange("s1")
	m.IncDispatchError()
	m.IncRecoveryError()
	m.ObserveQueueWait(0)
	m.ObserveExecution(0)
	m.ObserveEndToEnd(0)

	_, err = reg.Gather()
	require.NoError(t, err)
}

func TestMetrics_NilReceivers(t *testing.T) {
	var m *Metrics
	m.IncJobsCompleted()
	m.IncRetry(RetryReasonAbandoned)
	m.ObserveQueueWait(0)
	m.SetLeader("x", 0)
}
