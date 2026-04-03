package observability

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestMetrics_RetryVsTerminalFailure(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.IncRetry(RetryReasonExecutionFailure)
	require.Equal(t, 1.0, testutil.ToFloat64(m.JobRetries.WithLabelValues(string(RetryReasonExecutionFailure))))

	m.IncJobsFailed()
	require.Equal(t, 1.0, testutil.ToFloat64(m.JobsFailed))
	m.IncRetry(RetryReasonExecutionFailure)
	require.Equal(t, 2.0, testutil.ToFloat64(m.JobRetries.WithLabelValues(string(RetryReasonExecutionFailure))))
}

func TestMetrics_LeaderGauges(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.SetLeader("s1", 1)
	m.IncLeaderChange("s1")
	require.Equal(t, 1.0, testutil.ToFloat64(m.IsLeader.WithLabelValues("s1")))
}
