package app

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"scheduler/internal/observability"
)

func TestNewServeMux_Healthz(t *testing.T) {
	reg := prometheus.NewRegistry()
	mux := NewServeMux(HTTPOptions{Registry: reg, FailedRecent: observability.NewFailedRecentStore(nil, 10)})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	res, err := http.Get(srv.URL + "/healthz")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)
}

func TestNewServeMux_Metrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	_ = observability.NewMetrics(reg)
	mux := NewServeMux(HTTPOptions{Registry: reg, FailedRecent: observability.NewFailedRecentStore(nil, 10)})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	res, err := http.Get(srv.URL + "/metrics")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)
	body := make([]byte, 256)
	n, _ := res.Body.Read(body)
	require.Contains(t, string(body[:n]), "scheduler")
}

type errLister struct{}

func (errLister) ListRecent(ctx context.Context, limit int) ([]observability.FailedJobMeta, error) {
	return nil, errors.New("redis down")
}

func TestNewServeMux_FailedRecent503(t *testing.T) {
	reg := prometheus.NewRegistry()
	mux := NewServeMux(HTTPOptions{Registry: reg, FailedRecent: errLister{}})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	res, err := http.Get(srv.URL + "/debug/failed-jobs/recent")
	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
}

type okLister struct{}

func (okLister) ListRecent(ctx context.Context, limit int) ([]observability.FailedJobMeta, error) {
	_ = limit
	return []observability.FailedJobMeta{}, nil
}

func TestNewServeMux_FailedRecentLimit(t *testing.T) {
	reg := prometheus.NewRegistry()
	mux := NewServeMux(HTTPOptions{Registry: reg, FailedRecent: okLister{}})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	res, err := http.Get(srv.URL + "/debug/failed-jobs/recent?limit=3")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)
}

func TestNewServeMux_DemoSeedDisabled(t *testing.T) {
	reg := prometheus.NewRegistry()
	mux := NewServeMux(HTTPOptions{Registry: reg, FailedRecent: errLister{}, AllowDemoSeed: false})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	res, err := http.Post(srv.URL+"/debug/demo/seed", "application/json", strings.NewReader(`{"preset":"mixed-demo"}`))
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, res.StatusCode)
}
