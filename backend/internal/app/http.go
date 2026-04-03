package app

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"scheduler/internal/demo"
	"scheduler/internal/observability"
	"scheduler/internal/runtime"
)

// RecentFailedLister is implemented by observability.FailedRecentStore.
type RecentFailedLister interface {
	ListRecent(ctx context.Context, limit int) ([]observability.FailedJobMeta, error)
}

// DemoSeeder seeds demo jobs (implemented by coordinatorSeeder).
type DemoSeeder interface {
	SeedPreset(ctx context.Context, preset demo.Preset) (int, error)
}

type coordinatorSeeder struct {
	c *runtime.Coordinator
}

func (s *coordinatorSeeder) SeedPreset(ctx context.Context, preset demo.Preset) (int, error) {
	return demo.Seed(ctx, s.c, preset)
}

// HTTPOptions configures the debug mux.
type HTTPOptions struct {
	Log           *slog.Logger
	Registry      prometheus.Gatherer // *prometheus.Registry implements Gatherer
	FailedRecent  RecentFailedLister
	Coordinator   *runtime.Coordinator
	AllowDemoSeed bool
}

// NewServeMux registers /metrics, /healthz, /debug/failed-jobs/recent, and optionally POST /debug/demo/seed.
func NewServeMux(opts HTTPOptions) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(opts.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", handleHealthz)
	mux.HandleFunc("/debug/failed-jobs/recent", func(w http.ResponseWriter, r *http.Request) {
		handleFailedRecent(w, r, opts.FailedRecent)
	})

	if opts.AllowDemoSeed && opts.Coordinator != nil {
		seeder := &coordinatorSeeder{opts.Coordinator}
		mux.HandleFunc("/debug/demo/seed", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			handleDemoSeed(w, r, seeder, opts.Log)
		})
	}

	return mux
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func handleFailedRecent(w http.ResponseWriter, r *http.Request, store RecentFailedLister) {
	limit := 50
	if s := r.URL.Query().Get("limit"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			limit = n
		}
	}
	list, err := store.ListRecent(r.Context(), limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(list)
}

type seedRequest struct {
	Preset string `json:"preset"`
}

func handleDemoSeed(w http.ResponseWriter, r *http.Request, seeder DemoSeeder, log *slog.Logger) {
	var body seedRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	p := demo.PresetFromString(body.Preset)
	if p == demo.PresetUnknown {
		http.Error(w, "unknown preset", http.StatusBadRequest)
		return
	}
	n, err := seeder.SeedPreset(r.Context(), p)
	if err != nil {
		if log != nil {
			log.Error("demo seed failed", "preset", body.Preset, "err", err)
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"inserted": n, "preset": body.Preset})
}
