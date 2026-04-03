// Phase 4 service: HTTP metrics/health/debug, scheduler leader, gauge polling.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"scheduler/internal/app"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	cfg := app.LoadConfigFromEnv()
	if err := cfg.Validate(); err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	deps, err := app.Bootstrap(ctx, cfg, log)
	if err != nil {
		log.Error("bootstrap", "err", err)
		os.Exit(1)
	}
	defer deps.Pool.Close()
	defer func() { _ = deps.Redis.Close() }()

	mux := app.NewServeMux(app.HTTPOptions{
		Log:           log,
		Registry:      deps.Reg,
		FailedRecent:  deps.FailedRecent,
		Coordinator:   deps.Coordinator,
		AllowDemoSeed: cfg.AllowDemoEndpoints,
	})

	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		deps.Leader.Start(ctx)
	}()

	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		app.RunQueueAlivePoller(ctx, deps, log)
	}()

	srv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: mux,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http", "err", err)
			stop()
		}
	}()

	log.Info("service listening", "addr", cfg.HTTPAddr, "demo_endpoints", cfg.AllowDemoEndpoints)

	<-ctx.Done()
	log.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.HTTPShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown", "err", err)
	}
	<-leaderDone
	<-pollDone
	log.Info("shutdown complete")
}
