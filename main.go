package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	runID := newRunID()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})).With("run_id", runID)
	logger.Info("starting netbox dns sync", "namespace", cfg.Namespace, "dry_run", cfg.DryRun, "sync_interval", cfg.SyncInterval.String())

	metricsServer := startMetricsServer(cfg.MetricsAddress)
	defer func() {
		if metricsServer != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = metricsServer.Shutdown(ctx)
		}
	}()

	netboxClient, err := newNetBoxClient(cfg)
	if err != nil {
		return err
	}
	reconciler, err := newReconciler(cfg, logger)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	execute := func() error {
		fetch, err := netboxClient.fetchRecords(ctx, cfg, logger)
		if err != nil {
			getMetrics().syncResults.WithLabelValues("failure").Inc()
			logger.Error("failed to fetch NetBox records", "error", err)
			return err
		}
		summary, err := reconciler.reconcile(ctx, fetch.Records, fetch.SourceHealthy, fetch.RecordErrors)
		if err != nil {
			getMetrics().syncResults.WithLabelValues("failure").Inc()
			logger.Error("reconciliation failed", "error", err, "record_errors", len(fetch.RecordErrors))
			return err
		}
		getMetrics().syncResults.WithLabelValues("success").Inc()
		getMetrics().recordChanges.WithLabelValues("created").Add(float64(summary.Created))
		getMetrics().recordChanges.WithLabelValues("updated").Add(float64(summary.Updated))
		getMetrics().recordChanges.WithLabelValues("deleted").Add(float64(summary.Deleted))
		getMetrics().recordChanges.WithLabelValues("unchanged").Add(float64(summary.Unchanged))
		getMetrics().lastSuccessUnix.Set(float64(time.Now().Unix()))
		logger.Info("sync complete",
			"created", summary.Created,
			"updated", summary.Updated,
			"deleted", summary.Deleted,
			"unchanged", summary.Unchanged,
			"skipped_delete", summary.SkippedDelete,
			"record_errors", summary.RecordErrors,
			"source_healthy", fetch.SourceHealthy,
		)
		return nil
	}

	if cfg.SyncInterval <= 0 {
		return execute()
	}

	if err := execute(); err != nil && !errors.Is(err, context.Canceled) {
		logger.Warn("initial sync iteration failed", "error", err)
	}

	ticker := time.NewTicker(cfg.SyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := execute(); err != nil && !errors.Is(err, context.Canceled) {
				logger.Warn("scheduled sync iteration failed", "error", err)
			}
		}
	}
}

func newRunID() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}
