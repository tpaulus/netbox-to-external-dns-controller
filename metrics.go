package main

import (
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	metricsOnce sync.Once
	metrics     *syncMetrics
)

type syncMetrics struct {
	syncResults         *prometheus.CounterVec
	recordChanges       *prometheus.CounterVec
	sourceFetchDuration prometheus.Histogram
	lastSuccessUnix     prometheus.Gauge
}

func getMetrics() *syncMetrics {
	metricsOnce.Do(func() {
		metrics = &syncMetrics{
			syncResults: prometheus.NewCounterVec(
				prometheus.CounterOpts{Name: "netbox_dns_sync_runs_total", Help: "Count of sync runs by result."},
				[]string{"result"},
			),
			recordChanges: prometheus.NewCounterVec(
				prometheus.CounterOpts{Name: "netbox_dns_sync_records_total", Help: "Count of reconciled records by operation."},
				[]string{"operation"},
			),
			sourceFetchDuration: prometheus.NewHistogram(
				prometheus.HistogramOpts{Name: "netbox_dns_sync_source_fetch_seconds", Help: "Duration of NetBox source fetches."},
			),
			lastSuccessUnix: prometheus.NewGauge(
				prometheus.GaugeOpts{Name: "netbox_dns_sync_last_success_timestamp", Help: "Unix timestamp of the last successful sync."},
			),
		}
		prometheus.MustRegister(metrics.syncResults, metrics.recordChanges, metrics.sourceFetchDuration, metrics.lastSuccessUnix)
	})

	return metrics
}

func startMetricsServer(address string) *http.Server {
	if address == "" {
		return nil
	}
	server := &http.Server{
		Addr:              address,
		Handler:           promhttp.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		_ = server.ListenAndServe()
	}()
	return server
}
