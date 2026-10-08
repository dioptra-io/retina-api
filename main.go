// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

// @title			IP Routes Live API
// @version		2.0.0
// @description	Streams forwarding info elements (FIEs) collected by Retina agents, as newline-delimited JSON.
// @host			iprl.dioptra.io
// @BasePath		/api/v1
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	_ "github.com/dioptra-io/retina-api/docs"
	"github.com/dioptra-io/retina-api/internal/api"
)

func main() {
	if err := run(); err != nil {
		slog.Error("retina-api error", "err", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		publicAddr        = flag.String("public-addr", envOrDefault("RETINA_API_PUBLIC_ADDR", "127.0.0.1:8080"), "Public listener address")
		metricsAddr       = flag.String("metrics-addr", envOrDefault("RETINA_API_METRICS_ADDR", "127.0.0.1:9312"), "Internal-only address for /metrics (scraped locally, not exposed)")
		readHeaderTimeout = flag.Duration("read-header-timeout", envOrDefaultDuration("RETINA_API_READ_HEADER_TIMEOUT", 5*time.Second), "Timeout for reading HTTP request headers on the public listener (ingest is raw TCP, unaffected)")

		ingestAddr   = flag.String("ingest-addr", envOrDefault("RETINA_API_INGEST_ADDR", "127.0.0.1:8123"), "Ingest listener for the orchestrator")
		ringCapacity = flag.Int("ring-capacity", envOrDefaultInt("RETINA_API_RING_CAPACITY", 100), "Ring buffer capacity")

		logLevel = flag.String("log-level", envOrDefault("RETINA_API_LOG_LEVEL", "info"), "Log level (debug, info, warn, error)")
	)
	flag.Parse()

	logger := newLogger(*logLevel)

	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector())
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	metrics := api.NewMetrics(registry)
	metricsSrv, err := startMetricsServer(logger, registry, *metricsAddr)
	if err != nil {
		return err
	}

	server, err := api.NewServer(&api.ServerConfig{
		PublicAddress:     *publicAddr,
		IngestAddress:     *ingestAddr,
		ReadHeaderTimeout: *readHeaderTimeout,
		RingCapacity:      *ringCapacity,
		Logger:            logger,
		Metrics:           metrics,
	})
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	logger.Info("Starting retina-api",
		"public_addr", *publicAddr,
		"metrics_addr", *metricsAddr,
		"ingest_addr", *ingestAddr,
		"log_level", *logLevel,
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := server.ListenAndServe(ctx); !errors.Is(err, ctx.Err()) {
		return err
	}

	shutdown(logger, metricsSrv)
	return nil
}

// startMetricsServer starts an HTTP server exposing Prometheus metrics at /metrics.
// It binds eagerly so that a port conflict is detected before the server starts.
func startMetricsServer(logger *slog.Logger, registry *prometheus.Registry, addr string) (*http.Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("metrics server: %w", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))

	//nolint:gosec // G112: metrics endpoint is internal-only; timeout omitted intentionally
	srv := &http.Server{Handler: mux}

	go func() {
		logger.Info("Starting metrics server", slog.String("addr", ln.Addr().String()))
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("Metrics server failed", slog.Any("err", err))
		}
	}()

	return srv, nil
}

func shutdown(logger *slog.Logger, metricsSrv *http.Server) {
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("Metrics server shutdown failed", slog.Any("err", err))
	}
	logger.Info("Shutting down retina-api")
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: l,
	}))
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envOrDefaultInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		i, err := strconv.Atoi(v)
		if err != nil {
			slog.Error("Invalid environment variable", slog.String("key", key), slog.String("value", v))
			os.Exit(1)
		}
		return i
	}
	return def
}

func envOrDefaultDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			slog.Error("Invalid environment variable", slog.String("key", key), slog.String("value", v))
			os.Exit(1)
		}
		return d
	}
	return def
}
