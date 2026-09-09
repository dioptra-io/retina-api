// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT
package api

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metrics for retina-api.
// It is created once and passed to NewServer via ServerConfig.
type Metrics struct {
	FIEsIngestedTotal      prometheus.Counter
	SubscribersActive      prometheus.Gauge
	SubscriberSkippedTotal prometheus.Counter
	IngestConnectionUp     prometheus.Gauge
}

// NewMetrics creates and registers all retina-api metrics with the given registry.
func NewMetrics(registry prometheus.Registerer) *Metrics {
	factory := promauto.With(registry)

	return &Metrics{
		FIEsIngestedTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "retina_api_fies_ingested_total",
			Help: "Total FIEs received on the ingest endpoint.",
		}),
		SubscribersActive: factory.NewGauge(prometheus.GaugeOpts{
			Name: "retina_api_subscribers_active",
			Help: "Currently connected subscribers.",
		}),
		SubscriberSkippedTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "retina_api_subscriber_skipped_total",
			Help: "Cumulative FIEs a subscriber missed because it fell behind the ring buffer.",
		}),
		IngestConnectionUp: factory.NewGauge(prometheus.GaugeOpts{
			Name: "retina_api_ingest_connection_up",
			Help: "1 if the ingest request is currently open, 0 otherwise.",
		}),
	}
}
