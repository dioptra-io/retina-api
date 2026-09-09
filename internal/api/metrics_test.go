// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT
package api

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNewMetrics_NilRegistry(t *testing.T) {
	t.Parallel()
	m := NewMetrics(nil)
	if m == nil {
		t.Fatal("expected non-nil Metrics")
	}
}

func TestNewMetrics_WithRegistry(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	if m == nil {
		t.Fatal("expected non-nil Metrics")
		return
	}
	// NOTE: If a new metric is added to Metrics, add a corresponding nil check here.
	if m.FIEsIngestedTotal == nil {
		t.Error("expected FIEsIngestedTotal to be non-nil")
	}
	if m.SubscribersActive == nil {
		t.Error("expected SubscribersActive to be non-nil")
	}
	if m.SubscriberSkippedTotal == nil {
		t.Error("expected SubscriberSkippedTotal to be non-nil")
	}
	if m.IngestConnectionUp == nil {
		t.Error("expected IngestConnectionUp to be non-nil")
	}
}

func TestNewMetrics_DefaultRegistry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: registers global metrics")
	}
	m := NewMetrics(prometheus.DefaultRegisterer)
	if m == nil {
		t.Fatal("expected non-nil Metrics")
	}
}
