// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"
	"github.com/dioptra-io/retina-commons/framing"
	wire "github.com/dioptra-io/retina-commons/wire/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Intentionally uncovered:
//
//   - NewServer: NewRingBuffer's error is unreachable — NewServer itself
//     already guarantees capacity > 0 before calling it.
//   - close/ListenAndServe: the "Shutdown times out" branches require
//     s.server.Shutdown to genuinely exceed its deadline. handleStream
//     responds to context cancellation almost immediately (by design),
//     so reliably forcing a timeout would mean either a deliberately
//     uncooperative handler or racing an already-expired context against
//     a well-behaved one — a flaky test for a well-behaved code path.
//   - acceptIngest's non-net.ErrClosed Accept error branch requires a
//     listener failure unrelated to shutdown — not injectable without
//     refactoring (same reasoning as agentServer's equivalent in
//     retina-orchestrator).
//   - ListenAndServe's public-listener-failure return path requires the
//     same kind of unrelated-to-shutdown failure.

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testMetrics() *Metrics {
	return NewMetrics(prometheus.NewRegistry())
}

func waitForGauge(t *testing.T, g prometheus.Gauge, want float64) {
	t.Helper()
	const timeout = time.Second
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if testutil.ToFloat64(g) == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("gauge did not reach %v within %s (last value %v)", want, timeout, testutil.ToFloat64(g))
}

// validWireFIE returns a minimal but valid wire FIE — Agent,
// DestinationAddress, and ProductionTimestamp are required by
// model.ForwardingInfoElementFromProto.
func validWireFIE(id uint64) *wire.ForwardingInfoElement {
	return &wire.ForwardingInfoElement{
		Agent:               &wire.Agent{AgentId: "agent-1"},
		ProbingDirectiveId:  id,
		IpVersion:           wire.IPVersion_IP_VERSION_IPV4,
		DestinationAddress:  "192.0.2.1",
		ProductionTimestamp: timestamppb.New(time.Now()),
	}
}

// newIngestTestServer builds a Server with both listeners bound to dynamic
// ports and only the ingest accept loop running — for tests focused on
// ingest/ring-buffer behavior that don't need the public HTTP server active.
func newIngestTestServer(t *testing.T) *Server {
	t.Helper()
	s, err := NewServer(&ServerConfig{
		PublicAddress: "127.0.0.1:0",
		IngestAddress: "127.0.0.1:0",
		RingCapacity:  100,
		Logger:        discardLogger(),
		Metrics:       testMetrics(),
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	go s.acceptIngest()
	t.Cleanup(func() {
		_ = s.ingestListener.Close()
		_ = s.publicListener.Close()
	})
	return s
}

// dialIngest connects to s's ingest listener and sends fie.
func dialIngest(t *testing.T, s *Server, fie *wire.ForwardingInfoElement) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", s.ingestListener.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial ingest: %v", err)
	}
	if err := framing.Send(conn, time.Second, fie); err != nil {
		t.Fatalf("failed to send FIE: %v", err)
	}
	return conn
}

// ---- NewServer ----

func TestNewServer_NilMetrics(t *testing.T) {
	t.Parallel()
	_, err := NewServer(&ServerConfig{
		PublicAddress: "127.0.0.1:0",
		IngestAddress: "127.0.0.1:0",
	})
	if err == nil {
		t.Fatal("expected error for nil metrics")
	}
}

func TestNewServer_BindsBothListeners(t *testing.T) {
	t.Parallel()
	s, err := NewServer(&ServerConfig{
		PublicAddress: "127.0.0.1:0",
		IngestAddress: "127.0.0.1:0",
		Metrics:       testMetrics(),
		Logger:        discardLogger(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() {
		_ = s.publicListener.Close()
		_ = s.ingestListener.Close()
	}()

	if s.publicListener == nil || strings.HasSuffix(s.publicListener.Addr().String(), ":0") {
		t.Error("expected publicListener bound to a resolved (non-zero) port")
	}
	if s.ingestListener == nil || strings.HasSuffix(s.ingestListener.Addr().String(), ":0") {
		t.Error("expected ingestListener bound to a resolved (non-zero) port")
	}
}

func TestNewServer_InvalidPublicAddress(t *testing.T) {
	t.Parallel()
	_, err := NewServer(&ServerConfig{
		PublicAddress: "not-a-valid-address",
		IngestAddress: "127.0.0.1:0",
		Metrics:       testMetrics(),
	})
	if err == nil {
		t.Fatal("expected error for invalid PublicAddress")
	}
}

func TestNewServer_InvalidIngestAddress(t *testing.T) {
	t.Parallel()
	_, err := NewServer(&ServerConfig{
		PublicAddress: "127.0.0.1:0",
		IngestAddress: "not-a-valid-address",
		Metrics:       testMetrics(),
	})
	if err == nil {
		t.Fatal("expected error for invalid IngestAddress")
	}
}

func TestNewServer_ZeroRingCapacityDoesNotError(t *testing.T) {
	t.Parallel()
	s, err := NewServer(&ServerConfig{
		PublicAddress: "127.0.0.1:0",
		IngestAddress: "127.0.0.1:0",
		RingCapacity:  0,
		Metrics:       testMetrics(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = s.publicListener.Close()
	_ = s.ingestListener.Close()
}

func TestNewServer_DefaultLogger(t *testing.T) {
	t.Parallel()
	s, err := NewServer(&ServerConfig{
		PublicAddress: "127.0.0.1:0",
		IngestAddress: "127.0.0.1:0",
		Metrics:       testMetrics(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() {
		_ = s.publicListener.Close()
		_ = s.ingestListener.Close()
	}()
	if s.logger == nil {
		t.Error("expected default logger to be set")
	}
}

// ---- ingest ----

func TestIngest_ValidFIEReachesRingBuffer(t *testing.T) {
	t.Parallel()
	s := newIngestTestServer(t)

	sub := s.ring.NewSubscriber()
	defer sub.Close()

	fie := validWireFIE(42)
	fie.NearInfo = &wire.Info{
		ProbeTtl:          1,
		ReplyAddress:      "192.0.2.2",
		SentTimestamp:     timestamppb.New(time.Now()),
		ReceivedTimestamp: timestamppb.New(time.Now()),
	}
	fie.FarInfo = &wire.Info{
		ProbeTtl:          2,
		ReplyAddress:      "192.0.2.3",
		SentTimestamp:     timestamppb.New(time.Now()),
		ReceivedTimestamp: timestamppb.New(time.Now()),
	}
	conn := dialIngest(t, s, fie)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, _, err := sub.Pop(ctx)
	if err != nil {
		t.Fatalf("did not receive FIE via ring buffer: %v", err)
	}
	if got.ProbingDirectiveID != 42 {
		t.Errorf("expected ProbingDirectiveID 42, got %d", got.ProbingDirectiveID)
	}
	if got.Agent.AgentID != "agent-1" {
		t.Errorf("expected AgentID %q, got %q", "agent-1", got.Agent.AgentID)
	}
	if got.NearInfo == nil || got.NearInfo.ProbeTTL != 1 {
		t.Errorf("expected NearInfo to round-trip with ProbeTTL 1, got %+v", got.NearInfo)
	}
	if got.FarInfo == nil || got.FarInfo.ProbeTTL != 2 {
		t.Errorf("expected FarInfo to round-trip with ProbeTTL 2, got %+v", got.FarInfo)
	}
}

// TestIngest_MalformedFIEIsDroppedNotCrashed covers
// model.ForwardingInfoElementFromProto's validation failure path (a
// required field missing) — the connection should stay open and later,
// valid FIEs should still arrive.
func TestIngest_MalformedFIEIsDroppedNotCrashed(t *testing.T) {
	t.Parallel()
	s := newIngestTestServer(t)

	sub := s.ring.NewSubscriber()
	defer sub.Close()

	bad := validWireFIE(1)
	bad.DestinationAddress = "" // required
	conn := dialIngest(t, s, bad)
	defer conn.Close()

	if err := framing.Send(conn, time.Second, validWireFIE(2)); err != nil {
		t.Fatalf("failed to send the valid FIE: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, _, err := sub.Pop(ctx)
	if err != nil {
		t.Fatalf("did not receive the valid FIE after a malformed one: %v", err)
	}
	if got.ProbingDirectiveID != 2 {
		t.Errorf("expected only the valid FIE (ID 2), got %d", got.ProbingDirectiveID)
	}
}

// TestIngest_ConversionErrorIsDroppedNotCrashed covers modelFIEToAPI's
// range checks: out-of-range enum values pass FromProto's validation
// (proto3 enums accept any int32) and only fail at the narrowing
// conversion to uint8.
func TestIngest_ConversionErrorIsDroppedNotCrashed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*wire.ForwardingInfoElement)
	}{
		{"ip version", func(f *wire.ForwardingInfoElement) { f.IpVersion = wire.IPVersion(256) }},
		{"protocol", func(f *wire.ForwardingInfoElement) { f.Protocol = wire.Protocol(256) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := newIngestTestServer(t)

			sub := s.ring.NewSubscriber()
			defer sub.Close()

			bad := validWireFIE(1)
			tt.mutate(bad)
			conn := dialIngest(t, s, bad)
			defer conn.Close()

			if err := framing.Send(conn, time.Second, validWireFIE(2)); err != nil {
				t.Fatalf("failed to send the valid FIE: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			got, _, err := sub.Pop(ctx)
			if err != nil {
				t.Fatalf("did not receive the valid FIE after a conversion-error FIE: %v", err)
			}
			if got.ProbingDirectiveID != 2 {
				t.Errorf("expected only the valid FIE (ID 2), got %d", got.ProbingDirectiveID)
			}
		})
	}
}

func TestIngest_ConnectionCloseHandledGracefully(t *testing.T) {
	t.Parallel()
	s := newIngestTestServer(t)

	conn := dialIngest(t, s, validWireFIE(1))
	waitForGauge(t, s.metrics.IngestConnectionUp, 1)

	conn.Close()
	waitForGauge(t, s.metrics.IngestConnectionUp, 0)
}

// TestIngest_OverlappingConnectionsKeepGaugeUp covers the ingestConns
// map/mutex specifically: IngestConnectionUp should only drop once the
// last tracked connection closes, not the first — this is what makes a
// reconnect (old connection tearing down while a new one opens) not
// produce a false "down" blip.
func TestIngest_OverlappingConnectionsKeepGaugeUp(t *testing.T) {
	t.Parallel()
	s := newIngestTestServer(t)

	conn1 := dialIngest(t, s, validWireFIE(1))
	waitForGauge(t, s.metrics.IngestConnectionUp, 1)

	conn2 := dialIngest(t, s, validWireFIE(2))
	defer conn2.Close()

	conn1.Close()
	time.Sleep(50 * time.Millisecond)
	if got := testutil.ToFloat64(s.metrics.IngestConnectionUp); got != 1 {
		t.Errorf("expected IngestConnectionUp to stay 1 while a connection remains open, got %v", got)
	}

	conn2.Close()
	waitForGauge(t, s.metrics.IngestConnectionUp, 0)
}

// ---- stream ----

func TestStream_ReceivesPushedFIE(t *testing.T) {
	t.Parallel()
	ring, err := NewRingBuffer[SequencedFIE](100)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{ring: ring, metrics: testMetrics(), logger: discardLogger()}

	ts := httptest.NewServer(http.HandlerFunc(s.handleStream))
	defer ts.Close()

	type result struct {
		fie SequencedFIE
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		resp, err := http.Get(ts.URL) //nolint:noctx
		if err != nil {
			resultCh <- result{err: err}
			return
		}
		defer resp.Body.Close()
		var fie SequencedFIE
		err = json.NewDecoder(resp.Body).Decode(&fie)
		resultCh <- result{fie: fie, err: err}
	}()

	time.Sleep(50 * time.Millisecond) // let the subscriber register before pushing
	ring.Push(&SequencedFIE{ForwardingInfoElement: api.ForwardingInfoElement{ProbingDirectiveID: 42}})

	select {
	case res := <-resultCh:
		if res.err != nil {
			t.Fatalf("unexpected error: %v", res.err)
		}
		if res.fie.ProbingDirectiveID != 42 {
			t.Errorf("expected ProbingDirectiveID 42, got %d", res.fie.ProbingDirectiveID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive the streamed FIE in time")
	}
}

// noFlushWriter wraps http.ResponseWriter without exposing Flush, even if
// the underlying value has it — embedding an interface only promotes that
// interface's own methods, not the concrete value's extra ones.
type noFlushWriter struct {
	http.ResponseWriter
}

// failWriter implements http.ResponseWriter and http.Flusher, but every
// Write fails — used to deterministically trigger handleStream's
// enc.Encode error branch without any timing dependency (the very first
// write always fails).
type failWriter struct {
	header http.Header
}

func (f *failWriter) Header() http.Header {
	if f.header == nil {
		f.header = make(http.Header)
	}
	return f.header
}
func (f *failWriter) Write([]byte) (int, error) { return 0, fmt.Errorf("simulated write failure") }
func (f *failWriter) WriteHeader(int)           {}
func (f *failWriter) Flush()                    {}

func TestStream_FlusherUnsupported(t *testing.T) {
	t.Parallel()
	ring, err := NewRingBuffer[SequencedFIE](10)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{ring: ring, metrics: testMetrics(), logger: discardLogger()}

	rec := httptest.NewRecorder()
	w := &noFlushWriter{ResponseWriter: rec}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/stream", nil)

	s.handleStream(w, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected %d, got %d", http.StatusInternalServerError, rec.Code)
	}
}

// TestStream_EncodeErrorEndsHandlerCleanly covers enc.Encode's error
// branch deterministically: every Write on failWriter fails immediately,
// so the very first pushed FIE triggers it — no timing dependency.
func TestStream_EncodeErrorEndsHandlerCleanly(t *testing.T) {
	t.Parallel()
	ring, err := NewRingBuffer[SequencedFIE](10)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{ring: ring, metrics: testMetrics(), logger: discardLogger()}

	w := &failWriter{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/stream", nil).WithContext(ctx)

	done := make(chan struct{})
	go func() {
		s.handleStream(w, req)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond) // let the subscriber register
	ring.Push(&SequencedFIE{})

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handleStream did not return after a write failure")
	}
}

func TestStream_ReturnsWhenContextCanceled(t *testing.T) {
	t.Parallel()
	ring, err := NewRingBuffer[SequencedFIE](10)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{ring: ring, metrics: testMetrics(), logger: discardLogger()}

	rec := httptest.NewRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/api/v1/stream", nil).WithContext(ctx)

	done := make(chan struct{})
	go func() {
		s.handleStream(rec, req)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond) // let it start blocking in Pop
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handleStream did not return after context cancellation")
	}
}

// TestStream_SkippedMetricIncrements covers SubscriberSkippedTotal
// specifically: pushing more than the ring's capacity while a subscriber
// is registered but not yet draining forces it to be lapped.
func TestStream_SkippedMetricIncrements(t *testing.T) {
	t.Parallel()
	metrics := testMetrics()
	ring, err := NewRingBuffer[SequencedFIE](2)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{ring: ring, metrics: metrics, logger: discardLogger()}

	rec := httptest.NewRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/stream", nil).WithContext(ctx)

	done := make(chan struct{})
	go func() {
		s.handleStream(rec, req)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond) // let the subscriber register
	for i := 0; i < 5; i++ {          // more than capacity (2): forces skips
		ring.Push(&SequencedFIE{})
	}
	time.Sleep(50 * time.Millisecond) // let handleStream's loop observe the skip

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handleStream did not return after context cancellation")
	}

	if got := testutil.ToFloat64(metrics.SubscriberSkippedTotal); got == 0 {
		t.Error("expected SubscriberSkippedTotal to increment after the subscriber was lapped")
	}
}

// ---- ListenAndServe / close ----

func TestListenAndServe_ReturnsOnContextCancel(t *testing.T) {
	t.Parallel()
	s, err := NewServer(&ServerConfig{
		PublicAddress: "127.0.0.1:0",
		IngestAddress: "127.0.0.1:0",
		Metrics:       testMetrics(),
		Logger:        discardLogger(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.ListenAndServe(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("expected nil error on clean shutdown, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ListenAndServe did not return after context cancellation")
	}
}

func TestClose_ClosesActiveIngestConnections(t *testing.T) {
	t.Parallel()
	s := newIngestTestServer(t)

	conn := dialIngest(t, s, validWireFIE(1))
	defer conn.Close()
	waitForGauge(t, s.metrics.IngestConnectionUp, 1)

	if err := s.close(time.Second); err != nil {
		t.Fatalf("unexpected error from close: %v", err)
	}
	waitForGauge(t, s.metrics.IngestConnectionUp, 0)
}
