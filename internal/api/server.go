// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	httpSwagger "github.com/swaggo/http-swagger"

	"github.com/dioptra-io/retina-commons/framing"
	"github.com/dioptra-io/retina-commons/model"
	wire "github.com/dioptra-io/retina-commons/wire/v2"

	"github.com/dioptra-io/retina-commons/api/v1"
)

// SequencedFIE is a ForwardingInfoElement with a sequence number for ordered delivery to HTTP clients.
type SequencedFIE struct {
	api.ForwardingInfoElement
	// SequenceNumber is per-subscriber and per-process: it resets on
	// restart and isn't comparable across two different subscribers.
	SequenceNumber uint64 `json:"sequence_number"`
}

// ServerConfig configures NewServer.
type ServerConfig struct {
	PublicAddress string
	IngestAddress string
	// ReadHeaderTimeout applies to the public HTTP listener only — the
	// ingest listener is a raw TCP protocol, not HTTP.
	ReadHeaderTimeout time.Duration
	// RingCapacity is how far a slow subscriber can lag before being
	// skipped ahead automatically (new subscribers get no history — they
	// only see FIEs pushed after they connect). Each entry is one
	// SequencedFIE (a couple hundred bytes); at N FIEs/sec, capacity/N is
	// roughly how many seconds of lag a subscriber can tolerate before
	// skipping. Configurable via main.go's -ring-capacity flag.
	RingCapacity int
	Logger       *slog.Logger
	Metrics      *Metrics
}

// Server fans FIEs from orchestrator's ingest connection out to subscribers.
type Server struct {
	config         *ServerConfig
	logger         *slog.Logger
	metrics        *Metrics
	server         *http.Server
	publicListener net.Listener
	ingestListener net.Listener
	ring           *RingBuffer[SequencedFIE]

	ingestMu    sync.Mutex
	ingestConns map[net.Conn]struct{}
}

// NewServer requires Config.Metrics; Config.Logger defaults if nil.
func NewServer(config *ServerConfig) (*Server, error) {
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.Metrics == nil {
		return nil, fmt.Errorf("metrics cannot be nil")
	}

	capacity := config.RingCapacity
	if capacity <= 0 {
		capacity = 500
	}
	ring, err := NewRingBuffer[SequencedFIE](capacity)
	if err != nil {
		return nil, fmt.Errorf("failed to create ring buffer: %w", err)
	}

	s := &Server{
		config:      config,
		logger:      config.Logger,
		metrics:     config.Metrics,
		ring:        ring,
		ingestConns: make(map[net.Conn]struct{}),
	}

	ingestListener, err := net.Listen("tcp", config.IngestAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to bind ingest listener: %w", err)
	}
	s.ingestListener = ingestListener
	s.logger.Info("Ingest listener bound", slog.String("addr", ingestListener.Addr().String()))

	publicListener, err := net.Listen("tcp", config.PublicAddress)
	if err != nil {
		_ = ingestListener.Close()
		return nil, fmt.Errorf("failed to bind public listener: %w", err)
	}
	s.publicListener = publicListener
	s.logger.Info("Public listener bound", slog.String("addr", publicListener.Addr().String()))

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/stream", s.handleStream)
	mux.HandleFunc("/api/v1/swagger/", httpSwagger.WrapHandler)
	s.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: config.ReadHeaderTimeout,
	}

	return s, nil
}

// ListenAndServe blocks until ctx is canceled or a listener fails.
func (s *Server) ListenAndServe(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		if err := s.close(5 * time.Second); err != nil {
			s.logger.Warn("Shutdown did not complete cleanly", slog.String("error", err.Error()))
		}
	}()

	go s.acceptIngest()

	if err := s.server.Serve(s.publicListener); !errors.Is(err, http.ErrServerClosed) {
		s.logger.Error("Public listener failed", slog.String("error", err.Error()))
		return err
	}
	return nil
}

// close is called internally once ctx is canceled.
func (s *Server) close(timeout time.Duration) error {
	s.logger.Info("Shutting down retina-api")
	_ = s.ingestListener.Close()

	s.ingestMu.Lock()
	for conn := range s.ingestConns {
		_ = conn.Close()
	}
	s.ingestMu.Unlock()

	exitCtx, exitCancel := context.WithTimeout(context.Background(), timeout)
	defer exitCancel()
	if err := s.server.Shutdown(exitCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.logger.Warn("Server shutdown timed out", slog.Duration("timeout", timeout))
		return err
	}
	return nil
}

// acceptIngest accepts ingest connections until the listener closes
// (during shutdown). Each connection is handled independently, so a
// reconnect from orchestrator can briefly overlap with the old connection
// tearing down without disrupting anything.
func (s *Server) acceptIngest() {
	for {
		conn, err := s.ingestListener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			s.logger.Error("Ingest accept failed", slog.String("error", err.Error()))
			return
		}
		go s.handleIngestConn(conn)
	}
}

// handleIngestConn reads length-prefixed protobuf FIEs (retina-commons/framing)
// from one ingest connection until it closes or a malformed message
// arrives, converting and pushing each into the ring buffer. No read
// deadline is set — like agentServer's own FIE stream, this connection is
// expected to be long-lived and idle between probes.
func (s *Server) handleIngestConn(conn net.Conn) {
	s.ingestMu.Lock()
	s.ingestConns[conn] = struct{}{}
	s.ingestMu.Unlock()
	s.metrics.IngestConnectionUp.Set(1)

	defer func() {
		_ = conn.Close()
		s.ingestMu.Lock()
		delete(s.ingestConns, conn)
		remaining := len(s.ingestConns)
		s.ingestMu.Unlock()
		if remaining == 0 {
			s.metrics.IngestConnectionUp.Set(0)
		}
	}()

	for {
		var wireFIE wire.ForwardingInfoElement
		if err := framing.Receive(conn, 0, &wireFIE); err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				s.logger.Warn("Ingest receive failed", slog.String("error", err.Error()))
			}
			return
		}

		modelFIE, err := model.ForwardingInfoElementFromProto(&wireFIE)
		if err != nil {
			s.logger.Warn("Dropping malformed FIE on ingest", slog.String("error", err.Error()))
			continue
		}

		apiFIE, err := modelFIEToAPI(&modelFIE)
		if err != nil {
			s.logger.Warn("Dropping FIE: failed to convert for external API", slog.String("error", err.Error()))
			continue
		}

		s.ring.Push(&SequencedFIE{ForwardingInfoElement: apiFIE})
		s.metrics.FIEsIngestedTotal.Inc()
	}
}

// modelFIEToAPI converts an already-validated model.ForwardingInfoElement
// (see model.ForwardingInfoElementFromProto) to the api/v1 type exposed to
// external subscribers. Fallible: wire.IPVersion/wire.Protocol are
// int32-based but api.IPVersion/api.Protocol are uint8-based — a blind cast
// would be a genuine narrowing conversion, even though every legitimate
// value fits a uint8 in practice.
func modelFIEToAPI(fie *model.ForwardingInfoElement) (api.ForwardingInfoElement, error) {
	if fie.IPVersion < 0 || fie.IPVersion > 255 {
		return api.ForwardingInfoElement{}, fmt.Errorf("ip_version %d exceeds uint8 range", fie.IPVersion)
	}
	if fie.Protocol < 0 || fie.Protocol > 255 {
		return api.ForwardingInfoElement{}, fmt.Errorf("protocol %d exceeds uint8 range", fie.Protocol)
	}

	out := api.ForwardingInfoElement{
		Agent:               api.Agent{AgentID: fie.Agent.ID},
		ProbingDirectiveID:  fie.ProbingDirectiveID,
		IPVersion:           api.IPVersion(fie.IPVersion), //nolint:gosec // range-checked above
		Protocol:            api.Protocol(fie.Protocol),   //nolint:gosec // range-checked above
		SourceAddress:       fie.SourceAddress,
		DestinationAddress:  fie.DestinationAddress,
		ProductionTimestamp: fie.ProductionTimestamp,
	}
	if fie.NearInfo != nil {
		out.NearInfo = &api.Info{
			ProbeTTL:          fie.NearInfo.ProbeTTL,
			ReplyAddress:      fie.NearInfo.ReplyAddress,
			SentTimestamp:     fie.NearInfo.SentTimestamp,
			ReceivedTimestamp: fie.NearInfo.ReceivedTimestamp,
		}
	}
	if fie.FarInfo != nil {
		out.FarInfo = &api.Info{
			ProbeTTL:          fie.FarInfo.ProbeTTL,
			ReplyAddress:      fie.FarInfo.ReplyAddress,
			SentTimestamp:     fie.FarInfo.SentTimestamp,
			ReceivedTimestamp: fie.FarInfo.ReceivedTimestamp,
		}
	}
	return out, nil
}

// @Summary		Stream forwarding info elements
// @Description	Opens a long-lived NDJSON stream of FIEs. Slow subscribers are lapped
// @Description	automatically.
// @Tags			stream
// @Produce		application/x-ndjson
// @Success		200	{object}	SequencedFIE
// @Failure		500	{string}	string	"internal server error"
// @Router			/stream [get]
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher, ok := w.(http.Flusher)
	if !ok {
		s.logger.Error("Streaming unsupported: ResponseWriter does not implement http.Flusher")
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	subscriber := s.ring.NewSubscriber()
	defer subscriber.Close()

	s.metrics.SubscribersActive.Inc()
	defer s.metrics.SubscribersActive.Dec()

	enc := json.NewEncoder(w)
	s.logger.Debug("Client connected", slog.String("remote_addr", r.RemoteAddr))
	defer s.logger.Debug("Client disconnected", slog.String("remote_addr", r.RemoteAddr))

	var lastSkipped uint64
	for {
		fie, seq, err := subscriber.Pop(r.Context())
		if err != nil {
			return
		}
		if skipped := subscriber.Skipped(); skipped > lastSkipped {
			s.metrics.SubscriberSkippedTotal.Add(float64(skipped - lastSkipped))
			lastSkipped = skipped
		}
		fie.SequenceNumber = seq
		if err := enc.Encode(fie); err != nil {
			return
		}
		flusher.Flush()
	}
}
