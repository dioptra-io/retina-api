# retina-api

`retina-api` receives Forwarding Info Elements (FIEs) from the [Retina orchestrator](https://github.com/dioptra-io/retina-orchestrator) over a single connection and streams them live to any number of external subscribers.

**Retina Architecture:**
- **PD source**: Produces probing directives (PDs) as JSONL files: the
  [Generator](https://github.com/dioptra-io/retina-generator) or the
  [retina-tools](https://github.com/dioptra-io/retina-tools) pipeline for
  Iris-derived directives
- **[Orchestrator](https://github.com/dioptra-io/retina-orchestrator)**: Loads PD files,
  distributes directives to agents, collects forwarding info elements (FIEs), forwards them to the API
- **[Agents](https://github.com/dioptra-io/retina-agent)**: Execute network probes and
  return measurements
- **[API](https://github.com/dioptra-io/retina-api)** (this component): Receives FIEs from the orchestrator and streams them to clients

The orchestrator pushes FIEs to `retina-api` as Protobuf messages over a length-prefixed TCP stream (see the `framing` package in
[retina-commons](https://github.com/dioptra-io/retina-commons)). Subscribers receive them as newline-delimited JSON (NDJSON) over HTTP.

```
┌───────────┐
│ PD source │
└─────┬─────┘
      │ PD files (JSONL), diffs on SIGHUP
      ▼
┌─────────────┐         ProbingDirective         ┌───────┐
│Orchestrator │────────────────────────────────▶ │ Agent │
└──┬───▲──────┘                                  └───┬───┘
   │   │            ForwardingInfoElement            │
   │   └─────────────────────────────────────────────┘
   │ FIEs
   ▼
┌─────┐
│ API │
└─────┘
```

## Build

```bash
make build
```

`make build` generates the Swagger docs with [swag](https://github.com/swaggo/swag), lints, and builds the binary. The generated `docs/` files are not committed; `make docs`, `make lint` and `make test` all generate them first, so `swag` must be installed (`go install github.com/swaggo/swag/cmd/swag@v1.16.6`).

To clean:
```bash
make clean
```

## Test

```bash
make test
```

## Usage

```bash
./retina-api [flags]
```

### Example

```bash
./retina-api \
  --public-addr=0.0.0.0:8080 \
  --ingest-addr=0.0.0.0:8123 \
  --metrics-addr=127.0.0.1:9312 \
  --ring-capacity=100 \
  --log-level=info
```

Then, to watch the live stream:

```bash
curl -N http://localhost:8080/api/v1/stream
```

## Flags

| Flag                    | Default          | Description                                                                         |
| ----------------------- | ---------------- | ----------------------------------------------------------------------------------- |
| `--public-addr`         | `127.0.0.1:8080` | Public HTTP listener address (stream and Swagger UI)                                |
| `--ingest-addr`         | `127.0.0.1:8123` | Raw TCP listener address for the orchestrator's FIE connection                      |
| `--metrics-addr`        | `127.0.0.1:9312` | Address to expose Prometheus metrics on (internal only)                             |
| `--ring-capacity`       | `100`            | Ring buffer capacity: how many FIEs a slow subscriber can lag behind before skipping |
| `--read-header-timeout` | `5s`             | Timeout for reading HTTP request headers on the public listener                     |
| `--log-level`           | `info`           | Log level (`debug`, `info`, `warn`, `error`)                                        |

All addresses default to loopback, so the service isn't reachable from outside the host until you bind it explicitly.

## Environment Variables

All flags can be configured via environment variables. These act as defaults and are overridden by CLI flags.

Precedence:

```
CLI flags > environment variables > hardcoded defaults
```

| Variable                          | Default          | Description                                                  |
| --------------------------------- | ---------------- | ------------------------------------------------------------ |
| `RETINA_API_PUBLIC_ADDR`          | `127.0.0.1:8080` | Public HTTP listener address                                 |
| `RETINA_API_INGEST_ADDR`          | `127.0.0.1:8123` | Raw TCP listener address for the orchestrator                |
| `RETINA_API_METRICS_ADDR`         | `127.0.0.1:9312` | Address to expose Prometheus metrics on                      |
| `RETINA_API_RING_CAPACITY`        | `100`            | Ring buffer capacity                                         |
| `RETINA_API_READ_HEADER_TIMEOUT`  | `5s`             | Timeout for reading HTTP request headers (public listener)   |
| `RETINA_API_LOG_LEVEL`            | `info`           | Log level (`debug`, `info`, `warn`, `error`)                 |

## Endpoints

| Listener | Endpoint              | Description                                                         |
| -------- | --------------------- | ------------------------------------------------------------------- |
| Public   | `GET /api/v1/stream`  | Live FIE stream, `application/x-ndjson`, one JSON object per line   |
| Public   | `/api/v1/swagger/`    | Swagger UI with the full response schema                            |
| Ingest   | raw TCP               | Length-prefixed Protobuf `ForwardingInfoElement` messages (not HTTP) |
| Metrics  | `GET /metrics`        | Prometheus metrics                                                  |

## Behavior

- The orchestrator connects to the ingest address and sends FIEs as length-prefixed Protobuf messages (see the `framing` package in retina-commons).- Each FIE is validated on arrival. A malformed FIE, or one with an out-of-range enum value, is logged and dropped; the connection stays open and later FIEs are still processed.
- Accepted FIEs go into an in-memory ring buffer. Every subscriber reads from it independently, so a slow subscriber never blocks ingest or other subscribers.
- A subscriber only receives FIEs that arrive after it connects; there is no history replay.
- A subscriber that falls more than `--ring-capacity` FIEs behind skips ahead instead of stalling, and the skip is counted in `retina_api_subscriber_skipped_total`.
- Each streamed object carries a `sequence_number`. It is per subscriber and per process: it counts from the point a subscriber connects, includes skipped FIEs (so a gap means FIEs were missed), resets when `retina-api` restarts, and is not comparable between subscribers.
- Logs are written to stdout in JSON format, compatible with Loki/Grafana pipelines.
- `SIGINT` and `SIGTERM` trigger a graceful shutdown.

## Security

The ingest listener has **no authentication**: anything that can reach `--ingest-addr` can inject FIEs into the public stream. In deployment, restrict that port at the host firewall to the orchestrator's IP addresses only. Only the public listener (typically behind a reverse proxy on 80/443) should be reachable from outside. The metrics listener is meant for local scraping and defaults to loopback.

## Deployment

The container image is `ghcr.io/dioptra-io/retina-api`.
## Observability

Metrics are exposed at `--metrics-addr` (default `127.0.0.1:9312`) in Prometheus format:

- `retina_api_fies_ingested_total`: FIEs received from the orchestrator
- `retina_api_subscribers_active`: currently connected subscribers
- `retina_api_subscriber_skipped_total`: FIEs subscribers missed because they fell behind the ring buffer
- `retina_api_ingest_connection_up`: whether an orchestrator connection is currently open

See `internal/api/metrics.go` for the full list.

## License

MIT License - see [LICENSE](LICENSE) for details