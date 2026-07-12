# llogr

Langfuse-compatible LLM log collector.

A lightweight FastAPI service that acts as a drop-in replacement for Langfuse's ingestion server. Point the Langfuse Python SDK or LangChain CallbackHandler at llogr to collect all LLM observability data (traces, generations, spans, scores).

## Quick start

```bash
uv sync
make dev
```

The server starts at `http://localhost:8000`.

## Usage

```bash
curl -X POST http://localhost:8000/api/public/ingestion \
  -u "pk-test:sk-test" \
  -H "Content-Type: application/json" \
  -d '{"batch":[{"id":"test-1","timestamp":"2026-03-04T00:00:00Z","type":"trace-create","body":{"id":"trace-1","name":"test-trace"}}]}'
```

Or configure the Langfuse SDK:

```python
from langfuse import Langfuse

langfuse = Langfuse(
    public_key="pk-test",
    secret_key="sk-test",
    host="http://localhost:8000",
)
```

## Configuration

### Clickstream fan-out

`clickstream` in `config.yaml` forwards every ingested batch to one or more Amplitude-compatible endpoints (`POST /2/httpapi`). Accepts a single endpoint or a list — each is sent to concurrently and independently, so one endpoint failing doesn't block the others.

```yaml
clickstream:
- name: "primary"
  api_url: "http://clickstream:9999/2/httpapi"
  api_key: "clickstream-key"
- name: "backup"
  api_url: "https://cb2.example.com/2/httpapi"
  api_key: "other-key"
  agents: ["research-assistant"]
```

The `clickstream` backend must also be listed under `features.store_backends` to be active.

Set `agents` on an endpoint to only forward batches from those agents (matched against the `X-Agent-Name` request header); omit it to receive every agent's events, as `"primary"` does above.

### Tempo sink

`tempo` in `config.yaml` forwards every ingested batch to a Tempo (or any OTLP/HTTP-compatible) traces endpoint. `trace-create`, `span-create`, `generation-create`, and `event-create` events are converted into OTLP spans (trace/span IDs derived from the Langfuse observation IDs via hashing, since those aren't valid OTLP IDs on their own); score events carry no duration and are dropped rather than forced into a span.

```yaml
tempo:
  endpoint: "http://tempo:4318/v1/traces"
  service_name: "llogr"
```

The `tempo` backend must also be listed under `features.store_backends` to be active. Forwarding failures are logged and counted in `llogr_tempo_forward_errors_total` but never block ingestion.

## Endpoints

| Method | Path                       | Description              |
|--------|----------------------------|--------------------------|
| GET    | `/livez`                   | Liveness probe — instant 200, no dependency checks |
| GET    | `/ready`                   | Readiness probe — checks S3 and ClickHouse; returns 200 or 503 |
| GET    | `/health`                  | Full health status — JSON with component details |
| GET    | `/metrics`                 | Prometheus metrics (ingestion counts, S3/clickstream/tempo endpoint latency and errors) |
| POST   | `/api/public/ingestion`    | Ingest events (207)      |

### Kubernetes probes

The Helm chart (`chart/`) is configured to use:
- `livenessProbe` → `/livez` (restarts pod if process is stuck)
- `readinessProbe` → `/ready` (removes pod from service if backends are down)

### Alerting

Prometheus alerting rules are in `devops/alerting/alert_rules.yml`:
- Ingestion error rate and volume spikes
- S3 save failures and latency
- Clickstream forwarding errors (per endpoint)
- P95/P99 request latency
- Health check and availability

## Development

```bash
make test    # run pytest
make lint    # run ruff
make dev     # start dev server with reload
```
