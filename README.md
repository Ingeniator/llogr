# llogr

Langfuse-compatible LLM log collector.

A lightweight FastAPI service that acts as a drop-in replacement for Langfuse's ingestion server. Point the Langfuse Python SDK or LangChain CallbackHandler at llogr to collect all LLM observability data (traces, generations, spans, scores).

## Quick start

```bash
uv sync
make dev
```wede3fferc

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

## Endpoints

| Method | Path                       | Description              |
|--------|----------------------------|--------------------------|
| GET    | `/livez`                   | Liveness probe — instant 200, no dependency checks |
| GET    | `/ready`                   | Readiness probe — checks S3 and ClickHouse; returns 200 or 503 |
| GET    | `/health`                  | Full health status — JSON with component details |
| GET    | `/metrics`                 | Prometheus metrics (ingestion counts, S3/clickstream latency and errors) |
| POST   | `/api/public/ingestion`    | Ingest events (207)      |

### Kubernetes probes

The Helm chart (`chart/`) is configured to use:
- `livenessProbe` → `/livez` (restarts pod if process is stuck)
- `readinessProbe` → `/ready` (removes pod from service if backends are down)

### Alerting

Prometheus alerting rules are in `devops/alerting/alert_rules.yml`:
- Ingestion error rate and volume spikes
- S3 save failures and latency
- Clickstream forwarding errors
- P95/P99 request latency
- Health check and availability

## Development

```bash
make test    # run pytest
make lint    # run ruff
make dev     # start dev server with reload
```
