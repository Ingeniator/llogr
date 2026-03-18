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
| GET    | `/health`                  | Health check             |
| POST   | `/api/public/ingestion`    | Ingest events (207)      |

## Development

```bash
make test    # run pytest
make lint    # run ruff
make dev     # start dev server with reload
```
