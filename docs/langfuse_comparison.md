# llogr vs Langfuse

## What they share

Both collect the same event types (traces, spans, generations, scores) using the same Langfuse SDK wire protocol. llogr was explicitly built to be a drop-in replacement for Langfuse's ingestion layer.

## Where they differ

| Dimension | llogr | Langfuse |
|---|---|---|
| **Scope** | Ingestion + storage + search only | Full observability platform |
| **Prompt management** | No | Yes (versioned prompts, playground) |
| **Datasets & evals** | No | Yes (datasets, LLM-as-judge, human annotation) |
| **UI** | Minimal log browser + billing dashboard | Rich product UI (traces, sessions, dashboards, playground) |
| **Storage** | S3 + ClickHouse (optional each) | PostgreSQL (required) + ClickHouse (optional) |
| **Multi-tenancy** | Built-in (X-Group-ID, org_admin role) | Workspace/project-based |
| **Cost tracking** | First-class (promoted columns, billing dashboard) | Available but secondary |
| **Forwarding** | Fan-out to Langfuse, Amplitude, custom APIs | No built-in forwarding |
| **Infrastructure weight** | Single FastAPI process + optional DBs | Web server + async worker + PostgreSQL + optional ClickHouse |
| **OTEL support** | Yes (v3 SDK) | Yes (v3 SDK) |
| **Self-host complexity** | Low | Medium–high |

## The core tradeoff

**llogr** is a lightweight data pipeline — it collects, stores, and routes LLM telemetry cheaply and with full data ownership. It is intentionally not a product; it has no prompt editor, no annotation UI, no dataset management.

**Langfuse** is an observability *product* — it adds the workflow layer on top of telemetry (iterating on prompts, curating datasets, running evals). That breadth comes with more infra dependencies and operational overhead.

## When to use which

- Use **llogr** when you want to own your trace data, forward to multiple sinks (including Langfuse itself), do cost accounting, or run lean without a full PostgreSQL deployment.
- Use **Langfuse** when you need the full evaluation workflow — prompt versioning, dataset curation, LLM judges, human annotation queues.

They can also coexist: llogr can forward batches to Langfuse while simultaneously writing to S3/ClickHouse for cost analytics.
