# What Good Traces Should Contain

This document describes the fields that make LLM traces genuinely useful for analysis,
compares the Langfuse SDK schema and OpenTelemetry (OTEL) attribute keys, and maps each
field to what llogr currently captures.

---

## Why traces fail at analysis time

Most trace quality problems fall into one of three buckets:

1. **Missing identifiers** — you cannot group by user, session, or prompt template.
2. **Missing timing** — you can count calls but not diagnose latency regressions.
3. **Missing token/cost data** — billing, quota, and efficiency analysis are impossible.

The sections below go field-by-field on what to send, and note what llogr promotes to
indexed columns versus what stays buried in the `body` JSON blob.

---

## Field reference

### 1. Identifiers

| Field | Langfuse SDK key | OTEL attribute | llogr column | Notes |
|-------|-----------------|----------------|--------------|-------|
| Trace ID | `body.id` (trace-create) / `body.traceId` | `trace_id` (OTLP wire) | `trace_id` | Groups all spans in one request |
| Span/observation ID | `body.id` | `span_id` (OTLP wire) | `event_id` | Unique per span/generation |
| Parent span ID | `body.parentObservationId` | `parent_span_id` (OTLP wire) | `parent_span_id` | Enables waterfall tree reconstruction |
| Session ID | `body.sessionId` | `session.id` | `session_id` | Groups traces across a conversation |
| User ID | `body.userId` | `user.id` | ❌ body only | Critical for per-user cost / error rate |
| Span / trace name | `body.name` | `span.name` (OTLP wire) | `name` | ⚠️ overridden by `x-agent-name` header — see note below |

**Gaps:** `userId` is stored in `body` JSON but not promoted to a column — queries that
filter or aggregate by user require JSON extraction, which is slow on large tables.

**`name` override behavior:** If the `x-agent-name` HTTP header is set on the ingestion
request, llogr unconditionally stamps that value onto `body.name` for **every event in
the batch**, overriding any name the SDK set per-span. This means per-span names (e.g.
`"retrieval"`, `"llm-call"`) are lost when the agent name header is present. The `name`
column is then only reliable as an agent-level label, not a span-type identifier.

---

### 2. Timing

| Field | Langfuse SDK key | OTEL attribute | llogr column | Notes |
|-------|-----------------|----------------|--------------|-------|
| Span start | `body.startTime` | `start_time_unix_nano` | `start_time` | ISO 8601 / nanoseconds |
| Span end | `body.endTime` | `end_time_unix_nano` | `end_time` | |
| Duration | computed | computed | `duration_ms` | Promoted; computed if not in `metadata.duration_ms` |
| Time-to-first-token | `body.completionStartTime` | — | ❌ body only | TTFT = `completionStartTime - startTime`; streaming quality signal |

**Gaps:** `completionStartTime` is now set to the real first-chunk timestamp for
streaming requests (`first_chunk_time` captured in the stream generator). Non-streaming
requests still pass `None` — no TTFT for those. The field is not promoted to a column,
so `P95(TTFT)` queries require JSON extraction.

---

### 3. Model identity

| Field | Langfuse SDK key | OTEL attribute | llogr column | Notes |
|-------|-----------------|----------------|--------------|-------|
| Model name | `body.model` | `langfuse.observation.model.name` | `model` | e.g. `claude-sonnet-4-6` |
| Provider | `body.metadata.provider` | — | `provider` | e.g. `anthropic`; extracted from `metadata` |
| Model parameters | `body.modelParameters` | `langfuse.observation.model.parameters` | ❌ body only | `temperature`, `max_tokens`, `top_p`, `frequency_penalty`, `presence_penalty`, `seed` — yallmp sends all six |
| Environment | `body.environment` | — | ❌ body only | Set via `TRACING_ENVIRONMENT` config; promoted to OTEL resource attribute |

**Gaps:** `modelParameters` is sent by yallmp for all six standard params — useful for
regression analysis (e.g. a latency spike caused by a `max_tokens` bump). Not promoted
to a column, so param-aware queries require JSON extraction. `environment` is now sent
via the `tracing_environment` config setting (all clients, including per-group ones) but
has no llogr column — environment-scoped queries need JSON extraction.

---

### 4. Token usage

| Field | Langfuse SDK key | OTEL attribute | llogr column | Notes |
|-------|-----------------|----------------|--------------|-------|
| Input tokens | `body.usage.input` | `langfuse.observation.usage_details` | `input_tokens` | |
| Output tokens | `body.usage.output` | `langfuse.observation.usage_details` | `output_tokens` | |
| Total tokens | `body.usage.total` | `langfuse.observation.usage_details` | `total_tokens` | |
| Cache read tokens | `body.usageDetails.cache_read_input_tokens` | — | ❌ body only | yallmp sends as `usage_details.cache_read_input` |
| Cache write tokens | `body.usageDetails.cache_creation_input_tokens` | — | ❌ body only | yallmp sends as `usage_details.cache_creation_input` |
| Reasoning tokens | `body.usageDetails.reasoning_tokens` | — | ❌ body only | yallmp sends as `usage_details.reasoning` (OpenAI o-series via `completion_tokens_details.reasoning_tokens`) |
| Anthropic thinking | — | — | ❌ body only | yallmp extracts thinking blocks into `metadata.thinking`; no dedicated column |

**Gaps:** yallmp now forwards all extended token types into `usageDetails`:
`cache_read_input`, `cache_creation_input` (Anthropic), and `reasoning` (OpenAI o-series).
They reach `body` but are not promoted to columns — accurate cache-aware cost calculations
require JSON extraction:

```
effective_input_cost = (input_tokens - cache_read_input) * price_in
                     + cache_read_input * cache_price_in
                     + cache_creation_input * cache_write_price
```

Anthropic extended thinking content (`thinking` blocks) is stored in `metadata.thinking`
as a string or list of strings, separated from the regular `output`. This keeps the
Langfuse UI readable but means thinking text is only searchable via full-text scan on
`body`.

Token counts for Anthropic native responses (`input_tokens` / `output_tokens`) are now
handled alongside OpenAI field names, so usage data is correct regardless of whether the
response goes through an OpenAI-compat adapter.

---

### 5. Cost

| Field | Langfuse SDK key | OTEL attribute | llogr column | Notes |
|-------|-----------------|----------------|--------------|-------|
| Total cost | `body.costDetails.total` | `langfuse.observation.cost_details` | `cost` | USD |
| Input cost | `body.costDetails.input` | `langfuse.observation.cost_details` | `input_cost` | |
| Output cost | `body.costDetails.output` | `langfuse.observation.cost_details` | `output_cost` | |

All three cost fields are promoted — billing aggregations are fast.

---

### 6. Quality signals

| Field | Langfuse SDK key | OTEL attribute | llogr column | Notes |
|-------|-----------------|----------------|--------------|-------|
| Finish reason | `body.output.choices[0].finish_reason` | — | `finish_reason` | `stop` / `length` / `tool_calls` / `content_filter` |
| Observation level | `body.level` | `langfuse.observation.level` | ❌ body only | yallmp sends `"ERROR"` when `status_code >= 400`, else omits |
| Status message | `body.statusMessage` | `langfuse.observation.status_message` | ❌ body only | yallmp sends `"HTTP {status_code}"` on errors |
| Scores | `score-create` event | — | ❌ separate event | `faithfulness`, `relevance`, `hallucination`, etc. |

**Gaps:** `level` and `statusMessage` are sent correctly by yallmp for upstream HTTP
errors. Without a promoted `level` column, queries like "all ERROR spans in the last
hour" still require full JSON scans. Note: only upstream 4xx/5xx errors are marked;
model-level issues (hallucination, truncation) are not reflected in `level` — they
need explicit scoring.

`finish_reason = length` means the model hit `max_tokens` — a silent truncation that
corrupts downstream JSON parsing. It should be monitored as an error class.

---

### 7. Prompt identity

| Field | Langfuse SDK key | OTEL attribute | llogr column | Notes |
|-------|-----------------|----------------|--------------|-------|
| Prompt template name | `body.promptName` | `langfuse.observation.prompt_name` | ❌ body only | yallmp sends via `x-prompt-name` request header |
| Prompt template version | `body.promptVersion` | `langfuse.observation.prompt_version` | ❌ body only | yallmp sends via `x-prompt-version` request header |
| Prompt hash (computed) | — | — | `prompt_hash` | SHA-256[:8] of system messages; llogr-specific surrogate |

`prompt_hash` is llogr's lightweight surrogate for prompt versioning: stable across all
calls sharing the same system prompt text, changes on every system prompt edit — works
even without explicit `promptName`.

`promptName` + `promptVersion` are now sent by yallmp when the caller passes the
`x-prompt-name` / `x-prompt-version` HTTP headers. They land in `body` (via OTEL
attributes `OBSERVATION_PROMPT_NAME` / `OBSERVATION_PROMPT_VERSION`) but have no llogr
column — prompt A/B queries need JSON extraction or a `prompt_name` column migration.

---

### 8. Content

| Field | Langfuse SDK key | OTEL attribute | llogr column | Notes |
|-------|-----------------|----------------|--------------|-------|
| Input | `body.input` | `langfuse.observation.input` | `input_hash` (hash only) | Full content in `body` |
| Output | `body.output` | `langfuse.observation.output` | ❌ body only | |

`input_hash` enables grouping of all traces that received the same input (useful for
cache analysis, deduplication, regression testing). Full content is in `body` and
accessible via full-text search (`body ILIKE '%query%'`).

---

### 9. Retrieval / RAG fields

| Field | Langfuse SDK key | OTEL attribute | llogr column | Notes |
|-------|-----------------|----------------|--------------|-------|
| Retrieval query | `body.input.query` | — | `retrieval_query` | Promoted from search spans |
| Result count | `body.output.result_count` | — | `result_count` | Number of docs retrieved |

These are llogr-specific promoted fields — not part of Langfuse or OTEL schemas. They
enable queries like "which retrieval queries returned zero results."

---

### 10. Context / release

| Field | Langfuse SDK key | OTEL attribute | llogr column | Notes |
|-------|-----------------|----------------|--------------|-------|
| Release | `body.release` | — | ❌ body only | Set via `TRACING_RELEASE` config (e.g. git SHA or semver) |
| Tags | `body.tags` | — | ❌ body only | Array of labels (e.g. `["rag", "prod"]`) |
| Version | `body.version` | `langfuse.version` | ❌ body only | SDK / schema version |

---

## Summary: what llogr has vs. what's missing

### Promoted to indexed columns

```
event_id, event_type, timestamp, project_id
trace_id, session_id, parent_span_id
model, name, provider
start_time, end_time, duration_ms
input_tokens, output_tokens, total_tokens
cost, input_cost, output_cost
finish_reason
retrieval_query, result_count
input_hash, prompt_hash
```

### Ingestion headers that mutate trace data

| Header | Effect |
|--------|--------|
| `x-agent-name` | Overwrites `body.name` (→ `name` column) on **all** events unconditionally |
| `x-session-id` | Stamps `body.sessionId` only on events that have no `sessionId` yet |

### Present in body only (no column — JSON extraction required)

✅ = yallmp sends this field; ⚠️ = partial; ❌ = not instrumented at all

| Field | Sent by yallmp | Why it matters |
|-------|---------------|----------------|
| `userId` | ✅ `group_id.split("/")[1]` | Per-user cost, error rate, quota |
| `completionStartTime` | ⚠️ real TTFT for streaming; `None` for non-streaming | P95 TTFT analysis |
| `level` / `statusMessage` | ✅ on HTTP errors only | Error alerting; model-level failures not covered |
| `modelParameters` | ✅ 6 params | Regression analysis (was it a param change?) |
| `tags` | ✅ `[provider, agent_name]` | Filtering by provider or agent in UI |
| `usageDetails` | ✅ cache tokens (Anthropic) + reasoning tokens (OpenAI o-series) | Cache/reasoning cost accuracy |
| `environment` | ✅ via `TRACING_ENVIRONMENT` config | Separate prod from dev in dashboards |
| `promptName` / `promptVersion` | ⚠️ via `x-prompt-name` / `x-prompt-version` headers (optional) | Prompt A/B analysis |
| `release` | ✅ via `TRACING_RELEASE` config | Correlate regressions with deploys |
| Anthropic thinking content | ⚠️ stored in `metadata.thinking`; no dedicated column | Inspect reasoning for quality review |

---

## yallmp-specific metadata fields

yallmp stores extra context in `body.metadata` that has no standard Langfuse SDK
equivalent. These keys are accessible via JSON extraction from the `body` column.

### LLM proxy spans (`trace_proxy_request`)

| Metadata key | Type | Description |
|---|---|---|
| `provider` | str | LLM provider (e.g. `anthropic`, `openai`) — also promoted to `provider` column |
| `group_id` | str | Full org/user composite key (e.g. `myorg/alice`) |
| `is_streaming` | bool | Whether the request used streaming mode |
| `status_code` | int | HTTP status code returned by the upstream LLM API |
| `duration_ms` | float | End-to-end latency in ms — also promoted to `duration_ms` column |
| `agent_name` | str | Agent name, if provided (also in `tags`) |
| `tools_defined` | list[str] | Names of tools/functions passed in the request |
| `tool_calls` | list[str] | Names of tools the model actually called |
| `cost` / `input_cost` / `output_cost` | float | Legacy cost fields (llogr falls back to these if `costDetails` absent) |

### Search spans (`trace_search_request`)

| Metadata key | Type | Description |
|---|---|---|
| `provider` | str | Search provider (e.g. `brave`, `serper`) |
| `group_id` | str | Org/user composite key |
| `status_code` | int | HTTP status from the search API |
| `duration_ms` | float | Request latency |
| `num_results_requested` | int | How many results were asked for |
| `num_results_returned` | int | How many came back — also promoted to `result_count` column |
| `cost` | float | Per-search cost |

Note: `tools_defined` and `tool_calls` enable tool-use analysis (which tools were
available vs. which were actually invoked). Neither is promoted to a column — tool-usage
distribution queries require JSON extraction or a dedicated column migration.

---

## Recommendations for instrumentation

### Always send

```python
langfuse.generation(
    name="llm-call",
    trace_id=trace_id,
    session_id=session_id,       # group conversation turns
    model="claude-sonnet-4-6",
    start_time=start,
    end_time=end,
    completion_start_time=ttft,  # time-to-first-token
    input=messages,
    output=response,
    usage={
        "input": usage.input_tokens,
        "output": usage.output_tokens,
        "total": usage.input_tokens + usage.output_tokens,
    },
    usage_details={              # Anthropic-specific
        "cache_read_input_tokens": usage.cache_read_input_tokens,
        "cache_creation_input_tokens": usage.cache_creation_input_tokens,
    },
    metadata={"provider": "anthropic", "environment": "production"},
    level="DEFAULT",             # or "ERROR" on exception
)
```

### Always send on errors

```python
langfuse.generation(
    ...
    level="ERROR",
    status_message=str(exception),
)
```

### For RAG pipelines

```python
langfuse.span(
    name="retrieval",
    input={"query": user_query},
    output={"result_count": len(docs), "results": docs},
)
```
`retrieval_query` and `result_count` are promoted to columns by llogr automatically.

### For prompt versioning

```python
langfuse.generation(
    prompt_name="my-system-prompt",
    prompt_version=3,
)
```
Without `promptName`, llogr computes `prompt_hash` from system message content — stable
across calls on the same template, changes on every system prompt edit.

---

## OTEL path (Langfuse v3 SDK)

When using the OTEL transport (`/api/public/otel/v1/traces`), the SDK sets OTLP span
attributes. The mapping llogr applies:

| OTEL attribute | Maps to llogr body field |
|----------------|--------------------------|
| `langfuse.observation.type` | → `event_type` (generation vs span) |
| `langfuse.observation.input` | → `body.input` |
| `langfuse.observation.output` | → `body.output` |
| `langfuse.observation.model.name` | → `body.model` |
| `langfuse.observation.usage_details` | → `body.usage` |
| `langfuse.observation.cost_details` | → `body.costDetails` |
| `langfuse.observation.model.parameters` | → `body.modelParameters` |
| `langfuse.observation.level` | → `body.level` |
| `langfuse.observation.status_message` | → `body.statusMessage` |
| `langfuse.observation.metadata.*` | → `body.metadata` |
| `langfuse.trace.input` | → `body.input` (trace-level fallback) |
| `langfuse.trace.output` | → `body.output` (trace-level fallback) |
| `user.id` | → `body.userId` |
| `session.id` | → `body.sessionId` |
| `langfuse.version` | → `body.version` |

The OTEL path does not carry `completionStartTime`, `promptName`, `promptVersion`,
`release`, `tags`, or `usageDetails` — these are Langfuse-specific fields only
available via the HTTP ingestion path.

---

## OTEL path (Google ADK / OTel GenAI semantic conventions)

The same `/api/public/otel/v1/traces` endpoint also accepts spans from Google ADK (and
any other SDK following the [OTel GenAI semantic conventions](https://opentelemetry.io/docs/specs/semconv/gen-ai/)).
llogr detects the dialect per span — a span is treated as GenAI/ADK if it carries
`gen_ai.operation_name` or `gen_ai.system`; otherwise it's parsed as Langfuse. The two
dialects can be mixed in the same batch.

Span names: `generate_content {model}` (LLM call), `invoke_agent`, `execute_tool`,
`compact_events`. Only `generate_content` spans become `generation-create` (with model,
usage, and params populated) — everything else becomes `span-create`.

| OTEL attribute | Maps to llogr body field |
|----------------|--------------------------|
| `gen_ai.operation_name` | → `event_type` (`generation-create` iff `generate_content`, else `span-create`) |
| `gen_ai.request.model` | → `body.model` |
| `gen_ai.request.top_p` / `gen_ai.request.max_tokens` | → `body.modelParameters` |
| `gen_ai.usage.input_tokens` / `gen_ai.usage.output_tokens` | → `body.usage` (`input`/`output`/`total`) |
| `gen_ai.response.finish_reasons` (array; first element) | → `body.finishReason` |
| `gen_ai.tool_name` / `gen_ai.agent_name` | → `body.name` (falls back to span name) |
| `gen_ai.system` | → `body.metadata.provider` |
| `gcp.vertex.agent.llm_request` / `tool_call_args` / `data` | → `body.input` |
| `gcp.vertex.agent.llm_response` / `tool_response` | → `body.output` |
| `gcp.vertex.agent.session_id` | → `body.sessionId` |
| `user_id` | → `body.userId` |
| any other `gen_ai.*` / `gcp.vertex.agent.*` key (e.g. `invocation_id`, `event_id`, `tool_call_id`) | → `body.metadata.*` |

**Gaps:** ADK does not emit cost data, so `cost` / `input_cost` / `output_cost` stay `0`
for ADK-sourced spans — llogr has no model-price table to derive cost from token counts.
`gen_ai.response.finish_reasons` is set as an OTLP array attribute; llogr's protobuf
flattening decodes `array_value` (JSON-encodes it) specifically to support this field.
`finishReason` is a top-level body field rather than nested under `output.choices[0]`
(Gemini responses aren't OpenAI-shaped), so `_extract_finish_reason` checks both shapes.
