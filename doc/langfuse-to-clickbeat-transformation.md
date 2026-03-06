# Langfuse-to-Clickbeat Transformation Spec

llogr acts as a Langfuse-compatible ingestion server. It accepts standard Langfuse SDK
payloads, stores them in S3, and forwards them to the Clickbeat API in a normalized
envelope format.

## Pipeline Overview

```
Langfuse SDK  ──POST──>  /api/public/ingestion  ──>  Stage 1: save raw to S3 (blocking)
                                                 ──>  Stage 2: forward to Clickbeat (background)
```

- **Stage 1** is synchronous — the HTTP response waits for S3 persistence.
- **Stage 2** runs as a FastAPI background task. Failures are logged but do not affect
  the client response.

---

## Langfuse Ingestion Schema (Input)

### Batch Envelope

| Field      | Type                      | Required | Description                              |
|------------|---------------------------|----------|------------------------------------------|
| `batch`    | `IngestionEvent[]`        | yes      | Array of events to ingest                |
| `metadata` | `dict[str, Any] \| null`  | no       | SDK metadata (name, version, integration)|

### IngestionEvent

| Field       | Type        | Required | Description                        |
|-------------|-------------|----------|------------------------------------|
| `id`        | `str`       | yes      | Unique event identifier            |
| `timestamp` | `str`       | yes      | ISO 8601 timestamp                 |
| `type`      | `EventType` | yes      | Event type (see below)             |
| `body`      | `dict`      | yes      | Event-type-specific payload        |

### EventType (enum)

```
trace-create
span-create
span-update
generation-create
generation-update
event-create
score-create
sdk-log
```

### Event Body Schemas

#### trace-create

| Field         | Type          | Description                              |
|---------------|---------------|------------------------------------------|
| `id`          | `str`         | Trace UUID                               |
| `timestamp`   | `str`         | ISO 8601                                 |
| `name`        | `str`         | Trace name (e.g. pipeline name)          |
| `userId`      | `str`         | End-user identifier                      |
| `sessionId`   | `str`         | Session identifier                       |
| `release`     | `str`         | Application release version              |
| `version`     | `str`         | Trace schema version                     |
| `environment` | `str`         | Deployment environment                   |
| `input`       | `dict`        | Trace input data                         |
| `output`      | `dict`        | Trace output data                        |
| `metadata`    | `dict`        | Arbitrary metadata                       |
| `tags`        | `list[str]`   | Tags for categorization                  |
| `public`      | `bool`        | Whether the trace is publicly visible    |

#### span-create

| Field                 | Type         | Description                          |
|-----------------------|--------------|--------------------------------------|
| `id`                  | `str`        | Span UUID                            |
| `traceId`             | `str`        | Parent trace UUID                    |
| `parentObservationId` | `str\|null`  | Parent span/generation UUID          |
| `name`                | `str`        | Span name                            |
| `startTime`           | `str`        | ISO 8601 start                       |
| `endTime`             | `str`        | ISO 8601 end                         |
| `level`               | `str`        | Log level (DEFAULT, DEBUG, etc.)     |
| `statusMessage`       | `str\|null`  | Status message                       |
| `input`               | `dict`       | Span input                           |
| `output`              | `dict`       | Span output                          |
| `metadata`            | `dict`       | Arbitrary metadata                   |
| `environment`         | `str`        | Deployment environment               |

#### span-update

| Field     | Type   | Description                              |
|-----------|--------|------------------------------------------|
| `id`      | `str`  | Span UUID (must match existing span)     |
| `traceId` | `str`  | Parent trace UUID                        |
| `endTime` | `str`  | Updated end time                         |
| `output`  | `dict` | Updated output                           |

#### generation-create

| Field                 | Type         | Description                          |
|-----------------------|--------------|--------------------------------------|
| `id`                  | `str`        | Generation UUID                      |
| `traceId`             | `str`        | Parent trace UUID                    |
| `parentObservationId` | `str\|null`  | Parent span UUID                     |
| `name`                | `str`        | Generation name                      |
| `startTime`           | `str`        | ISO 8601 start                       |
| `endTime`             | `str`        | ISO 8601 end                         |
| `completionStartTime` | `str`       | Time-to-first-token                  |
| `model`               | `str`        | Model identifier (e.g. `gpt-4o`)    |
| `modelParameters`     | `dict`       | Model params (temperature, etc.)     |
| `input`               | `list\|dict` | Prompt messages or structured input  |
| `output`              | `dict`       | Model response                       |
| `usage`               | `dict`       | Token usage: `{input, output, total, unit}` |
| `usageDetails`        | `dict`       | Detailed usage breakdown             |
| `costDetails`         | `dict`       | Cost per component: `{input, output, total}` |
| `promptName`          | `str`        | Prompt template name                 |
| `promptVersion`       | `int`        | Prompt template version              |
| `level`               | `str`        | Log level                            |
| `environment`         | `str`        | Deployment environment               |
| `metadata`            | `dict`       | Arbitrary metadata                   |

#### generation-update

| Field     | Type   | Description                              |
|-----------|--------|------------------------------------------|
| `id`      | `str`  | Generation UUID (must match existing)    |
| `traceId` | `str`  | Parent trace UUID                        |
| `endTime` | `str`  | Updated end time                         |
| `output`  | `dict` | Updated output                           |
| `usage`   | `dict` | Updated token usage                      |

#### event-create

| Field                 | Type        | Description                          |
|-----------------------|-------------|--------------------------------------|
| `id`                  | `str`       | Event UUID                           |
| `traceId`             | `str`       | Parent trace UUID                    |
| `parentObservationId` | `str\|null` | Parent span UUID                     |
| `name`                | `str`       | Event name                           |
| `startTime`           | `str`       | ISO 8601 timestamp                   |
| `level`               | `str`       | Log level                            |
| `input`               | `dict`      | Event input                          |
| `output`              | `dict`      | Event output                         |
| `metadata`            | `dict`      | Arbitrary metadata                   |
| `environment`         | `str`       | Deployment environment               |

#### score-create

| Field           | Type           | Description                          |
|-----------------|----------------|--------------------------------------|
| `id`            | `str`          | Score UUID                           |
| `traceId`       | `str`          | Parent trace UUID                    |
| `observationId` | `str\|null`    | Associated observation UUID          |
| `name`          | `str`          | Score name (e.g. `faithfulness`)     |
| `value`         | `float\|str`   | Numeric value or categorical label   |
| `dataType`      | `str`          | `NUMERIC` or `CATEGORICAL`          |
| `comment`       | `str`          | Human-readable explanation           |
| `environment`   | `str`          | Deployment environment               |

#### sdk-log

| Field | Type   | Description                              |
|-------|--------|------------------------------------------|
| `log` | `dict` | SDK log entry with `level`, `message`, `sdk`, `version`, `extra` |

---

## Clickbeat API Schema (Output)

### Request

```
POST {clickbeat.api_url}
Authorization: Bearer {clickbeat.api_key}
Content-Type: application/json
```

Body is a JSON array of transformed events:

### Clickbeat Event

| Field        | Type   | Description                                          |
|--------------|--------|------------------------------------------------------|
| `event_id`   | `str`  | Original Langfuse event `id`                         |
| `event_type` | `str`  | Original Langfuse event `type`                       |
| `timestamp`  | `str`  | Original Langfuse event `timestamp` (ISO 8601)       |
| `project_id` | `str`  | Auth context `public_key` (from Basic auth username) |
| `payload`    | `dict` | Complete Langfuse event `body`, preserved as-is      |

### Example

Langfuse input event:
```json
{
  "id": "evt-trace-001",
  "timestamp": "2026-03-05T12:00:00.000Z",
  "type": "trace-create",
  "body": {
    "id": "trace-11111111",
    "name": "chat-completion-pipeline",
    "userId": "user-abc123"
  }
}
```

Clickbeat output event:
```json
{
  "event_id": "evt-trace-001",
  "event_type": "trace-create",
  "timestamp": "2026-03-05T12:00:00.000Z",
  "project_id": "pk-lf-my-project",
  "payload": {
    "id": "trace-11111111",
    "name": "chat-completion-pipeline",
    "userId": "user-abc123"
  }
}
```

---

## Transformation Rules

The transformation is a **pass-through with envelope normalization**:

1. `event.id` → `event_id`
2. `event.type` → `event_type`
3. `event.timestamp` → `timestamp`
4. `auth.public_key` → `project_id` (injected from HTTP Basic auth)
5. `event.body` → `payload` (copied verbatim, no schema validation on body contents)

The Langfuse event body is **not** parsed, restructured, or validated beyond Pydantic's
type checks on the envelope fields. Clickbeat receives the full original payload and is
responsible for interpreting it based on `event_type`.

---

## Ingestion Response Schema

HTTP status: `207 Multi-Status`

```json
{
  "successes": [
    { "id": "evt-trace-001", "status": 201 },
    { "id": "evt-span-001", "status": 201 }
  ],
  "errors": []
}
```

| Field       | Type                | Description                     |
|-------------|---------------------|---------------------------------|
| `successes` | `IngestionSuccess[]`| Successfully processed events   |
| `errors`    | `IngestionError[]`  | Failed events with error detail |

### IngestionSuccess

| Field    | Type  | Description           |
|----------|-------|-----------------------|
| `id`     | `str` | Event ID              |
| `status` | `int` | HTTP status (201)     |

### IngestionError

| Field     | Type        | Description           |
|-----------|-------------|-----------------------|
| `id`      | `str`       | Event ID              |
| `status`  | `int`       | HTTP error status     |
| `message` | `str`       | Error description     |
| `error`   | `dict\|null`| Additional error info |

---

## Authentication

- Method: HTTP Basic Authentication
- Username = Langfuse public key (becomes `project_id` in Clickbeat events)
- Password = Langfuse secret key

---

## Source Files

| File | Purpose |
|------|---------|
| `src/llogr/models.py` | Pydantic models for Langfuse ingestion schema |
| `src/llogr/clickbeat.py` | `transform_events()` and `send_to_clickbeat()` |
| `src/llogr/processing.py` | Two-stage pipeline orchestration |
| `src/llogr/routes/ingestion.py` | HTTP endpoint handler |
| `tests/test_langfuse_compat.py` | Langfuse SDK payload compatibility tests |
| `tests/test_clickbeat.py` | Transformation and forwarding unit tests |
