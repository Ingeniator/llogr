from __future__ import annotations

from prometheus_client import Counter, Histogram

EVENTS_INGESTED = Counter(
    "llogr_events_ingested_total",
    "Total events ingested",
    ["project_id"],
)

S3_SAVE_SECONDS = Histogram(
    "llogr_s3_save_seconds",
    "S3 save latency in seconds",
)

S3_SAVE_ERRORS = Counter(
    "llogr_s3_save_errors_total",
    "S3 save failures",
)

CLICKBEAT_FORWARD_SECONDS = Histogram(
    "llogr_clickbeat_forward_seconds",
    "Clickbeat forward latency in seconds",
)

CLICKBEAT_FORWARD_ERRORS = Counter(
    "llogr_clickbeat_forward_errors_total",
    "Clickbeat forward failures",
)
