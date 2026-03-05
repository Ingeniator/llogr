"""Seed llogr with sample trace data via the ingestion API."""

import hashlib
import json
import os
import time
import uuid

import httpx

BASE_URL = os.environ.get("LLOGR_URL", "http://localhost:8000")
PUBLIC_KEY = "pk-test"
SECRET_KEY = "sk-test"


def make_batch(session_id: str, trace_name: str, user_input: str, output: str) -> dict:
    trace_id = str(uuid.uuid4())
    gen_id = str(uuid.uuid4())
    span_id = str(uuid.uuid4())
    ts = f"2026-03-0{1 + hash(trace_name) % 5}T{10 + hash(trace_name) % 12}:00:00Z"

    return {
        "batch": [
            {
                "id": trace_id,
                "timestamp": ts,
                "type": "trace-create",
                "body": {"name": trace_name, "input": user_input, "sessionId": session_id},
            },
            {
                "id": span_id,
                "timestamp": ts,
                "type": "span-create",
                "body": {"traceId": trace_id, "name": "retrieval", "input": user_input},
            },
            {
                "id": span_id,
                "timestamp": ts,
                "type": "span-update",
                "body": {"traceId": trace_id, "output": "retrieved 3 documents"},
            },
            {
                "id": gen_id,
                "timestamp": ts,
                "type": "generation-create",
                "body": {
                    "traceId": trace_id,
                    "name": "chat",
                    "model": "gpt-4o",
                    "input": [{"role": "user", "content": user_input}],
                    "output": output,
                    "usage": {"input": 150, "output": 80, "unit": "TOKENS"},
                },
            },
            {
                "id": str(uuid.uuid4()),
                "timestamp": ts,
                "type": "score-create",
                "body": {
                    "traceId": trace_id,
                    "name": "quality",
                    "value": round(0.7 + hash(output) % 30 / 100, 2),
                    "dataType": "NUMERIC",
                },
            },
        ],
    }


SAMPLES = [
    ("session-alpha", "summarize-doc", "Summarize the Q4 earnings report", "Revenue grew 15% YoY..."),
    ("session-alpha", "followup-question", "What about operating margins?", "Operating margins expanded to 22%..."),
    ("session-beta", "code-review", "Review this Python function for bugs", "Found 2 issues: missing null check..."),
    ("session-beta", "code-fix", "Fix the null check issue", "Added guard clause at line 12..."),
    ("session-gamma", "translate", "Translate to French: Hello world", "Bonjour le monde"),
    ("session-gamma", "translate-back", "Translate to English: Bonjour le monde", "Hello world"),
    ("session-delta", "rag-query", "What is our refund policy?", "Our refund policy allows returns within 30 days..."),
    ("session-delta", "rag-query-2", "How do I request a refund?", "To request a refund, visit your order page..."),
]


def main():
    auth = httpx.BasicAuth(PUBLIC_KEY, SECRET_KEY)

    # Wait for service
    for i in range(30):
        try:
            r = httpx.get(f"{BASE_URL}/health", timeout=2)
            if r.status_code == 200:
                break
        except httpx.ConnectError:
            pass
        time.sleep(1)
    else:
        print("Service not available")
        return

    for session_id, trace_name, user_input, output in SAMPLES:
        batch = make_batch(session_id, trace_name, user_input, output)
        r = httpx.post(
            f"{BASE_URL}/api/public/ingestion",
            json=batch,
            auth=auth,
            headers={"X-Session-Id": session_id},
        )
        print(f"  {trace_name}: {r.status_code}")

    print(f"\nSeeded {len(SAMPLES)} batches. Open http://localhost:8000")
    print(f"  Public Key: {PUBLIC_KEY}")
    print(f"  Secret Key: {SECRET_KEY}")


if __name__ == "__main__":
    main()
