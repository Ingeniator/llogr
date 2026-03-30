"""DuckDB-based full-text search over S3 JSONL log files."""

from __future__ import annotations

import duckdb
import structlog

from llogr.config import Settings

logger = structlog.get_logger(__name__)


def search_logs(
    keys: list[str],
    query: str,
    settings: Settings,
    limit: int = 100,
) -> list[dict]:
    """Search inside JSONL files on S3 using DuckDB.

    Args:
        keys: S3 keys pre-filtered by time/session/trace.
        query: Free-text search string (matched against the full JSON line).
        settings: App settings with S3 config.
        limit: Max results to return.

    Returns:
        List of matching event dicts.
    """
    if not keys:
        return []

    s3_cfg = settings.s3

    endpoint = s3_cfg.endpoint or ""
    use_ssl = endpoint.startswith("https://")
    endpoint_host = endpoint.replace("https://", "").replace("http://", "")

    # DuckDB s3_url_style=path constructs: https://{s3_endpoint}/{bucket}/{key}
    # For OBS (path addressing): endpoint=shared-bucket.obs..., bucket=llogr
    #   → https://shared-bucket.obs.../llogr/key ✓
    # For MinIO (virtual): endpoint=minio:9000, bucket=shared-bucket
    #   → https://minio:9000/shared-bucket/key ✓
    urls = [f"s3://{s3_cfg.bucket}/{k}" for k in keys]

    conn = duckdb.connect(":memory:", config={"extension_directory": settings.features.duckdb_extension_dir})
    try:
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        conn.execute(f"SET s3_endpoint = '{endpoint_host}';")
        conn.execute(f"SET s3_access_key_id = '{s3_cfg.access_key_id}';")
        conn.execute(f"SET s3_secret_access_key = '{s3_cfg.secret_access_key}';")
        conn.execute(f"SET s3_region = '{s3_cfg.region}';")
        conn.execute(f"SET s3_use_ssl = {'true' if use_ssl else 'false'};")
        duckdb_url_style = "vhost" if s3_cfg.addressing_style == "virtual" else "path"
        conn.execute(f"SET s3_url_style = '{duckdb_url_style}';")

        files_list = ", ".join(f"'{u}'" for u in urls)

        sql = f"""
            SELECT *
            FROM read_json_auto([{files_list}],
                 format='newline_delimited',
                 ignore_errors=true,
                 union_by_name=true)
            WHERE CAST(body AS VARCHAR) ILIKE $1
            LIMIT {min(limit, 500)}
        """

        result = conn.execute(sql, [f"%{query}%"])
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()

        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error("duckdb_search_failed", error=str(e))
        return []
    finally:
        conn.close()
