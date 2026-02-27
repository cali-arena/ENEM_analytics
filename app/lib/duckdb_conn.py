"""
DuckDB connection over Cloudflare R2 (S3-compatible) using httpfs.

- Loads httpfs extension and configures S3/R2 (path-style, custom endpoint).
- Use st.cache_resource(get_connection) in Streamlit for a single connection.
"""
from __future__ import annotations

from typing import Any

from app.lib.storage_r2 import get_r2_config


def get_connection(secrets: dict[str, Any] | None = None):
    """
    Create a DuckDB in-memory connection with httpfs and R2/S3 credentials.
    Caller must pass Streamlit secrets (st.secrets) when running in Streamlit.
    """
    import duckdb

    config = get_r2_config(secrets)
    if not config["access_key"] or not config["secret_key"]:
        raise ValueError(
            "R2_ACCESS_KEY and R2_SECRET_KEY must be set (Streamlit secrets or env)."
        )

    con = duckdb.connect(database=":memory:")
    # Load S3/httpfs support
    con.execute("INSTALL httpfs; LOAD httpfs;")

    # Escape single quotes in credentials so SET commands don't break
    def _esc(s: str) -> str:
        return (s or "").replace("\\", "\\\\").replace("'", "''")

    # R2 uses path-style URLs and custom endpoint
    con.execute("SET s3_url_style = 'path';")
    con.execute("SET s3_use_ssl = true;")
    con.execute(f"SET s3_region = '{_esc(config['region'])}';")
    con.execute(f"SET s3_endpoint = '{_esc(config['endpoint_host'])}';")
    con.execute(f"SET s3_access_key_id = '{_esc(config['access_key'])}';")
    con.execute(f"SET s3_secret_access_key = '{_esc(config['secret_key'])}';")

    return con


def run_self_test(con, silver_uri: str, gold_uri: str) -> tuple[bool, str]:
    """
    Run a lightweight query against silver and gold to verify credentials and paths.
    Returns (success, message).
    """
    for name, uri in [("silver", silver_uri), ("gold", gold_uri)]:
        try:
            con.execute(f"SELECT 1 FROM read_parquet('{uri}', hive_partitioning=1) LIMIT 1")
        except Exception as e:
            return False, f"Failed to read {name} at {uri}: {e}"
    return True, "OK"
