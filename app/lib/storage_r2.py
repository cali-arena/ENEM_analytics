"""
Cloudflare R2 (S3-compatible) path and config helpers.

Expects Streamlit secrets or env:
  R2_ACCESS_KEY, R2_SECRET_KEY, R2_ENDPOINT, R2_BUCKET, R2_REGION (optional)
"""
from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlparse


# Default path layout on R2 (override via secrets)
DEFAULT_SILVER_PREFIX = "silver"
DEFAULT_GOLD_PREFIX = "gold"
DEFAULT_BRONZE_PREFIX = "bronze"


def get_r2_config(secrets: dict[str, Any] | None = None) -> dict[str, str]:
    """
    Build R2/S3 config from Streamlit secrets or os.environ.
    Returns dict with: access_key, secret_key, endpoint, bucket, region, endpoint_host.
    """
    s = secrets or {}
    env = os.environ

    def _get(key: str, default: str = "") -> str:
        return (s.get(key) or env.get(key) or default).strip()

    endpoint = _get("R2_ENDPOINT", "https://placeholder.r2.cloudflarestorage.com")
    # DuckDB httpfs expects endpoint as host (no scheme) for custom S3
    parsed = urlparse(endpoint)
    endpoint_host = parsed.netloc or endpoint.replace("https://", "").replace("http://", "")

    return {
        "access_key": _get("R2_ACCESS_KEY"),
        "secret_key": _get("R2_SECRET_KEY"),
        "endpoint": endpoint,
        "endpoint_host": endpoint_host,
        "bucket": _get("R2_BUCKET", "datalake"),
        "region": _get("R2_REGION", "auto"),
        "silver_prefix": _get("R2_SILVER_PREFIX", DEFAULT_SILVER_PREFIX),
        "gold_prefix": _get("R2_GOLD_PREFIX", DEFAULT_GOLD_PREFIX),
        "bronze_prefix": _get("R2_BRONZE_PREFIX", DEFAULT_BRONZE_PREFIX),
    }


def s3_uri(bucket: str, prefix: str, glob: bool = True) -> str:
    """Return S3 URI for layer. If glob, append /**/*.parquet for DuckDB read_parquet."""
    base = f"s3://{bucket}/{prefix}"
    if glob:
        return f"{base}/**/*.parquet"
    return base


def silver_s3_uri(config: dict[str, str]) -> str:
    return s3_uri(config["bucket"], config["silver_prefix"])


def gold_s3_uri(config: dict[str, str]) -> str:
    return s3_uri(config["bucket"], config["gold_prefix"])


def bronze_s3_uri(config: dict[str, str]) -> str:
    return s3_uri(config["bucket"], config["bronze_prefix"])
