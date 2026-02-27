"""
Insanely fast Streamlit dashboard: Parquet on Cloudflare R2 via DuckDB.

- Reads Parquet directly from R2 (S3-compatible) using DuckDB httpfs.
- Strong caching (cache_resource for conn, cache_data for queries) and minimal memory.
- Schema discovery: detect time/tenant/project/status columns; degrade gracefully.

Run (from repo root):
  streamlit run app/app_s3_duckdb.py

Secrets (Streamlit Cloud or .streamlit/secrets.toml):
  R2_ACCESS_KEY, R2_SECRET_KEY, R2_ENDPOINT, R2_BUCKET, R2_REGION (optional)
"""
from __future__ import annotations

import sys
from pathlib import Path

# Ensure repo root on path for app.lib
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st
import pandas as pd

# Optional: plotly for charts (already in repo)
try:
    import plotly.express as px
    import plotly.graph_objects as go
    HAS_PLOTLY = True
except Exception:
    HAS_PLOTLY = False

from app.lib.storage_r2 import (
    get_r2_config,
    silver_s3_uri,
    gold_s3_uri,
)
from app.lib.duckdb_conn import get_connection, run_self_test


# -----------------------------------------------------------------------------
# Schema discovery: candidate column names (lowercase match)
# -----------------------------------------------------------------------------
TIME_COL_CANDIDATES = [
    "created_at", "timestamp", "date", "event_time", "ano", "dt", "datetime",
    "data", "data_inicio", "event_date",
]
TENANT_COL_CANDIDATES = ["tenant_id", "tenant", "org_id", "account_id", "sg_uf_residencia"]
PROJECT_COL_CANDIDATES = ["project_id", "project", "slug", "id_perfil"]
STATUS_COL_CANDIDATES = ["status", "state", "priority", "prioridade", "tp_sexo", "faixa_renda"]


def _detect_column(candidates: list[str], columns: list[str]) -> str | None:
    """Return first column name (from schema) that matches any candidate (case-insensitive)."""
    lower_cols = [c.lower() for c in columns]
    for cand in candidates:
        for i, col in enumerate(columns):
            if lower_cols[i] == cand or cand in lower_cols[i]:
                return col
    return None


def discover_schema(con, silver_uri: str, gold_uri: str) -> dict:
    """
    Return schema info for silver and gold: columns, and detected time/tenant/project/status.
    Uses LIMIT 1 to avoid loading full data. Graceful on missing layers.
    """
    out = {
        "silver_columns": [],
        "gold_columns": [],
        "time_col_silver": None,
        "time_col_gold": None,
        "tenant_col_silver": None,
        "tenant_col_gold": None,
        "project_col_silver": None,
        "project_col_gold": None,
        "status_col_silver": None,
        "status_col_gold": None,
        "silver_ok": False,
        "gold_ok": False,
    }
    # Silver
    try:
        df = con.execute(
            f"SELECT * FROM read_parquet('{silver_uri}', hive_partitioning=0) LIMIT 1"
        ).fetchdf()
        cols = df.columns.tolist()
        out["silver_columns"] = cols
        out["silver_ok"] = True
        out["time_col_silver"] = _detect_column(TIME_COL_CANDIDATES, cols)
        out["tenant_col_silver"] = _detect_column(TENANT_COL_CANDIDATES, cols)
        out["project_col_silver"] = _detect_column(PROJECT_COL_CANDIDATES, cols)
        out["status_col_silver"] = _detect_column(STATUS_COL_CANDIDATES, cols)
    except Exception:
        pass
    # Gold: try first table-like path (gold often has multiple parquet trees; we scan one)
    try:
        df = con.execute(
            f"SELECT * FROM read_parquet('{gold_uri}', hive_partitioning=0) LIMIT 1"
        ).fetchdf()
        cols = df.columns.tolist()
        out["gold_columns"] = cols
        out["gold_ok"] = True
        out["time_col_gold"] = _detect_column(TIME_COL_CANDIDATES, cols)
        out["tenant_col_gold"] = _detect_column(TENANT_COL_CANDIDATES, cols)
        out["project_col_gold"] = _detect_column(PROJECT_COL_CANDIDATES, cols)
        out["status_col_gold"] = _detect_column(STATUS_COL_CANDIDATES, cols)
    except Exception:
        pass
    return out


# -----------------------------------------------------------------------------
# Cached DuckDB connection (single per session)
# -----------------------------------------------------------------------------
@st.cache_resource
def _cached_connection(secrets_dict: dict):
    """Build DuckDB connection with R2 config. Cached for the session."""
    return get_connection(secrets_dict)


# -----------------------------------------------------------------------------
# Cached queries (TTL + max_entries to limit memory)
# -----------------------------------------------------------------------------
def _get_con():
    """Get DuckDB connection from session state (set in main before any cached query)."""
    return st.session_state.get("_duckdb_con")


@st.cache_data(ttl=600, max_entries=50)
def _query_silver_count(con_key: int, silver_uri: str) -> int:
    con = _get_con()
    if con is None:
        return 0
    try:
        return con.execute(
            f"SELECT count(*) AS n FROM read_parquet('{silver_uri}', hive_partitioning=0)"
        ).fetchone()[0]
    except Exception:
        return 0


@st.cache_data(ttl=600, max_entries=50)
def _query_gold_count(con_key: int, gold_uri: str) -> int:
    con = _get_con()
    if con is None:
        return 0
    try:
        return con.execute(
            f"SELECT count(*) AS n FROM read_parquet('{gold_uri}', hive_partitioning=0)"
        ).fetchone()[0]
    except Exception:
        return 0


@st.cache_data(ttl=600, max_entries=50)
def _query_latest_ts(con_key: int, uri: str, time_col: str) -> str | None:
    con = _get_con()
    if con is None or not time_col:
        return None
    try:
        safe_col = f'"{time_col}"' if not time_col.replace("_", "").isalnum() else time_col
        r = con.execute(
            f"SELECT max({safe_col}) AS m FROM read_parquet('{uri}', hive_partitioning=0)"
        ).fetchone()[0]
        return str(r) if r is not None else None
    except Exception:
        return None


@st.cache_data(ttl=600, max_entries=50)
def _query_distinct_count(con_key: int, uri: str, col: str) -> int:
    con = _get_con()
    if con is None or not col:
        return 0
    try:
        safe_col = f'"{col}"' if not col.replace("_", "").isalnum() else col
        return con.execute(
            f"SELECT count(DISTINCT {safe_col}) AS n FROM read_parquet('{uri}', hive_partitioning=0)"
        ).fetchone()[0]
    except Exception:
        return 0


@st.cache_data(ttl=600, max_entries=50)
def _query_timeseries(con_key: int, uri: str, time_col: str, limit: int = 500) -> pd.DataFrame:
    con = _get_con()
    if con is None or not time_col:
        return pd.DataFrame()
    try:
        safe_col = f'"{time_col}"' if not time_col.replace("_", "").isalnum() else time_col
        q = f"""
        SELECT {safe_col} AS dt, count(*) AS cnt
        FROM read_parquet('{uri}', hive_partitioning=0)
        WHERE {safe_col} IS NOT NULL
        GROUP BY 1 ORDER BY 1
        LIMIT {limit}
        """
        return con.execute(q).fetchdf()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600, max_entries=50)
def _query_top_categories(con_key: int, uri: str, col: str, limit: int = 20) -> pd.DataFrame:
    con = _get_con()
    if con is None or not col:
        return pd.DataFrame()
    try:
        safe_col = f'"{col}"' if not col.replace("_", "").isalnum() else col
        q = f"""
        SELECT {safe_col} AS category, count(*) AS cnt
        FROM read_parquet('{uri}', hive_partitioning=0)
        WHERE {safe_col} IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC
        LIMIT {limit}
        """
        return con.execute(q).fetchdf()
    except Exception:
        return pd.DataFrame()


# -----------------------------------------------------------------------------
# Page
# -----------------------------------------------------------------------------
def main():
    st.set_page_config(page_title="Data Lake (R2 + DuckDB)", layout="wide")
    st.title("Data Lake — Parquet on R2 (DuckDB)")

    # Secrets: from Streamlit or env
    try:
        secrets = dict(st.secrets) if hasattr(st, "secrets") and st.secrets else {}
    except Exception:
        secrets = {}
    config = get_r2_config(secrets)
    if not config["access_key"] or not config["secret_key"]:
        st.error(
            "R2 credentials missing. Set **R2_ACCESS_KEY** and **R2_SECRET_KEY** in "
            "Streamlit Cloud Secrets (or `.streamlit/secrets.toml` locally). "
            "See `.streamlit/secrets.toml.example`."
        )
        st.stop()

    silver_uri = silver_s3_uri(config)
    gold_uri = gold_s3_uri(config)

    # Connection (cached)
    try:
        con = _cached_connection(secrets)
        st.session_state["_duckdb_con"] = con
    except Exception as e:
        st.error(f"Failed to create DuckDB connection: {e}. Check R2_ENDPOINT and credentials.")
        st.stop()

    # Self-test on load
    ok, msg = run_self_test(con, silver_uri, gold_uri)
    if not ok:
        hint_403 = ""
        if "403" in msg or "Forbidden" in msg:
            hint_403 = (
                " **HTTP 403:** No painel do Cloudflare R2, o token de API precisa ter permissão "
                "**Object Read** (e listagem) no bucket. Crie um novo token em R2 → Manage R2 API Tokens "
                "com acesso de leitura ao bucket `datalake` e use o novo Access Key e Secret nas Secrets. "
            )
        st.error(
            f"**Connection self-test failed:** {msg}. "
            "Check bucket name (R2_BUCKET), path prefixes (silver/gold), and R2 permissions. "
            "Ensure endpoint is exactly `https://<accountid>.r2.cloudflarestorage.com` (no spaces)."
            + hint_403
        )
        st.stop()

    # Schema discovery (cached per session by running once)
    if "_schema" not in st.session_state:
        with st.spinner("Discovering schema…"):
            st.session_state["_schema"] = discover_schema(con, silver_uri, gold_uri)
    schema = st.session_state["_schema"]

    # Refresh cache button
    if st.sidebar.button("Refresh cache"):
        _query_silver_count.clear()
        _query_gold_count.clear()
        _query_latest_ts.clear()
        _query_distinct_count.clear()
        _query_timeseries.clear()
        _query_top_categories.clear()
        st.rerun()

    # Sidebar filters (degrade if columns missing)
    st.sidebar.subheader("Filters")
    time_col = schema.get("time_col_silver") or schema.get("time_col_gold")
    tenant_col = schema.get("tenant_col_silver") or schema.get("tenant_col_gold")
    project_col = schema.get("project_col_silver") or schema.get("project_col_gold")
    status_col = schema.get("status_col_silver") or schema.get("status_col_gold")

    filter_date_range = None
    if time_col:
        st.sidebar.caption(f"Time column: `{time_col}`")
        # Optional: date range (we could query min/max and add sliders; keep simple)
    else:
        st.sidebar.info("No time column detected. Available columns shown in Schema Inspector.")

    con_key = id(con)

    # KPIs
    st.subheader("KPIs")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        n_silver = _query_silver_count(con_key, silver_uri)
        st.metric("Silver rows", f"{n_silver:,}")
    with c2:
        n_gold = _query_gold_count(con_key, gold_uri)
        st.metric("Gold rows", f"{n_gold:,}")
    with c3:
        latest = _query_latest_ts(con_key, silver_uri, time_col) if time_col else _query_latest_ts(con_key, gold_uri, time_col)
        st.metric("Latest (time)", latest or "—")
    with c4:
        distinct_n = _query_distinct_count(con_key, silver_uri, tenant_col) if tenant_col else _query_distinct_count(con_key, gold_uri, tenant_col)
        st.metric("Distinct tenants" if tenant_col else "Distinct (tenant col)", f"{distinct_n:,}" if tenant_col else "—")

    # Schema inspector
    st.subheader("Schema Inspector")
    with st.expander("Silver & Gold columns and detected dimensions", expanded=False):
        st.write("**Silver** (ok=", schema["silver_ok"], "): ", schema["silver_columns"] or "—")
        st.write("**Gold** (ok=", schema["gold_ok"], "): ", schema["gold_columns"] or "—")
        st.write(
            "Detected — Time:", time_col or "—",
            "| Tenant:", tenant_col or "—",
            "| Project:", project_col or "—",
            "| Status:", status_col or "—",
        )

    # Charts (only if we have columns)
    st.subheader("Charts")

    if time_col:
        uri_ts = silver_uri if schema["silver_ok"] else gold_uri
        df_ts = _query_timeseries(con_key, uri_ts, time_col)
        if not df_ts.empty:
            if HAS_PLOTLY:
                fig = px.line(df_ts, x="dt", y="cnt", title=f"Count by {time_col}")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.line_chart(df_ts.set_index("dt"))
        else:
            st.caption("No time-series data (or query returned no rows).")
    else:
        st.caption("Enable time-series by having a detected time column (e.g. ano, date, created_at).")

    # Top categories (use tenant or status or first categorical)
    cat_col = tenant_col or status_col or (schema["silver_columns"][0] if schema["silver_columns"] else schema["gold_columns"][0] if schema["gold_columns"] else None)
    if cat_col:
        uri_cat = silver_uri if schema["silver_ok"] else gold_uri
        df_cat = _query_top_categories(con_key, uri_cat, cat_col)
        if not df_cat.empty:
            if HAS_PLOTLY:
                fig = px.bar(df_cat, x="category", y="cnt", title=f"Top by {cat_col}")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.bar_chart(df_cat.set_index("category"))
        else:
            st.caption("No category data.")

    # Distribution by status/priority if available
    if status_col and status_col != cat_col:
        uri_dist = silver_uri if schema["silver_ok"] else gold_uri
        df_dist = _query_top_categories(con_key, uri_dist, status_col, limit=30)
        if not df_dist.empty and HAS_PLOTLY:
            fig = px.pie(df_dist, values="cnt", names="category", title=f"Distribution by {status_col}")
            st.plotly_chart(fig, use_container_width=True)

    # Small table sample (avoid loading full data)
    st.subheader("Sample (Gold)")
    if schema["gold_ok"]:
        try:
            sample = con.execute(
                f"SELECT * FROM read_parquet('{gold_uri}', hive_partitioning=0) LIMIT 100"
            ).fetchdf()
            st.dataframe(sample, use_container_width=True)
        except Exception as e:
            st.caption(f"Sample load failed: {e}")
    else:
        st.caption("Gold layer not available or empty.")


if __name__ == "__main__":
    main()
