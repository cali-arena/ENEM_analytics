"""
ENEM Opportunity & Equity Radar (2020–2024) — One-page executive dashboard on R2.

100% Cloudflare R2 (Parquet) via DuckDB httpfs. No local CSV. No sidebar.
Same storytelling as app_one_page.py; all data from read_parquet(s3://...).

Run: streamlit run app/app_s3_duckdb.py
Secrets: R2_ACCESS_KEY, R2_SECRET_KEY, R2_ENDPOINT, R2_BUCKET, R2_REGION (optional)
"""
from __future__ import annotations

import os
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st
import pandas as pd

try:
    import plotly.express as px
    import plotly.graph_objects as go
    HAS_PLOTLY = True
except Exception:
    HAS_PLOTLY = False

from app.lib.storage_r2 import get_r2_config, silver_s3_uri, gold_s3_uri
from app.lib.duckdb_conn import get_connection, run_self_test
from app.components import (
    inject_dark_theme_css,
    section_header_anchor,
    section_divider,
    toc_links,
    kpi_row,
    data_sources_badge,
    not_generated_yet,
    tier_card_dark,
)

# -----------------------------------------------------------------------------
# Secrets (Streamlit Cloud pode ter formato que quebra dict(st.secrets))
# -----------------------------------------------------------------------------
def _safe_secrets_dict() -> dict:
    """Constrói um dict flat a partir de st.secrets sem assumir que dict(st.secrets) funciona."""
    out: dict = {}
    try:
        if not hasattr(st, "secrets") or st.secrets is None:
            return out
        raw = st.secrets
        if isinstance(raw, dict):
            for k, v in raw.items():
                if isinstance(v, (str, int, float, bool)):
                    out[str(k)] = str(v).strip()
                elif isinstance(v, dict):
                    for k2, v2 in v.items():
                        if isinstance(v2, (str, int, float, bool)):
                            out[str(k2)] = str(v2).strip()
        else:
            for key in ("R2_ACCESS_KEY", "R2_SECRET_KEY", "R2_ENDPOINT", "R2_BUCKET", "R2_REGION",
                       "DEEPSEEK_API_KEY", "OPENAI_API_KEY", "OLLAMA_BASE_URL", "OLLAMA_MODEL"):
                try:
                    v = raw.get(key) if hasattr(raw, "get") else getattr(raw, key, None)
                    if v is not None:
                        out[key] = str(v).strip()
                except Exception:
                    pass
    except Exception:
        pass
    return out


# -----------------------------------------------------------------------------
# R2 URIs (paths; override via R2_SILVER_PREFIX / R2_GOLD_PREFIX)
# -----------------------------------------------------------------------------
def _gold_kpis_uri(config: dict) -> str:
    return f"s3://{config['bucket']}/{config['gold_prefix']}/kpis_uf_ano/**/*.parquet"

def _gold_cluster_profiles_uri(config: dict) -> str:
    return f"s3://{config['bucket']}/{config['gold_prefix']}/cluster_profiles.parquet"

def _gold_cluster_evolution_uri(config: dict) -> str:
    return f"s3://{config['bucket']}/{config['gold_prefix']}/cluster_evolution_uf_ano.parquet"

def _silver_quality_report_uri(config: dict) -> str:
    return f"s3://{config['bucket']}/{config['silver_prefix']}/quality_report/**/*.parquet"

def _silver_null_report_uri(config: dict) -> str:
    return f"s3://{config['bucket']}/{config['silver_prefix']}/null_report/**/*.parquet"


def _safe_col(name: str) -> str:
    if name.replace("_", "").isalnum():
        return name
    return f'"{name}"'


# -----------------------------------------------------------------------------
# Schema discovery (existing logic; one glob per layer)
# -----------------------------------------------------------------------------
TIME_COL_CANDIDATES = ["ano", "created_at", "timestamp", "date", "event_time", "dt", "datetime"]
TENANT_COL_CANDIDATES = ["sg_uf_residencia", "tenant_id", "tenant", "org_id", "account_id"]

def _detect_column(candidates: list[str], columns: list[str]) -> str | None:
    lower_cols = [c.lower() for c in columns]
    for cand in candidates:
        for i, col in enumerate(columns):
            if lower_cols[i] == cand or cand in lower_cols[i]:
                return columns[i]
    return None


def discover_schema(con, silver_uri: str, gold_uri: str) -> dict:
    out = {
        "silver_columns": [], "gold_columns": [],
        "time_col_silver": None, "time_col_gold": None,
        "tenant_col_silver": None, "tenant_col_gold": None,
        "silver_ok": False, "gold_ok": False,
    }
    for name, uri, key_ok, key_cols, key_time, key_tenant in [
        ("silver", silver_uri, "silver_ok", "silver_columns", "time_col_silver", "tenant_col_silver"),
        ("gold", gold_uri, "gold_ok", "gold_columns", "time_col_gold", "tenant_col_gold"),
    ]:
        try:
            df = con.execute(f"SELECT * FROM read_parquet('{uri}', hive_partitioning=0) LIMIT 1").fetchdf()
            cols = df.columns.tolist()
            out[key_cols] = cols
            out[key_ok] = True
            out[key_time] = _detect_column(TIME_COL_CANDIDATES, cols)
            out[key_tenant] = _detect_column(TENANT_COL_CANDIDATES, cols)
        except Exception:
            pass
    return out


def _layer_available(con, uri: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM read_parquet('{uri}', hive_partitioning=0) LIMIT 1")
        return True
    except Exception:
        return False


# -----------------------------------------------------------------------------
# Cached connection
# -----------------------------------------------------------------------------
@st.cache_resource
def _cached_connection(secrets_dict: dict):
    return get_connection(secrets_dict)


def _get_con():
    return st.session_state.get("_duckdb_con")


# -----------------------------------------------------------------------------
# Cached queries (TTL + max_entries; aggregated / LIMIT)
# -----------------------------------------------------------------------------
@st.cache_data(ttl=600, max_entries=50)
def query_kpis(con_key: int, uri: str, year_min: int, year_max: int, uf: str | None, year_col: str | None = "ano") -> pd.DataFrame:
    con = _get_con()
    if con is None:
        return pd.DataFrame()
    try:
        uf_filter = f" AND {_safe_col('sg_uf_residencia')} = '{str(uf).replace(chr(39), chr(39)+chr(39))}'" if uf and uf != "Todos" else ""
        if year_col:
            ycol = year_col if year_col.startswith('"') else year_col
            q = f"""
            SELECT * FROM read_parquet('{uri}', hive_partitioning=0)
            WHERE {ycol} BETWEEN {year_min} AND {year_max}
              AND (sg_uf_residencia IS NULL OR TRIM(sg_uf_residencia) != '' AND sg_uf_residencia != 'NA')
            {uf_filter}
            """
        else:
            q = f"""
            SELECT * FROM read_parquet('{uri}', hive_partitioning=0)
            WHERE (sg_uf_residencia IS NULL OR (TRIM(sg_uf_residencia) != '' AND sg_uf_residencia != 'NA'))
            {uf_filter}
            """
        return con.execute(q).fetchdf()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600, max_entries=20)
def query_quality_report(con_key: int, uri: str) -> pd.DataFrame:
    con = _get_con()
    if con is None:
        return pd.DataFrame()
    try:
        return con.execute(f"SELECT * FROM read_parquet('{uri}', hive_partitioning=0) LIMIT 50000").fetchdf()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600, max_entries=20)
def query_null_report(con_key: int, uri: str) -> pd.DataFrame:
    con = _get_con()
    if con is None:
        return pd.DataFrame()
    try:
        return con.execute(f"SELECT * FROM read_parquet('{uri}', hive_partitioning=0) LIMIT 50000").fetchdf()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600, max_entries=20)
def query_cluster_profiles(con_key: int, uri: str) -> pd.DataFrame:
    con = _get_con()
    if con is None:
        return pd.DataFrame()
    try:
        return con.execute(f"SELECT * FROM read_parquet('{uri}', hive_partitioning=0)").fetchdf()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600, max_entries=30)
def query_cluster_evolution(con_key: int, uri: str, uf: str | None) -> pd.DataFrame:
    con = _get_con()
    if con is None:
        return pd.DataFrame()
    try:
        uf_filter = f" WHERE uf = '{str(uf).replace(chr(39), chr(39)+chr(39))}'" if uf and uf != "Todos" else ""
        return con.execute(f"SELECT * FROM read_parquet('{uri}', hive_partitioning=0){uf_filter}").fetchdf()
    except Exception:
        return pd.DataFrame()


# -----------------------------------------------------------------------------
# Resumo executivo (determinístico + opcional LLM)
# -----------------------------------------------------------------------------
def build_story_report_r2(
    df_k: pd.DataFrame,
    qr_df: pd.DataFrame,
    nr_df: pd.DataFrame,
    ev_df: pd.DataFrame,
    year_start: int,
    year_end: int,
    uf: str | None,
) -> tuple[str, dict]:
    context_pack = {"kpis": {}, "top_ufs": [], "bottom_ufs": [], "largest_variation": None, "cluster_growth": None, "silver_quality": {}}
    if not df_k.empty:
        agg_cols = {"media_objetiva": "mean", "media_redacao": "mean", "count_participantes": "sum"}
        if "pct_presence_full" in df_k.columns:
            agg_cols["pct_presence_full"] = "mean"
        cols_agg = {k: v for k, v in agg_cols.items() if k in df_k.columns}
        if "ano" in df_k.columns and cols_agg:
            agg_year = df_k.groupby("ano").agg(cols_agg).reset_index()
        else:
            agg_year = df_k.agg({k: v for k, v in cols_agg.items()}).to_frame().T if cols_agg else pd.DataFrame()
            if not agg_year.empty:
                agg_year["ano"] = year_end
        if agg_year.empty:
            agg_year = pd.DataFrame({"ano": [year_end], "media_objetiva": [0.0], "media_redacao": [0.0], "pct_presence": [100.0]})
        else:
            pct_col = "pct_presence_full" if "pct_presence_full" in agg_year.columns else None
            if pct_col:
                agg_year = agg_year.rename(columns={"pct_presence_full": "pct_presence"})
            elif "pct_presence" not in agg_year.columns:
                agg_year["pct_presence"] = 100.0
        last_row = agg_year.sort_values("ano").iloc[-1]
        context_pack["kpis"] = {
            "year_last": int(last_row["ano"]),
            "media_objetiva_last": float(last_row["media_objetiva"]),
            "media_redacao_last": float(last_row["media_redacao"]),
            "pct_presenca_last": float(last_row["pct_presence"]),
            "year_start": year_start, "year_end": year_end, "uf": uf,
        }
        by_uf = df_k.groupby("sg_uf_residencia").agg(
            media_objetiva=("media_objetiva", "mean"),
            participantes=("count_participantes", "sum"),
        ).reset_index()
        by_uf = by_uf[by_uf["sg_uf_residencia"].notna() & (by_uf["sg_uf_residencia"].astype(str) != "NA")]
        by_uf = by_uf.sort_values("media_objetiva", ascending=False)
        context_pack["top_ufs"] = by_uf.head(5).assign(media_objetiva=lambda d: d["media_objetiva"].round(1)).to_dict(orient="records")
        context_pack["bottom_ufs"] = by_uf.tail(5).assign(media_objetiva=lambda d: d["media_objetiva"].round(1)).to_dict(orient="records")
        if len(agg_year) >= 2:
            agg_year = agg_year.sort_values("ano")
            diffs = [{"from_year": int(agg_year.iloc[i-1]["ano"]), "to_year": int(agg_year.iloc[i]["ano"]),
                     "delta_media_objetiva": float(agg_year.iloc[i]["media_objetiva"] - agg_year.iloc[i-1]["media_objetiva"])}
                    for i in range(1, len(agg_year))]
            if diffs:
                context_pack["largest_variation"] = max(diffs, key=lambda d: abs(d["delta_media_objetiva"]))
    if not ev_df.empty and "ano" in ev_df.columns and "uf" in ev_df.columns and "cluster_id" in ev_df.columns and "pct_participants" in ev_df.columns:
        try:
            ev2020 = ev_df[ev_df["ano"] == 2020]
            ev2024 = ev_df[ev_df["ano"] == 2024]
            merged = ev2020.merge(ev2024, on=["uf", "cluster_id"], suffixes=("_2020", "_2024"), how="inner")
            if not merged.empty:
                merged["delta"] = merged["pct_participants_2024"] - merged["pct_participants_2020"]
                top_growth = merged.sort_values("delta", ascending=False).head(1).iloc[0]
                context_pack["cluster_growth"] = {"uf": top_growth["uf"], "cluster_id": int(top_growth["cluster_id"]), "delta_pct": float(top_growth["delta"] * 100)}
        except Exception:
            pass
    if not qr_df.empty and "rule_name" in qr_df.columns and "rows_removed" in qr_df.columns:
        total_removed = int(qr_df["rows_removed"].sum())
        by_rule = qr_df.groupby("rule_name")["rows_removed"].sum().reset_index().sort_values("rows_removed", ascending=False)
        context_pack["silver_quality"]["total_rows_removed"] = total_removed
        context_pack["silver_quality"]["top_rule"] = by_rule.iloc[0].to_dict()
    if not nr_df.empty and "column_name" in nr_df.columns and "pct_null" in nr_df.columns:
        by_col = nr_df.groupby("column_name")["pct_null"].max().reset_index().sort_values("pct_null", ascending=False)
        context_pack["silver_quality"]["top_null_column"] = by_col.iloc[0].to_dict()

    text_md = None
    if os.environ.get("OPENAI_API_KEY"):
        try:
            import openai
            import json
            client = openai.OpenAI(api_key=os.environ["OPENAI_API_KEY"])
            prompt = "Gere um mini-relatório executivo em português, em Markdown, com base apenas no JSON a seguir.\nRegras: 3 a 5 parágrafos curtos; depois 3 ações recomendadas (bullet list). Cite valores numéricos do contexto.\n\nJSON:\n" + str(context_pack)
            r = client.chat.completions.create(model="gpt-4o-mini", messages=[{"role": "system", "content": "Analista de dados educacionais."}, {"role": "user", "content": prompt}], temperature=0.2)
            text_md = r.choices[0].message.content.strip()
        except Exception:
            pass
    if not text_md:
        k = context_pack.get("kpis") or {}
        sq = context_pack.get("silver_quality") or {}
        cg = context_pack.get("cluster_growth") or {}
        p1 = f"Entre {k.get('year_start', year_start)} e {k.get('year_end', year_end)}, a média objetiva no último ano ({k.get('year_last')}) foi de {k.get('media_objetiva_last', 0):.1f}, com média de redação de {k.get('media_redacao_last', 0):.1f}." if k else "Indicadores não disponíveis."
        top_ufs, bottom_ufs = context_pack.get("top_ufs") or [], context_pack.get("bottom_ufs") or []
        p2 = f"Melhor UF: {top_ufs[0]['sg_uf_residencia']} ({top_ufs[0]['media_objetiva']:.1f}); menor: {bottom_ufs[0]['sg_uf_residencia']} ({bottom_ufs[0]['media_objetiva']:.1f})." if top_ufs and bottom_ufs else ""
        tr, tn = sq.get("top_rule") or {}, sq.get("top_null_column") or {}
        p3 = f"Silver: {sq.get('total_rows_removed', 0)} linhas removidas; regra '{tr.get('rule_name','')}'; coluna '{tn.get('column_name','')}' com {tn.get('pct_null',0):.1f}% nulos." if sq else ""
        p4 = f"Cluster {cg.get('cluster_id')} cresceu {cg.get('delta_pct',0):.1f} p.p. na UF {cg.get('uf')}." if cg else ""
        bullets = ["- Priorizar UFs com menor média e presença.", "- Revisar regras Silver e campos nulos.", "- Explorar clusters por território."]
        text_md = "# Resumo executivo\n\n" + p1 + "\n\n" + p2 + "\n\n" + p3 + "\n\n" + p4 + "\n\n## Ações recomendadas\n" + "\n".join(bullets)
    return text_md, context_pack


# -----------------------------------------------------------------------------
# Explicação de gráfico (heurística + opcional LLM)
# -----------------------------------------------------------------------------
def _heuristic_explanation(title: str, df: pd.DataFrame, filters: dict | None) -> str:
    if df is None or df.empty:
        return f"O gráfico **{title}** está vazio."
    filters = filters or {}
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    if not numeric_cols:
        return f"**{title}**: sem métricas numéricas."
    if "ano" in df.columns:
        metric_col = next((c for c in numeric_cols if c != "ano"), numeric_cols[0])
        df_s = df.sort_values("ano")
        first_val = float(df_s[metric_col].iloc[0])
        last_val = float(df_s[metric_col].iloc[-1])
        trend = "subiu" if last_val > first_val * 1.02 else ("caiu" if last_val < first_val * 0.98 else "estável")
        return f"Em **{title}**, {metric_col} {trend} de {first_val:.1f} para {last_val:.1f}."
    cat_cols = [c for c in df.columns if not pd.api.types.is_numeric_dtype(df[c])]
    x_col = cat_cols[0] if cat_cols else df.columns[0]
    y_col = numeric_cols[0]
    df_s = df.sort_values(y_col, ascending=False)
    top, bot = df_s.iloc[0], df_s.iloc[-1]
    return f"Em **{title}**, maior {y_col}: {top[x_col]} ({float(top[y_col]):.1f}); menor: {bot[x_col]} ({float(bot[y_col]):.1f})."


def explain_chart(title: str, df: pd.DataFrame, filters: dict | None) -> str:
    if df is None or df.empty:
        return f"**{title}** está vazio."
    if os.environ.get("OPENAI_API_KEY"):
        try:
            import openai
            client = openai.OpenAI(api_key=os.environ["OPENAI_API_KEY"])
            r = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": f"Explique em 2-4 frases em português o gráfico '{title}' com dados: {df.head(20).to_dict(orient='records')}. Seja objetivo."}],
                temperature=0.2,
            )
            return r.choices[0].message.content.strip()
        except Exception:
            pass
    return _heuristic_explanation(title, df, filters)


def decode_llm_result_to_message(question: str, sql: str, df: pd.DataFrame, max_rows: int = 10) -> str:
    """
    Decoder: transforma o resultado do banco gold (DataFrame) em mensagem formatada legível.
    Não precisa armazenar no R2; é só formatação da resposta para exibir na UI.
    """
    if df is None or df.empty:
        return f"**Resposta:** Nenhum registro encontrado para a pergunta."
    n = len(df)
    head = df.head(max_rows)
    parts = [f"**Resposta:** Encontrados **{n}** registro(s) no gold."]
    # Resumo das primeiras linhas em texto legível
    rows_text = []
    for _, row in head.iterrows():
        pairs = [f"{k}: {v}" for k, v in row.items()]
        rows_text.append(" · ".join(pairs))
    if rows_text:
        parts.append("\n\n*Principais:*\n" + "\n".join(f"- {t}" for t in rows_text))
    if n > max_rows:
        parts.append(f"\n*… e mais {n - max_rows} registro(s).*")
    return "\n".join(parts)


def _get_llm_client():
    """
    Cliente LLM (ordem de prioridade): OpenAI, DeepSeek, Ollama.
    Ollama (grátis, local): instale https://ollama.com e rode `ollama run llama3.2`.
    No Streamlit Cloud: salve em Secrets: DEEPSEEK_API_KEY ou OLLAMA_BASE_URL.
    """
    try:
        import openai
    except ImportError:
        return None, None
    secrets = _safe_secrets_dict()

    def _get(key: str, default: str = "") -> str:
        return (secrets.get(key) or os.environ.get(key) or default) or ""

    api_key = _get("OPENAI_API_KEY").strip()
    if api_key:
        return openai.OpenAI(api_key=api_key), "gpt-4o-mini"

    api_key = _get("DEEPSEEK_API_KEY").strip()
    if api_key:
        base_url = _get("DEEPSEEK_BASE_URL", "https://api.deepseek.com").strip()
        return openai.OpenAI(api_key=api_key, base_url=base_url or "https://api.deepseek.com"), "deepseek-chat"

    ollama_url = _get("OLLAMA_BASE_URL").strip() or os.environ.get("OLLAMA_BASE_URL", "").strip()
    if not ollama_url:
        ollama_url = "http://localhost:11434/v1"  # padrão local (instale Ollama e rode ollama run llama3.2)
    if ollama_url:
        model = _get("OLLAMA_MODEL").strip() or "llama3.2"
        return openai.OpenAI(api_key="ollama", base_url=ollama_url.rstrip("/")), model
    return None, None


def _detect_kpis_year_column(con, kpis_uri: str) -> str | None:
    """Usado só por query_kpis/overview. Retorna nome da coluna de ano ou None."""
    try:
        df = con.execute(f"SELECT * FROM read_parquet('{kpis_uri}', hive_partitioning=0) LIMIT 1").fetchdf()
        for cand in ["ano", "year", "Ano", "Year", "ANO", "year_id", "anio"]:
            if cand in df.columns:
                return cand if cand.replace("_", "").isalnum() and cand.islower() else f'"{cand}"'
    except Exception:
        pass
    return None


def tier_from_score(s: float, b_max: int, s_max: int) -> str:
    if s <= b_max:
        return "Bronze"
    if s <= s_max:
        return "Silver"
    return "Gold"


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    try:
        st.set_page_config(page_title="ENEM Radar — R2", layout="wide", initial_sidebar_state="collapsed")
    except Exception:
        pass
    inject_dark_theme_css()
    st.markdown('<p id="top"></p>', unsafe_allow_html=True)
    try:
        _main_body()
    except Exception as e:
        st.error("Erro ao carregar o app. Abra **Manage app → Logs** no Streamlit Cloud para ver o traceback completo.")
        st.code(str(e), language="text")
        with st.expander("Detalhes (traceback)"):
            import traceback
            st.text(traceback.format_exc())
        st.caption("Dica: KeyError com 'ano' ou colunas? O Parquet no R2 pode ter schema diferente. Rode `python scripts/build_and_upload_demo_r2.py` para subir dados de demo.")


def _main_body():
    secrets = _safe_secrets_dict()
    config = get_r2_config(secrets)
    if not config["access_key"] or not config["secret_key"]:
        st.error("R2 credentials missing. Set R2_ACCESS_KEY and R2_SECRET_KEY in Secrets.")
        st.stop()

    silver_uri = silver_s3_uri(config)
    gold_uri = gold_s3_uri(config)
    uris = {
        "gold_kpis": _gold_kpis_uri(config),
        "gold_cluster_profiles": _gold_cluster_profiles_uri(config),
        "gold_cluster_evolution": _gold_cluster_evolution_uri(config),
        "silver_quality": _silver_quality_report_uri(config),
        "silver_null": _silver_null_report_uri(config),
    }

    try:
        con = _cached_connection(secrets)
        st.session_state["_duckdb_con"] = con
    except Exception as e:
        st.error(f"Failed to create DuckDB connection: {e}")
        st.stop()

    ok, msg = run_self_test(con, silver_uri, gold_uri)
    if not ok:
        st.error(f"Connection self-test failed: {msg}")
        st.stop()

    if "_schema" not in st.session_state:
        with st.spinner("Discovering schema…"):
            st.session_state["_schema"] = discover_schema(con, silver_uri, gold_uri)
    schema = st.session_state["_schema"]

    has_kpis = _layer_available(con, uris["gold_kpis"])
    has_quality_report = _layer_available(con, uris["silver_quality"])
    has_null_report = _layer_available(con, uris["silver_null"])
    has_cluster_profiles = _layer_available(con, uris["gold_cluster_profiles"])
    has_cluster_evolution = _layer_available(con, uris["gold_cluster_evolution"])

    if "_kpis_year_col" not in st.session_state and has_kpis:
        st.session_state["_kpis_year_col"] = _detect_kpis_year_column(con, uris["gold_kpis"])
    kpis_year_col = st.session_state.get("_kpis_year_col")  # None = use fallback SQL (no year column)

    loaded = []
    if schema.get("silver_ok"):
        loaded.append("Silver")
    if schema.get("gold_ok"):
        loaded.append("Gold")

    st.title("ENEM Opportunity & Equity Radar")
    st.caption("2020–2024 | Parquet no Cloudflare R2 (DuckDB) — One-page executivo")

    if st.button("Refresh cache", key="refresh_cache"):
        query_kpis.clear()
        query_quality_report.clear()
        query_null_report.clear()
        query_cluster_profiles.clear()
        query_cluster_evolution.clear()
        if "_kpis_year_col" in st.session_state:
            del st.session_state["_kpis_year_col"]
        st.rerun()

    row1 = st.columns([3, 1])
    with row1[0]:
        toc_links([
            ("Visão geral", "sec-executive"),
            ("CRISP-DM", "sec-method"),
            ("Qualidade Silver", "sec-quality"),
            ("Overview Nacional", "sec-overview"),
            ("Radar Prioridade", "sec-radar"),
            ("Cohorts & Clusters", "sec-clusters"),
            ("Fidelidade", "sec-fidelity"),
            ("LLM Bot", "sec-llm"),
        ])
    with row1[1]:
        data_sources_badge(loaded, has_kpis)
    st.markdown("---")

    anos_disponiveis = [2020, 2021, 2022, 2023, 2024]
    col_y1, col_y2, col_uf = st.columns([2, 2, 2])
    with col_y1:
        year_min = st.slider("Ano mín.", min_value=min(anos_disponiveis), max_value=max(anos_disponiveis), value=2020, key="ymin")
    with col_y2:
        year_max = st.slider("Ano máx.", min_value=min(anos_disponiveis), max_value=max(anos_disponiveis), value=2024, key="ymax")
    if year_min > year_max:
        year_min, year_max = year_max, year_min
    ufs_list = ["Todos"]
    if has_kpis:
        try:
            _kpis_uri = uris["gold_kpis"]
            df_ufs = con.execute(f"SELECT DISTINCT sg_uf_residencia FROM read_parquet('{_kpis_uri}', hive_partitioning=0) WHERE sg_uf_residencia IS NOT NULL AND TRIM(sg_uf_residencia) != '' AND sg_uf_residencia != 'NA' LIMIT 50").fetchdf()
            ufs_list += sorted(df_ufs["sg_uf_residencia"].dropna().astype(str).tolist())
        except Exception:
            pass
    with col_uf:
        uf_filter = st.selectbox("UF", ufs_list, key="uf_global")
    st.markdown("---")

    con_key = id(con)
    uf_param = uf_filter if uf_filter != "Todos" else None
    df_kpis = query_kpis(con_key, uris["gold_kpis"], year_min, year_max, uf_param, kpis_year_col)
    DEMO_KPIS = {"media_objetiva": 515.5, "media_redacao": 634.7, "pct_presence": 100.0, "total_participantes": 12_839_968}
    # Normalizar df_kpis: garantir ano e sg_uf_residencia para evitar KeyError. Se falhar, usar df vazio (modo demo).
    try:
        if not df_kpis.empty:
            df_kpis = pd.DataFrame(df_kpis.copy())
            cols = list(df_kpis.columns)
            if "ano" not in cols:
                df_kpis["ano"] = year_max
            if "sg_uf_residencia" not in cols:
                df_kpis["sg_uf_residencia"] = "BR"
    except Exception:
        df_kpis = pd.DataFrame()
    use_demo = df_kpis.empty

    # ----- A) Visão geral -----
    section_header_anchor("A — Visão geral", "sec-executive", level=2)
    presence_warning = False
    try:
        if not df_kpis.empty:
            cols_list = list(df_kpis.columns)
            agg_cols = {"media_objetiva": "mean", "media_redacao": "mean", "count_participantes": "sum"}
            if "pct_presence_full" in cols_list:
                agg_cols["pct_presence_full"] = "mean"
            cols_agg = {k: v for k, v in agg_cols.items() if k in cols_list}
            if not cols_agg:
                agg = pd.DataFrame()
            elif "ano" in cols_list:
                try:
                    agg = df_kpis.groupby("ano").agg(cols_agg).reset_index()
                except KeyError:
                    agg = df_kpis.agg({k: v for k, v in cols_agg.items()}).to_frame().T
                    agg["ano"] = year_max
            else:
                agg = df_kpis.agg({k: v for k, v in cols_agg.items()}).to_frame().T
                agg["ano"] = year_max
            if not agg.empty:
                if "pct_presence_full" in agg.columns:
                    agg = agg.rename(columns={"pct_presence_full": "pct_presence"})
                elif "pct_presence" not in agg.columns:
                    agg["pct_presence"] = 100.0
                    presence_warning = True
                kpi_media_obj = f"{agg['media_objetiva'].iloc[-1]:.1f}" if "media_objetiva" in agg.columns else "—"
                kpi_media_red = f"{agg['media_redacao'].iloc[-1]:.1f}" if "media_redacao" in agg.columns else "—"
                kpi_pres = f"{agg['pct_presence'].iloc[-1]:.1f}%" if "pct_presence" in agg.columns else "—"
            else:
                kpi_media_obj = kpi_media_red = kpi_pres = "—"
            kpi_total = f"{int(df_kpis['count_participantes'].sum()):,}" if "count_participantes" in df_kpis.columns else "—"
        else:
            kpi_media_obj = f"{DEMO_KPIS['media_objetiva']}"
            kpi_media_red = f"{DEMO_KPIS['media_redacao']}"
            kpi_pres = f"{DEMO_KPIS['pct_presence']:.1f}%"
            kpi_total = f"{DEMO_KPIS['total_participantes']:,}"
    except (KeyError, Exception):
        use_demo = True
        kpi_media_obj = f"{DEMO_KPIS['media_objetiva']}"
        kpi_media_red = f"{DEMO_KPIS['media_redacao']}"
        kpi_pres = f"{DEMO_KPIS['pct_presence']:.1f}%"
        kpi_total = f"{DEMO_KPIS['total_participantes']:,}"
    st.markdown("- **Média objetiva (provas objetivas)** e **média redação** refletem o desempenho no período selecionado.")
    st.markdown("- **Presença** indica % de participantes que fizeram as 4 provas objetivas.")
    if presence_warning and has_kpis:
        st.caption("⚠️ Presença não disponível; exibido 100%.")
    if use_demo:
        st.caption("**Valores demonstrativos** (sem dados no R2 — use *Refresh cache* após subir dados ou rode `python scripts/build_and_upload_demo_r2.py`).")
    kpi_row([
        ("Média objetiva (último ano)", kpi_media_obj),
        ("Média redação (último ano)", kpi_media_red),
        ("Presença média (%)", kpi_pres),
        ("Total participantes", kpi_total),
    ])
    if use_demo or has_kpis:
        st.caption(f"**Data freshness:** Anos {year_min}–{year_max} | Tabelas carregadas: {len(loaded)} ({', '.join(loaded) or 'Gold + Silver'})")
    if st.button("Gerar resumo executivo", key="btn_story"):
        qr_df = query_quality_report(con_key, uris["silver_quality"])
        nr_df = query_null_report(con_key, uris["silver_null"])
        ev_df = query_cluster_evolution(con_key, uris["gold_cluster_evolution"], None)
        report_md, _ = build_story_report_r2(df_kpis, qr_df, nr_df, ev_df, year_min, year_max, uf_param)
        st.markdown(report_md)
        st.download_button("Baixar Markdown", data=report_md, file_name="auto_report.md", mime="text/markdown", key="dl_story")
    section_divider()

    # ----- B) CRISP-DM -----
    section_header_anchor("B — CRISP-DM + Lakehouse (método em 60s)", "sec-method", level=2)
    steps = ["Business Understanding", "Data Understanding", "Data Prep (Silver)", "Modeling (Gold)", "Evaluation", "Deployment"]
    st.markdown("**Fluxo:** " + " → ".join(steps))
    st.markdown("*\"Bronze guarda a verdade. Silver organiza. Gold transforma em decisão.\"*")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("**Bronze**"); st.caption("Ingestão raw, Parquet por ano.")
    with c2:
        st.markdown("**Silver**"); st.caption("Contrato, validação, quality_report, null_report.")
    with c3:
        st.markdown("**Gold**"); st.caption("KPIs, kpis_uf_ano, cluster_*.")
    section_divider()

    # ----- C) Qualidade Silver -----
    section_header_anchor("C — Qualidade dos dados (Silver)", "sec-quality", level=2)
    if not has_quality_report and not has_null_report:
        st.caption("Quality_report e null_report não encontrados no R2. Caminhos: silver/quality_report/**/*.parquet e silver/null_report/**/*.parquet")
    else:
        if has_quality_report:
            qr_df = query_quality_report(con_key, uris["silver_quality"])
            if not qr_df.empty and "rule_name" in qr_df.columns and "rows_removed" in qr_df.columns:
                rule_totals = qr_df.groupby("rule_name").agg(rows_removed=("rows_removed", "sum")).reset_index().sort_values("rows_removed", ascending=False).head(10)
                st.dataframe(rule_totals, use_container_width=True, hide_index=True)
                if "year" in qr_df.columns:
                    by_year = qr_df.groupby("year")["rows_removed"].sum().reset_index()
                    if HAS_PLOTLY and not by_year.empty:
                        st.plotly_chart(px.bar(by_year, x="year", y="rows_removed", labels={"year": "Ano", "rows_removed": "Linhas removidas"}), use_container_width=True)
            else:
                st.caption("Relatório de qualidade vazio.")
        if has_null_report:
            nr_df = query_null_report(con_key, uris["silver_null"])
            if not nr_df.empty and "column_name" in nr_df.columns and "pct_null" in nr_df.columns:
                top_null = nr_df.groupby("column_name")["pct_null"].max().reset_index().sort_values("pct_null", ascending=False).head(5)
                st.caption("**Top 5 colunas com maior % nulos:**")
                st.dataframe(top_null, use_container_width=True, hide_index=True)
    section_divider()

    # ----- D) Overview Nacional -----
    section_header_anchor("D — Overview Nacional (KPIs e tendências)", "sec-overview", level=2)
    if not has_kpis and not use_demo:
        not_generated_yet("gold.kpis_uf_ano não encontrado no R2 (gold/kpis_uf_ano/**/*.parquet).")
    elif df_kpis.empty and not use_demo:
        st.warning("Nenhum dado para os filtros.")
    else:
        if use_demo:
            agg = pd.DataFrame({
                "ano": [2020, 2021, 2022, 2023, 2024],
                "media_objetiva": [500.0, 505.0, 510.0, 512.5, DEMO_KPIS["media_objetiva"]],
                "media_redacao": [600.0, 615.0, 625.0, 630.0, DEMO_KPIS["media_redacao"]],
            })
            ufs_demo = ["SP", "MG", "RJ", "RS", "PR", "BA", "SC", "GO", "PE", "CE", "ES", "DF", "PA", "MT", "MS", "PB", "RN", "AL", "SE", "PI", "MA", "RO", "TO", "AC", "AP", "AM", "RR"]
            by_uf = pd.DataFrame({
                "sg_uf_residencia": ufs_demo,
                "media_objetiva": [520 + i * 2 for i in range(len(ufs_demo))],
                "count": [400000 - i * 10000 for i in range(len(ufs_demo))],
            })
            st.caption("**Gráficos demonstrativos** (dados manuais — suba dados no R2 para valores reais).")
        else:
            try:
                agg_cols = {"media_objetiva": "mean", "media_redacao": "mean", "count_participantes": "sum"}
                if "pct_presence_full" in df_kpis.columns:
                    agg_cols["pct_presence_full"] = "mean"
                cols_agg = {k: v for k, v in agg_cols.items() if k in df_kpis.columns}
                if "ano" in df_kpis.columns and cols_agg:
                    agg = df_kpis.groupby("ano").agg(cols_agg).reset_index()
                else:
                    agg = df_kpis.agg({k: v for k, v in cols_agg.items()}).to_frame().T if cols_agg else pd.DataFrame()
                    if not agg.empty and "ano" not in agg.columns:
                        agg["ano"] = year_max
                if not agg.empty and "pct_presence_full" in agg.columns:
                    agg = agg.rename(columns={"pct_presence_full": "pct_presence"})
                if "sg_uf_residencia" in df_kpis.columns:
                    by_uf = df_kpis.groupby("sg_uf_residencia").agg(media_objetiva=("media_objetiva", "mean"), count=("count_participantes", "sum")).reset_index()
                    by_uf = by_uf[by_uf["sg_uf_residencia"].notna() & (by_uf["sg_uf_residencia"].astype(str).str.strip() != "") & (by_uf["sg_uf_residencia"].astype(str) != "NA")]
                    by_uf = by_uf.sort_values("media_objetiva", ascending=False)
                else:
                    by_uf = pd.DataFrame()
            except (KeyError, Exception):
                agg = pd.DataFrame({"ano": [year_max], "media_objetiva": [DEMO_KPIS["media_objetiva"]], "media_redacao": [DEMO_KPIS["media_redacao"]]})
                by_uf = pd.DataFrame()
        if HAS_PLOTLY and not agg.empty:
            fig = go.Figure()
            x_vals = agg["ano"].tolist() if "ano" in agg.columns else list(range(len(agg)))
            if "media_objetiva" in agg.columns:
                fig.add_trace(go.Scatter(x=x_vals, y=agg["media_objetiva"], name="Média objetiva", mode="lines+markers"))
            if "media_redacao" in agg.columns:
                fig.add_trace(go.Scatter(x=x_vals, y=agg["media_redacao"], name="Média redação", mode="lines+markers"))
            fig.update_layout(xaxis_title="Ano", yaxis_title="Nota média", height=350)
            st.plotly_chart(fig, use_container_width=True)
        if HAS_PLOTLY and not by_uf.empty:
            st.plotly_chart(px.bar(by_uf.head(27), x="sg_uf_residencia", y="media_objetiva", labels={"sg_uf_residencia": "UF", "media_objetiva": "Média objetiva"}).update_layout(height=400, xaxis_tickangle=-45), use_container_width=True)
        if st.button("Explicar tendência", key="exp_trend"):
            cols = [c for c in ["ano", "media_objetiva", "media_redacao"] if c in agg.columns]
            st.info(explain_chart("Tendência média por ano", agg[cols] if cols else agg, {"year_start": year_min, "year_end": year_max, "uf": uf_filter}))
    section_divider()

    # ----- E) Radar de Prioridade -----
    section_header_anchor("E — Radar de Prioridade (Score 0–100)", "sec-radar", level=2)
    if not has_kpis:
        not_generated_yet("gold.kpis_uf_ano não encontrado.")
    else:
        with st.expander("Pesos da fórmula", expanded=False):
            c1, c2, c3, c4 = st.columns(4)
            with c1: w_queda = st.slider("Queda tendência", 0.0, 1.0, 0.30, 0.05, key="w_queda")
            with c2: w_presenca = st.slider("Presença baixa", 0.0, 1.0, 0.25, 0.05, key="w_presenca")
            with c3: w_gap = st.slider("Gap socioeconômico", 0.0, 1.0, 0.25, 0.05, key="w_gap")
            with c4: w_cluster = st.slider("Cluster risco", 0.0, 1.0, 0.20, 0.05, key="w_cluster")
        total_w = w_queda + w_presenca + w_gap + w_cluster
        if abs(total_w - 1.0) > 0.01:
            total_w = max(total_w, 0.01)
            w_queda, w_presenca, w_gap, w_cluster = w_queda / total_w, w_presenca / total_w, w_gap / total_w, w_cluster / total_w
        if not df_kpis.empty:
            df_r = df_kpis.copy()
            if "ano" not in df_r.columns:
                df_r["ano"] = year_max
            if "sg_uf_residencia" not in df_r.columns:
                df_r["sg_uf_residencia"] = "BR"
            df_r["ano"] = df_r["ano"].astype(int)
            anos_r = sorted(df_r["ano"].unique())
            if len(anos_r) >= 2 and "sg_uf_residencia" in df_r.columns:
                first_year, last_year = min(anos_r), max(anos_r)
                early = df_r[df_r["ano"] == first_year].set_index("sg_uf_residencia")
                late = df_r[df_r["ano"] == last_year].set_index("sg_uf_residencia")
                ufs_r = [u for u in (set(early.index) & set(late.index)) if u and str(u) != "NA"]
                max_media = df_r["media_objetiva"].max()
                scores = []
                ev_df_radar = query_cluster_evolution(con_key, uris["gold_cluster_evolution"], None) if has_cluster_evolution else pd.DataFrame()
                for uf in ufs_r:
                    m_early = early.loc[uf, "media_objetiva"] if uf in early.index else None
                    m_late = late.loc[uf, "media_objetiva"] if uf in late.index else None
                    p_early = early.loc[uf, "pct_presence_full"] if (uf in early.index and "pct_presence_full" in early.columns) else 100
                    p_late = late.loc[uf, "pct_presence_full"] if (uf in late.index and "pct_presence_full" in late.columns) else 100
                    queda = max(0, min(100, 100 * (1 - (m_late - m_early) / (max_media * 0.1 + 1e-6)) if m_early is not None and m_late is not None else 50))
                    presenca_baixa = max(0, min(100, 100 - (p_late if p_late is not None else 100)))
                    gap = max(0, min(100, 100 * (1 - (m_late or 0) / (max_media or 1)) if max_media else 50))
                    cluster_risk = 0
                    if has_cluster_evolution and not ev_df_radar.empty and "uf" in ev_df_radar.columns:
                        ev_uf = ev_df_radar[ev_df_radar["uf"] == uf]
                        if not ev_uf.empty and "ano" in ev_uf.columns and "pct_participants" in ev_uf.columns:
                            early_pct = ev_uf[ev_uf["ano"] == first_year]["pct_participants"].sum()
                            late_pct = ev_uf[ev_uf["ano"] == last_year]["pct_participants"].sum()
                            cluster_risk = min(100, max(0, 50 + (late_pct - early_pct) * 100))
                    score = max(0, min(100, w_queda * queda + w_presenca * presenca_baixa + w_gap * gap + w_cluster * cluster_risk))
                    scores.append({"uf": uf, "score": round(score, 1), "queda": round(queda, 1), "presenca_baixa": round(presenca_baixa, 1), "gap": round(gap, 1), "cluster_risk": cluster_risk})
                score_df = pd.DataFrame(scores).sort_values("score", ascending=False)
                st.dataframe(score_df.head(10), use_container_width=True, hide_index=True)
                if HAS_PLOTLY:
                    st.plotly_chart(px.bar(score_df.head(15), x="uf", y="score", labels={"uf": "UF", "score": "Score (0–100)"}).update_layout(height=400), use_container_width=True)
                if st.button("Explicar este gráfico", key="exp_radar"):
                    st.info(explain_chart("Radar de Prioridade por UF", score_df[["uf", "score"]], {"year_start": first_year, "year_end": last_year}))
            else:
                st.warning("Precisamos de pelo menos 2 anos para o score.")
    section_divider()

    # ----- F) Cohorts & Clusters -----
    section_header_anchor("F — Cohorts & Clusters (Mapa de Perfis)", "sec-clusters", level=2)
    if not has_cluster_profiles and not has_cluster_evolution:
        not_generated_yet("cluster_profiles e cluster_evolution não encontrados no R2.")
    else:
        if has_cluster_profiles:
            profiles = query_cluster_profiles(con_key, uris["gold_cluster_profiles"])
            if not profiles.empty:
                st.dataframe(profiles, use_container_width=True, hide_index=True)
                if HAS_PLOTLY and "size" in profiles.columns:
                    st.plotly_chart(px.pie(profiles, values="size", names="cluster_id", title="Distribuição por cluster"), use_container_width=True)
        if has_cluster_evolution:
            ev_df = query_cluster_evolution(con_key, uris["gold_cluster_evolution"], None)
            ufs_ev = ["Todos"] + (sorted(ev_df["uf"].dropna().unique().tolist()) if "uf" in ev_df.columns else [])
            uf_sel_ev = st.selectbox("Evolução por UF", ufs_ev if ufs_ev else ["Todos"], key="cluster_uf")
            ev_df_f = ev_df if uf_sel_ev == "Todos" else (ev_df[ev_df["uf"] == uf_sel_ev] if "uf" in ev_df.columns else ev_df)
            if not ev_df_f.empty and "ano" in ev_df_f.columns and "cluster_id" in ev_df_f.columns and "pct_participants" in ev_df_f.columns:
                ev_df_f = ev_df_f.copy()
                ev_df_f["ano"] = ev_df_f["ano"].astype(int)
                ev_wide = ev_df_f.pivot_table(index="ano", columns="cluster_id", values="pct_participants", aggfunc="first").reset_index()
                if HAS_PLOTLY:
                    fig_ev = go.Figure()
                    for c in ev_wide.columns:
                        if c == "ano":
                            continue
                        fig_ev.add_trace(go.Scatter(x=ev_wide["ano"], y=ev_wide[c], name=f"Cluster {c}", mode="lines+markers"))
                    fig_ev.update_layout(xaxis_title="Ano", yaxis_title="% participantes", height=400)
                    st.plotly_chart(fig_ev, use_container_width=True)
    section_divider()

    # ----- G) Plano de Fidelidade -----
    section_header_anchor("G — Plano de Fidelidade (Feature Produto)", "sec-fidelity", level=2)
    col_t1, col_t2, col_t3 = st.columns(3)
    with col_t1:
        bronze_max = st.number_input("Bronze (máx. score)", 0, 100, 39, key="bronze")
    with col_t2:
        silver_max = st.number_input("Silver (máx. score)", 0, 100, 69, key="silver")
    with col_t3:
        st.caption("Gold: score > Silver máx.")
    if has_kpis and not df_kpis.empty:
        try:
            df_t = df_kpis.copy()
            df_t["ano"] = df_t["ano"].astype(int)
            anos_t = sorted(df_t["ano"].unique())
            if len(anos_t) >= 2 and "sg_uf_residencia" in df_t.columns:
                first_year, last_year = min(anos_t), max(anos_t)
                early = df_t[df_t["ano"] == first_year].set_index("sg_uf_residencia")
                late = df_t[df_t["ano"] == last_year].set_index("sg_uf_residencia")
                ufs_t = [u for u in set(early.index) & set(late.index) if u and str(u) != "NA"]
                max_media = df_t["media_objetiva"].max()
                tier_list = []
                for uf in ufs_t:
                    m_early = early.loc[uf, "media_objetiva"] if uf in early.index else None
                    m_late = late.loc[uf, "media_objetiva"] if uf in late.index else None
                    queda = 100 * (1 - (m_late - m_early) / (max_media * 0.1 + 1e-6)) if m_early is not None and m_late is not None else 50
                    score = max(0, min(100, 0.3 * max(0, min(100, queda)) + 0.25 * (100 * (1 - (m_late or 0) / (max_media or 1)))))
                    tier_list.append({"uf": uf, "score": round(score, 1), "tier": tier_from_score(score, bronze_max, silver_max)})
                tier_df = pd.DataFrame(tier_list).sort_values("score", ascending=False)
                if not tier_df.empty:
                    uf_sel_t = st.selectbox("Ver tier por UF", ["—"] + tier_df["uf"].tolist(), key="tier_uf")
                    if uf_sel_t != "—":
                        sel = tier_df[tier_df["uf"] == uf_sel_t]
                        if not sel.empty:
                            row = sel.iloc[0]
                            st.metric("UF", uf_sel_t); st.metric("Score Radar", row["score"]); st.metric("Tier", row["tier"])
        except (KeyError, Exception):
            pass
    st.markdown("**Benefícios por tier:**")
    c1, c2, c3 = st.columns(3)
    with c1: tier_card_dark("Bronze", "Bronze", ["Relatórios agregados.", "Score Radar.", "Dashboards públicos."])
    with c2: tier_card_dark("Silver", "Silver", ["Quality e null report.", "Segmentação por perfil/ano."])
    with c3: tier_card_dark("Gold", "Gold", ["Gold completo.", "LLM Analyst.", "Mapa de Perfis."])
    section_divider()

    # ----- H) LLM Bot + Instant SQL -----
    section_header_anchor("H — LLM Analyst Bot (Grounded)", "sec-llm", level=2)
    question = st.text_input("Pergunta (LLM)", placeholder="Ex.: Quais UFs têm maior média objetiva?", key="llm_q")
    if st.button("Executar LLM") and question.strip():
        client, model = _get_llm_client()
        if not client:
            st.info("LLM desativado; configure OPENAI_API_KEY, DEEPSEEK_API_KEY ou OLLAMA_BASE_URL (Ollama grátis).")
        else:
            try:
                prompt = f"Gere apenas um SELECT em DuckDB. Use APENAS: read_parquet('{uris['gold_kpis']}', hive_partitioning=0). Colunas permitidas (NÃO use 'ano'): sg_uf_residencia, media_objetiva, count_participantes. Pergunta: {question}. Retorne só o SQL, sem explicação."
                r = client.chat.completions.create(model=model, messages=[{"role": "user", "content": prompt}], temperature=0)
                sql = (r.choices[0].message.content or "").strip()
                sql = re.sub(r"```\w*\n?", "", sql).strip()
                if not sql.upper().startswith("SELECT"):
                    st.warning("Resposta do LLM não é um SELECT.")
                else:
                    df_res = con.execute(sql).fetchdf()
                    st.code(sql, language="sql")
                    # Decoder: resultado do gold em mensagem formatada (não precisa armazenar no R2)
                    msg = decode_llm_result_to_message(question, sql, df_res, max_rows=10)
                    st.markdown(msg)
                    st.dataframe(df_res.head(20), use_container_width=True, hide_index=True)
                    if "llm_log" not in st.session_state:
                        st.session_state["llm_log"] = []
                    st.session_state["llm_log"].append({"question": question, "sql": sql, "rows": len(df_res)})
            except Exception as e:
                st.error(f"Erro: {e}")

    section_divider()
    st.markdown('[↑ Voltar ao topo](#top)')
    st.caption("Dados: s3://" + config["bucket"] + "/silver · gold (R2)")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        st.set_page_config(page_title="ENEM Radar — Erro", layout="wide")
        st.error("Erro ao iniciar o app.")
        st.code(str(e), language="text")
        with st.expander("Traceback completo"):
            st.text(traceback.format_exc())
