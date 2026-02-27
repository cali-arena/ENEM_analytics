"""
ENEM Opportunity & Equity Radar (2020–2024) — Single-page executive dashboard.

One-page storytelling: CRISP-DM + Lakehouse narrative, same data/graphs as app.py,
no sidebar. Run: streamlit run app/app_one_page.py  (from repo root)
"""
import sys
import os
import time
import datetime

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from app.paths import ROOT, DATA_DIR, RAW_DIR, GOLD_DIR, SILVER_DIR, REPORTS_DIR, LOGS_DIR
from app.db import get_connection, table_exists, list_loaded_tables
from app.components import (
    section_header_anchor,
    section_divider,
    data_sources_badge,
    toc_links,
    not_generated_yet,
    tier_card_dark,
    kpi_row,
    inject_dark_theme_css,
)
from app.instant_intents import INTENT_CATALOG, build_intent_sql


def _gold_has_data() -> bool:
    p = GOLD_DIR / "kpis_uf_ano"
    if not p.exists():
        return False
    return any(p.rglob("*.parquet")) if p.is_dir() else True


def _raw_has_data() -> bool:
    raw = RAW_DIR
    return raw.exists() and bool(list(raw.glob("*.csv")))


def _clusters_has_data() -> bool:
    p1 = GOLD_DIR / "cluster_profiles.parquet"
    p2 = GOLD_DIR / "cluster_evolution_uf_ano.parquet"
    return p1.exists() and p2.exists()


# ----- Page config (no sidebar) -----
st.set_page_config(
    page_title="ENEM Radar — Executive",
    layout="wide",
    initial_sidebar_state="collapsed",
)
inject_dark_theme_css()

# ----- Data readiness / Cloud bootstrap -----
bootstrap_message = None
try:
    # Tentativa de garantir demo Gold em ambientes imutáveis (Cloud).
    from scripts.bootstrap_cloud import ensure_demo_gold

    ok_boot, bootstrap_message = ensure_demo_gold()
    if not ok_boot and not _gold_has_data():
        st.warning(
            "Não foi possível preparar dados Gold automaticamente. "
            "Se estiver rodando localmente, execute os pipelines Bronze→Silver→Gold "
            "ou forneça DEMO_GOLD_URL para o demo_gold.zip."
        )
except Exception as _e:
    # Bootstrap é best-effort – nunca deve quebrar o app.
    bootstrap_message = None

if not _gold_has_data():
    if _raw_has_data():
        with st.spinner("Gerando dados pela primeira vez (pode levar alguns minutos)..."):
            try:
                from scripts.run_lakehouse_duckdb import main as run_pipeline

                run_pipeline()
            except Exception as e:
                st.error(f"Erro ao gerar dados: {e}")
                st.stop()
        st.rerun()
    else:
        st.warning(
            "Nenhum CSV em data/raw e nenhum demo Gold disponível. "
            "Coloque ENEM_2020.csv a ENEM_2024.csv em data/raw ou configure DEMO_GOLD_URL "
            "para usar o conjunto demo."
        )
        st.stop()

if _gold_has_data() and not _clusters_has_data():
    with st.spinner("Gerando clusters (Cohorts & Clusters)..."):
        try:
            from scripts.run_clusters_duckdb import main as run_clusters

            run_clusters()
        except Exception as e:
            st.warning(f"Clusters não gerados: {e}")
        else:
            st.rerun()


@st.cache_resource
def get_db():
    return get_connection()


con = get_db()
loaded = list_loaded_tables(con)
has_kpis = table_exists(con, "gold.kpis_uf_ano")
has_cluster_profiles = table_exists(con, "gold.cluster_profiles")
has_cluster_evolution = table_exists(con, "gold.cluster_evolution_uf_ano")
has_quality_report = table_exists(con, "silver.quality_report")
has_null_report = table_exists(con, "silver.null_report")

if _clusters_has_data() and not has_cluster_profiles:
    get_db.clear()
    st.rerun()

# ----- Cached KPIs dataframe (keyed by filters) -----
@st.cache_data(ttl=300)
def get_kpis_filtered(_con, year_min: int, year_max: int, uf: str):
    if not table_exists(_con, "gold.kpis_uf_ano"):
        return pd.DataFrame()
    df = _con.execute("SELECT * FROM gold.kpis_uf_ano").fetchdf()
    df["ano"] = df["ano"].astype(int)
    df = df[df["ano"].between(year_min, year_max)]
    if uf and uf != "Todos":
        df = df[df["sg_uf_residencia"] == uf]
    return df


@st.cache_data(ttl=300)
def cached_run_intent(intent_id: str, params_items: tuple):
    """
    Cached execution for Instant SQL Engine.

    params_items is a hashable representation of the params dict
    (e.g. tuple(sorted(params.items()))).
    """
    params = dict(params_items)
    sql = build_intent_sql(intent_id, params)
    con_local = get_db()
    start = time.perf_counter()
    df = con_local.execute(sql).fetchdf()
    duration = time.perf_counter() - start
    return df, sql, duration


def _heuristic_explanation(title: str, df: pd.DataFrame, filters: dict | None) -> str:
    """Fallback explanation without LLM: simples, mas sempre ancorado em números."""
    if df is None or df.empty:
        return f"O gráfico **{title}** está vazio para os filtros atuais."

    filters = filters or {}
    context_parts = []
    ys, ye = filters.get("year_start"), filters.get("year_end")
    uf = filters.get("uf")
    if ys is not None and ye is not None:
        context_parts.append(f"entre {ys} e {ye}")
    if uf and uf != "Todos":
        context_parts.append(f"para a UF {uf}")
    context = ""
    if context_parts:
        context = " " + ", ".join(context_parts)

    # Escolhe coluna numérica principal
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    if not numeric_cols:
        return f"O gráfico **{title}**{context} mostra apenas categorias sem métricas numéricas suficientes para uma explicação automática."

    # Se houver ano, tratamos como série temporal
    if "ano" in df.columns:
        metric_col = next((c for c in numeric_cols if c != "ano"), numeric_cols[0])
        df_sorted = df.sort_values("ano")
        first_year = int(df_sorted["ano"].iloc[0])
        last_year = int(df_sorted["ano"].iloc[-1])
        first_val = float(df_sorted[metric_col].iloc[0])
        last_val = float(df_sorted[metric_col].iloc[-1])
        if last_val > first_val * 1.02:
            trend_txt = "subiu"
        elif last_val < first_val * 0.98:
            trend_txt = "caiu"
        else:
            trend_txt = "se manteve relativamente estável"

        max_idx = df_sorted[metric_col].idxmax()
        min_idx = df_sorted[metric_col].idxmin()
        max_row = df_sorted.loc[max_idx]
        min_row = df_sorted.loc[min_idx]
        max_year = int(max_row.get("ano", first_year))
        min_year = int(min_row.get("ano", first_year))
        max_val = float(max_row[metric_col])
        min_val = float(min_row[metric_col])

        sentences = [
            f"No gráfico **{title}**{context}, a métrica `{metric_col}` {trend_txt} de {first_val:.1f} em {first_year} para {last_val:.1f} em {last_year}.",
            f"O maior valor aparece em {max_year} ({max_val:.1f}) e o menor em {min_year} ({min_val:.1f}).",
            "Isso indica uma tendência geral que pode orientar o foco de políticas nos anos com pior desempenho.",
        ]
        return " ".join(sentences)

    # Caso categórico (ex.: por UF)
    # Assumimos primeira coluna como categoria e primeira numérica como métrica
    cat_cols = [c for c in df.columns if not pd.api.types.is_numeric_dtype(df[c])]
    x_col = cat_cols[0] if cat_cols else df.columns[0]
    y_col = numeric_cols[0]
    df_sorted = df.sort_values(y_col, ascending=False)
    top = df_sorted.iloc[0]
    bottom = df_sorted.iloc[-1]
    sentences = [
        f"No gráfico **{title}**{context}, a categoria com maior `{y_col}` é `{top[x_col]}` ({float(top[y_col]):.1f}).",
        f"A categoria com menor valor é `{bottom[x_col]}` ({float(bottom[y_col]):.1f}).",
        "A diferença entre o topo e a base sugere onde concentrar esforços para reduzir desigualdades ou consolidar bons resultados.",
    ]
    return " ".join(sentences)


def explain_chart(title: str, df: pd.DataFrame, filters: dict | None) -> str:
    """
    Gera explicação em 2–4 frases sobre um gráfico, usando LLM se disponível
    e caindo para uma heurística quando não houver chave ou em caso de erro.
    """
    if df is None or df.empty:
        return f"O gráfico **{title}** está vazio para os filtros atuais."

    # Tentativa com LLM (opcional)
    api_key = os.environ.get("OPENAI_API_KEY")
    if api_key:
        try:
            import openai

            client = openai.OpenAI(api_key=api_key)
            df_small = df.head(20).to_dict(orient="records")
            prompt = (
                "Você é um analista de dados explicando um gráfico para gestores de educação. "
                "Explique em 2 a 4 frases, em português simples, cobrindo:\n"
                "1) Tendência geral (subida/queda/estabilidade)\n"
                "2) Algum outlier ou destaque\n"
                "3) Uma possível implicação para decisão (sem sugerir ações extremas).\n\n"
                f"Título do gráfico: {title}\n"
                f"Filtros atuais: {filters}\n"
                f"Dados agregados (até 20 linhas): {df_small}\n"
            )
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "Responda sempre com 2 a 4 frases em português, sem código, ancorado apenas nos dados fornecidos.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
            )
            text = response.choices[0].message.content.strip()
            return text
        except Exception:
            # Cai para heurística
            pass

    return _heuristic_explanation(title, df, filters)


def log_query(question: str, intent: str, sql: str, rows_returned: int, duration_sec: float) -> None:
    """Append basic query log for observability."""
    try:
        logs_dir = LOGS_DIR
        logs_dir.mkdir(parents=True, exist_ok=True)
        path = logs_dir / "llm_queries.log"
        ts = datetime.datetime.utcnow().isoformat()
        line = (
            f"{ts}\tquestion={question}\tintent={intent}\t"
            f"duration_sec={duration_sec:.3f}\trows={rows_returned}\tsql={sql.replace(chr(10), ' ')}\n"
        )
        with path.open("a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        # Logging must never quebrar o app
        pass


@st.cache_data(ttl=900)
def build_story_report(year_start: int, year_end: int, uf: str | None) -> tuple[str, dict]:
    """
    Gera um mini-relatório executivo (Markdown) + context_pack para os filtros atuais.
    Usa LLM se disponível, com fallback determinístico.
    """
    con_local = get_db()
    df_k = get_kpis_filtered(con_local, year_start, year_end, uf if uf and uf != "Todos" else None)
    context_pack: dict = {"kpis": {}, "top_ufs": [], "bottom_ufs": [], "largest_variation": None, "cluster_growth": None, "silver_quality": {}}

    if not df_k.empty:
        agg_cols = {"media_objetiva": "mean", "media_redacao": "mean", "count_participantes": "sum"}
        if "pct_presence_full" in df_k.columns:
            agg_cols["pct_presence_full"] = "mean"
        agg_year = df_k.groupby("ano").agg(agg_cols).reset_index()
        if "pct_presence_full" in agg_year.columns:
            agg_year = agg_year.rename(columns={"pct_presence_full": "pct_presence"})
        else:
            agg_year["pct_presence"] = 100.0

        last_row = agg_year.sort_values("ano").iloc[-1]
        context_pack["kpis"] = {
            "year_last": int(last_row["ano"]),
            "media_objetiva_last": float(last_row["media_objetiva"]),
            "media_redacao_last": float(last_row["media_redacao"]),
            "pct_presenca_last": float(last_row["pct_presence"]),
            "year_start": year_start,
            "year_end": year_end,
            "uf": uf,
        }

        # Top/bottom UFs por média objetiva
        by_uf = (
            df_k.groupby("sg_uf_residencia")
            .agg(media_objetiva=("media_objetiva", "mean"), participantes=("count_participantes", "sum"))
            .reset_index()
        )
        by_uf = by_uf[by_uf["sg_uf_residencia"].notna() & (by_uf["sg_uf_residencia"].astype(str) != "NA")]
        by_uf = by_uf.sort_values("media_objetiva", ascending=False)
        context_pack["top_ufs"] = (
            by_uf.head(5)
            .assign(media_objetiva=lambda d: d["media_objetiva"].round(1))
            .to_dict(orient="records")
        )
        context_pack["bottom_ufs"] = (
            by_uf.tail(5)
            .assign(media_objetiva=lambda d: d["media_objetiva"].round(1))
            .to_dict(orient="records")
        )

        # Maior variação ano-a-ano
        if len(agg_year) >= 2:
            agg_year = agg_year.sort_values("ano")
            diffs = []
            for i in range(1, len(agg_year)):
                prev, curr = agg_year.iloc[i - 1], agg_year.iloc[i]
                diffs.append(
                    {
                        "from_year": int(prev["ano"]),
                        "to_year": int(curr["ano"]),
                        "delta_media_objetiva": float(curr["media_objetiva"] - prev["media_objetiva"]),
                    }
                )
            if diffs:
                biggest = max(diffs, key=lambda d: abs(d["delta_media_objetiva"]))
                context_pack["largest_variation"] = biggest

    # Cluster growth (se disponível)
    try:
        from app.db import table_exists as _te

        if _te(con_local, "gold.cluster_evolution_uf_ano"):
            ev = con_local.execute(
                "SELECT ano, uf, cluster_id, pct_participants FROM gold.cluster_evolution_uf_ano"
            ).fetchdf()
            if not ev.empty:
                ev2020 = ev[ev["ano"] == 2020]
                ev2024 = ev[ev["ano"] == 2024]
                merged = (
                    ev2020.merge(
                        ev2024,
                        on=["uf", "cluster_id"],
                        suffixes=("_2020", "_2024"),
                        how="inner",
                    )
                )
                if not merged.empty:
                    merged["delta"] = merged["pct_participants_2024"] - merged["pct_participants_2020"]
                    top_growth = merged.sort_values("delta", ascending=False).head(1).iloc[0]
                    context_pack["cluster_growth"] = {
                        "uf": top_growth["uf"],
                        "cluster_id": int(top_growth["cluster_id"]),
                        "delta_pct": float(top_growth["delta"] * 100.0),
                    }
    except Exception:
        pass

    # Silver quality
    try:
        from app.db import table_exists as _te2

        silver_info = {}
        if _te2(con_local, "silver.quality_report"):
            qr = con_local.execute("SELECT * FROM silver.quality_report").fetchdf()
            if not qr.empty:
                total_removed = int(qr["rows_removed"].sum())
                by_rule = (
                    qr.groupby("rule_name")["rows_removed"]
                    .sum()
                    .reset_index()
                    .sort_values("rows_removed", ascending=False)
                )
                top_rule = by_rule.iloc[0].to_dict()
                silver_info["total_rows_removed"] = total_removed
                silver_info["top_rule"] = top_rule
        if _te2(con_local, "silver.null_report"):
            nr = con_local.execute("SELECT * FROM silver.null_report").fetchdf()
            if not nr.empty:
                by_col = (
                    nr.groupby("column_name")["pct_null"]
                    .max()
                    .reset_index()
                    .sort_values("pct_null", ascending=False)
                )
                silver_info["top_null_column"] = by_col.iloc[0].to_dict()
        context_pack["silver_quality"] = silver_info
    except Exception:
        pass

    # ---------- Tenta LLM ----------
    text_md = None
    api_key = os.environ.get("OPENAI_API_KEY")
    if api_key:
        try:
            import openai
            import json

            client = openai.OpenAI(api_key=api_key)
            prompt = (
                "Gere um mini-relatório executivo em português, em Markdown, com base apenas no JSON a seguir.\n"
                "Regras:\n"
                "- 3 a 5 parágrafos curtos (máx 3 frases cada).\n"
                "- Depois, uma lista com 3 ações recomendadas (bullet list).\n"
                "- Cite sempre valores numéricos concretos (anos, médias, percentuais) do contexto.\n"
                "- Não invente tabelas ou colunas que não estejam no JSON.\n"
                "- Público: secretários de educação e times técnicos.\n\n"
                f"JSON:\n{json.dumps(context_pack, ensure_ascii=False)}"
            )
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "Você é um analista de dados educacionais gerando resumos executivos claros e objetivos.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
            )
            text_md = response.choices[0].message.content.strip()
        except Exception:
            text_md = None

    # ---------- Fallback determinístico ----------
    if not text_md:
        k = context_pack.get("kpis") or {}
        sq = context_pack.get("silver_quality") or {}
        cg = context_pack.get("cluster_growth") or {}
        yr = k.get("year_last")
        uf_txt = f" na UF {k.get('uf')}" if k.get("uf") and k.get("uf") != "Todos" else ""
        p1 = (
            f"Entre {k.get('year_start', year_start)} e {k.get('year_end', year_end)}, "
            f"a média objetiva no último ano disponível ({yr}) foi de {k.get('media_objetiva_last', 0):.1f}{uf_txt}, "
            f"com média de redação de {k.get('media_redacao_last', 0):.1f}."
            if k
            else "Os indicadores principais ainda não estão disponíveis para estes filtros."
        )
        p2 = ""
        top_ufs = context_pack.get("top_ufs") or []
        bottom_ufs = context_pack.get("bottom_ufs") or []
        if top_ufs:
            best = top_ufs[0]
            worst = bottom_ufs[0] if bottom_ufs else None
            p2 = (
                f"As UFs com melhor desempenho médio objetivo incluem {best['sg_uf_residencia']} "
                f"({best['media_objetiva']:.1f} pontos), enquanto "
                f"{worst['sg_uf_residencia']} aparece entre as menores médias "
                f"({worst['media_objetiva']:.1f})."
                if worst
                else ""
            )
        p3 = ""
        if sq:
            tr = sq.get("top_rule") or {}
            tn = sq.get("top_null_column") or {}
            p3 = (
                f"No Silver, as regras de qualidade removeram {sq.get('total_rows_removed', 0)} linhas no total; "
                f"a regra '{tr.get('rule_name', '')}' foi a que mais filtrou casos. "
                f"A coluna '{tn.get('column_name', '')}' apresenta a maior taxa de nulos, "
                f"com cerca de {tn.get('pct_null', 0):.1f}%."
            )
        p4 = ""
        if cg:
            p4 = (
                f"Nos clusters de perfis, o cluster {cg.get('cluster_id')} cresceu aproximadamente "
                f"{cg.get('delta_pct', 0):.1f} p.p. na UF {cg.get('uf')}, sinalizando mudança relevante de perfil."
            )

        bullets = [
            "- Priorizar ações pedagógicas e de preparação nas UFs com menor média objetiva e redação, "
            "aproveitando os dados de tendência para definir metas anuais.",
            "- Usar o relatório Silver para revisar campos com alta taxa de nulos ou regras que removem muitas linhas, "
            "refinando formulários e processos de coleta.",
            "- Explorar os clusters que mais cresceram para desenhar intervenções específicas por perfil (sexo, renda, território).",
        ]

        parts = ["# Resumo executivo\n", p1, "\n\n", p2, "\n\n", p3, "\n\n", p4, "\n\n", "## Ações recomendadas\n", "\n".join(bullets)]
        text_md = "".join(parts)

    return text_md, context_pack


# =============================================================================
# TOP HEADER: Title, subtitle, badge, TOC, global filters, methodology toggle
# =============================================================================
st.markdown('<p id="top"></p>', unsafe_allow_html=True)
header = st.container()
with header:
    st.markdown("# ENEM Opportunity & Equity Radar")
    st.caption("2020–2024 | Gold + Silver — Relatório executivo em uma página")
    row1 = st.columns([3, 1])
    with row1[0]:
        toc_links([
            ("Visão geral", "sec-executive"),
            ("Método CRISP-DM", "sec-method"),
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
    col_y1, col_y2, col_uf, col_method = st.columns([2, 2, 2, 1])
    anos_disponiveis = [2020, 2021, 2022, 2023, 2024]
    with col_y1:
        year_min = st.slider("Ano mín.", min_value=min(anos_disponiveis), max_value=max(anos_disponiveis), value=2020, key="ymin")
    with col_y2:
        year_max = st.slider("Ano máx.", min_value=min(anos_disponiveis), max_value=max(anos_disponiveis), value=2024, key="ymax")
    if year_min > year_max:
        year_min, year_max = year_max, year_min
    ufs_list = ["Todos"]
    if has_kpis:
        try:
            df_ufs = con.execute("SELECT DISTINCT sg_uf_residencia FROM gold.kpis_uf_ano WHERE sg_uf_residencia IS NOT NULL AND TRIM(sg_uf_residencia) != '' AND sg_uf_residencia != 'NA'").fetchdf()
            ufs_list += sorted(df_ufs["sg_uf_residencia"].dropna().astype(str).tolist())
        except Exception:
            pass
    with col_uf:
        uf_filter = st.selectbox("UF", ufs_list, key="uf_global")
    with col_method:
        show_methodology = st.checkbox("Mostrar método (CRISP-DM)", value=True, key="show_method")
    st.markdown("---")

# =============================================================================
# SECTION A — Executive Summary (Visão Geral)
# =============================================================================
section_header_anchor("A — Visão geral", "sec-executive", level=2)
df_kpis = get_kpis_filtered(con, year_min, year_max, uf_filter if uf_filter != "Todos" else None)
presence_warning = False
if not df_kpis.empty:
    agg_cols = {"media_objetiva": "mean", "media_redacao": "mean", "count_participantes": "sum"}
    if "pct_presence_full" in df_kpis.columns:
        agg_cols["pct_presence_full"] = "mean"
    agg = df_kpis.groupby("ano").agg(agg_cols).reset_index()
    if "pct_presence_full" in agg.columns:
        agg = agg.rename(columns={"pct_presence_full": "pct_presence"})
    else:
        agg["pct_presence"] = 100.0
        presence_warning = True
    kpi_media_obj = f"{agg['media_objetiva'].iloc[-1]:.1f}" if len(agg) else "—"
    kpi_media_red = f"{agg['media_redacao'].iloc[-1]:.1f}" if len(agg) else "—"
    kpi_pres = f"{agg['pct_presence'].iloc[-1]:.1f}%" if len(agg) else "—"
else:
    kpi_media_obj = kpi_media_red = kpi_pres = "—"

st.markdown("- **Média objetiva (provas objetivas)** e **média redação** refletem o desempenho no período selecionado.")
st.markdown("- **Presença** indica % de participantes que fizeram as 4 provas objetivas.")
if presence_warning and has_kpis:
    st.caption("⚠️ Presença não disponível nos dados atuais; valor exibido é 100%.")
kpi_row([
    ("Média objetiva (último ano)", kpi_media_obj),
    ("Média redação (último ano)", kpi_media_red),
    ("Presença média (%)", kpi_pres),
])
anos_loaded = list(loaded) if loaded else []
st.caption(f"**Data freshness:** Anos {year_min}–{year_max} | Tabelas carregadas: {len(loaded)} ({', '.join(anos_loaded[:5])}{'…' if len(anos_loaded) > 5 else ''})")

if st.button("Gerar resumo executivo", key="btn_story_exec"):
    report_md, context_pack = build_story_report(
        year_min,
        year_max,
        uf_filter if uf_filter != "Todos" else None,
    )
    st.markdown(report_md)
    # Exportar para arquivo
    reports_dir = REPORTS_DIR
    reports_dir.mkdir(parents=True, exist_ok=True)
    report_path = reports_dir / "auto_report.md"
    try:
        report_path.write_text(report_md, encoding="utf-8")
    except Exception:
        pass
    st.download_button(
        "Baixar Markdown",
        data=report_md,
        file_name="auto_report.md",
        mime="text/markdown",
        key="download_story_exec",
    )

section_divider()

# =============================================================================
# SECTION B — CRISP-DM + Lakehouse (Método em 60 segundos)
# =============================================================================
if show_methodology:
    section_header_anchor("B — CRISP-DM + Lakehouse (método em 60 segundos)", "sec-method", level=2)
    steps = [
        "Business Understanding",
        "Data Understanding",
        "Data Prep (Contract + Silver)",
        "Modeling (Gold)",
        "Evaluation",
        "Deployment",
    ]
    st.markdown("**Fluxo:** " + " → ".join(steps))
    st.markdown("*\"Bronze guarda a verdade. Silver organiza a verdade. Gold transforma a verdade em decisão.\"*")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("**Bronze**")
        st.caption("Propósito: ingestão raw, metadados.")
        st.caption("Artefatos: Parquet por ano (ex.: enem_2020.parquet).")
        st.caption("Métrica: linhas por ano (raw).")
    with c2:
        st.markdown("**Silver**")
        st.caption("Propósito: contrato canônico, validação, qualidade.")
        st.caption("Artefatos: enem_participante, quality_report, null_report.")
        st.caption("Métrica: linhas válidas após regras.")
    with c3:
        st.markdown("**Gold**")
        st.caption("Propósito: decisão, star schema, KPIs, clusters.")
        st.caption("Artefatos: fato_desempenho, dim_*, kpis_uf_ano, cluster_*.")
        st.caption(f"Métrica: {len([t for t in loaded if t.startswith('gold.')])} tabelas Gold carregadas.")
    section_divider()

# =============================================================================
# SECTION C — Data Quality (Silver Transparência)
# =============================================================================
section_header_anchor("C — Qualidade dos dados (Silver)", "sec-quality", level=2)
if has_quality_report:
    try:
        qr = con.execute("SELECT * FROM silver.quality_report").fetchdf()
        if not qr.empty:
            rule_totals = qr.groupby("rule_name").agg(rows_removed=("rows_removed", "sum")).reset_index()
            rule_totals = rule_totals.sort_values("rows_removed", ascending=False).head(10)
            st.dataframe(rule_totals, use_container_width=True, hide_index=True)
            if "year" in qr.columns and "rows_removed" in qr.columns:
                by_year = qr.groupby("year")["rows_removed"].sum().reset_index()
                fig_q = px.bar(by_year, x="year", y="rows_removed", labels={"year": "Ano", "rows_removed": "Linhas removidas"})
                fig_q.update_layout(height=250)
                st.plotly_chart(fig_q, use_container_width=True)
                if st.button("Explicar este gráfico", key="exp_quality_year"):
                    st.info(
                        explain_chart(
                            "Linhas removidas por ano (Silver)",
                            by_year.rename(columns={"year": "ano"}),
                            {},
                        )
                    )
        else:
            st.caption("Relatório de qualidade vazio.")
    except Exception:
        st.caption("Silver quality_report não disponível. Execute o pipeline Silver (contract + cleaning).")
else:
    st.caption("Silver quality_report não carregado. Execute o pipeline Silver para ver linhas válidas vs removidas e top regras.")
if has_null_report:
    try:
        nr = con.execute("SELECT * FROM silver.null_report").fetchdf()
        if not nr.empty and "column_name" in nr.columns and "pct_null" in nr.columns:
            top_null = nr.groupby("column_name")["pct_null"].max().reset_index().sort_values("pct_null", ascending=False).head(5)
            st.caption("**Top 5 colunas com maior % nulos:**")
            st.dataframe(top_null, use_container_width=True, hide_index=True)
    except Exception:
        pass
else:
    st.caption("Silver null_report não carregado.")
section_divider()

# =============================================================================
# SECTION D — Overview Nacional (Gold KPIs & Trends)
# =============================================================================
section_header_anchor("D — Overview Nacional (KPIs e tendências)", "sec-overview", level=2)
if not has_kpis:
    not_generated_yet("Tabela gold.kpis_uf_ano não encontrada.")
else:
    if df_kpis.empty:
        st.warning("Nenhum dado para os filtros selecionados.")
    else:
        agg_cols = {"media_objetiva": "mean", "media_redacao": "mean", "count_participantes": "sum"}
        if "pct_presence_full" in df_kpis.columns:
            agg_cols["pct_presence_full"] = "mean"
        agg = df_kpis.groupby("ano").agg(agg_cols).reset_index()
        if "pct_presence_full" in agg.columns:
            agg = agg.rename(columns={"pct_presence_full": "pct_presence"})
        else:
            agg["pct_presence"] = 100.0
        fig_trend = go.Figure()
        fig_trend.add_trace(go.Scatter(x=agg["ano"], y=agg["media_objetiva"], name="Média objetiva", mode="lines+markers"))
        fig_trend.add_trace(go.Scatter(x=agg["ano"], y=agg["media_redacao"], name="Média redação", mode="lines+markers"))
        fig_trend.update_layout(xaxis_title="Ano", yaxis_title="Nota média", height=350)
        st.plotly_chart(fig_trend, use_container_width=True)
        if st.button("Explicar este gráfico", key="exp_overview_trend"):
            st.info(
                explain_chart(
                    "Tendência (média por ano)",
                    agg[["ano", "media_objetiva", "media_redacao"]],
                    {"year_start": year_min, "year_end": year_max, "uf": uf_filter},
                )
            )
        by_uf = df_kpis.groupby("sg_uf_residencia").agg(media_objetiva=("media_objetiva", "mean"), count=("count_participantes", "sum")).reset_index()
        by_uf = by_uf[by_uf["sg_uf_residencia"].notna() & (by_uf["sg_uf_residencia"].astype(str).str.strip() != "") & (by_uf["sg_uf_residencia"].astype(str) != "NA")]
        by_uf = by_uf.sort_values("media_objetiva", ascending=False)
        if not by_uf.empty:
            fig_uf = px.bar(by_uf.head(27), x="sg_uf_residencia", y="media_objetiva", labels={"sg_uf_residencia": "UF", "media_objetiva": "Média objetiva"})
            fig_uf.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig_uf, use_container_width=True)
            if st.button("Explicar este gráfico", key="exp_overview_uf"):
                st.info(
                    explain_chart(
                        "Por UF (média objetiva no período)",
                        by_uf[["sg_uf_residencia", "media_objetiva"]],
                        {"year_start": year_min, "year_end": year_max, "uf": uf_filter},
                    )
                )
section_divider()

# =============================================================================
# SECTION E — Radar de Prioridade (Score 0–100)
# =============================================================================
section_header_anchor("E — Radar de Prioridade (Score 0–100)", "sec-radar", level=2)
if not has_kpis:
    not_generated_yet("Tabela gold.kpis_uf_ano não encontrada.")
else:
    with st.expander("Pesos da fórmula (clique para expandir)", expanded=False):
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            w_queda = st.slider("Queda tendência", 0.0, 1.0, 0.30, 0.05, key="w_queda")
        with c2:
            w_presenca = st.slider("Presença baixa", 0.0, 1.0, 0.25, 0.05, key="w_presenca")
        with c3:
            w_gap = st.slider("Gap socioeconômico", 0.0, 1.0, 0.25, 0.05, key="w_gap")
        with c4:
            w_cluster = st.slider("Cluster risco", 0.0, 1.0, 0.20, 0.05, key="w_cluster")
    total_w = w_queda + w_presenca + w_gap + w_cluster
    if abs(total_w - 1.0) > 0.01:
        total_w = max(total_w, 0.01)
        w_queda, w_presenca, w_gap, w_cluster = w_queda / total_w, w_presenca / total_w, w_gap / total_w, w_cluster / total_w
    df_r = con.execute("SELECT * FROM gold.kpis_uf_ano").fetchdf()
    df_r["ano"] = df_r["ano"].astype(int)
    anos_r = sorted(df_r["ano"].unique())
    if len(anos_r) >= 2:
        first_year, last_year = min(anos_r), max(anos_r)
        early = df_r[df_r["ano"] == first_year].set_index("sg_uf_residencia")
        late = df_r[df_r["ano"] == last_year].set_index("sg_uf_residencia")
        ufs_r = [u for u in (set(early.index) & set(late.index)) if u and str(u) != "NA"]
        max_media = df_r["media_objetiva"].max()
        scores = []
        for uf in ufs_r:
            m_early = early.loc[uf, "media_objetiva"] if uf in early.index else None
            m_late = late.loc[uf, "media_objetiva"] if uf in late.index else None
            p_early = early.loc[uf, "pct_presence_full"] if (uf in early.index and "pct_presence_full" in early.columns) else 100
            p_late = late.loc[uf, "pct_presence_full"] if (uf in late.index and "pct_presence_full" in late.columns) else 100
            queda = 100 * (1 - (m_late - m_early) / (max_media * 0.1 + 1e-6)) if m_early is not None and m_late is not None else 50
            queda = max(0, min(100, queda))
            presenca_baixa = max(0, min(100, 100 - (p_late if p_late is not None else 100)))
            gap = max(0, min(100, 100 * (1 - (m_late or 0) / (max_media or 1)) if max_media else 50))
            cluster_risk = 0
            if has_cluster_evolution:
                try:
                    ev = con.execute(f"SELECT ano, uf, cluster_id, pct_participants FROM gold.cluster_evolution_uf_ano WHERE ano IN ({first_year}, {last_year})").fetchdf()
                    if not ev.empty and "uf" in ev.columns:
                        ev_uf = ev[ev["uf"] == uf]
                        if not ev_uf.empty:
                            early_pct = ev_uf[ev_uf["ano"] == first_year]["pct_participants"].sum()
                            late_pct = ev_uf[ev_uf["ano"] == last_year]["pct_participants"].sum()
                            cluster_risk = min(100, max(0, 50 + (late_pct - early_pct) * 100))
                except Exception:
                    pass
            score = max(0, min(100, w_queda * queda + w_presenca * presenca_baixa + w_gap * gap + w_cluster * cluster_risk))
            scores.append({"uf": uf, "score": round(score, 1), "queda": round(queda, 1), "presenca_baixa": round(presenca_baixa, 1), "gap": round(gap, 1), "cluster_risk": cluster_risk})
        score_df = pd.DataFrame(scores).sort_values("score", ascending=False)
        st.dataframe(score_df.head(10), use_container_width=True, hide_index=True)
        fig_radar = px.bar(score_df.head(15), x="uf", y="score", labels={"uf": "UF", "score": "Score (0–100)"})
        fig_radar.update_layout(height=400)
        st.plotly_chart(fig_radar, use_container_width=True)
        if st.button("Explicar este gráfico", key="exp_radar"):
            st.info(
                explain_chart(
                    "Radar de Prioridade (Score 0–100 por UF)",
                    score_df[["uf", "score"]],
                    {"year_start": first_year, "year_end": last_year},
                )
            )
        top_uf = score_df.iloc[0]["uf"] if len(score_df) else "—"
        st.markdown(f"**Recomendações:** UFs no topo (ex.: {top_uf}) combinam tendência de queda, presença baixa, gap socioeconômico e/ou cluster de risco. Priorize reforço (matemática, redação), campanhas de presença e redução de desigualdade.")
    else:
        st.warning("Precisamos de pelo menos 2 anos para calcular o score.")
section_divider()

# =============================================================================
# SECTION F — Cohorts & Clusters (Mapa de Perfis)
# =============================================================================
section_header_anchor("F — Cohorts & Clusters (Mapa de Perfis)", "sec-clusters", level=2)
if not has_cluster_profiles:
    not_generated_yet("Tabelas gold.cluster_profiles e/ou gold.cluster_evolution_uf_ano não encontradas. Execute: python scripts/run_clusters_duckdb.py")
else:
    profiles = con.execute("SELECT * FROM gold.cluster_profiles").fetchdf()
    st.dataframe(profiles, use_container_width=True, hide_index=True)
    if not profiles.empty and "size" in profiles.columns:
        fig_dist = px.pie(profiles, values="size", names="cluster_id", title="Distribuição por cluster")
        st.plotly_chart(fig_dist, use_container_width=True)
    if has_cluster_evolution:
        ev_df = con.execute("SELECT * FROM gold.cluster_evolution_uf_ano").fetchdf()
        if "ano" in ev_df.columns:
            ev_df["ano"] = ev_df["ano"].astype(int)
        uf_col = "uf" if "uf" in ev_df.columns else ev_df.columns[1]
        ufs_ev = ["Todos"] + sorted(ev_df[uf_col].dropna().unique().tolist())
        uf_sel_ev = st.selectbox("Evolução por UF", ufs_ev, key="cluster_uf")
        ev_df_f = ev_df if uf_sel_ev == "Todos" else ev_df[ev_df[uf_col] == uf_sel_ev]
        pct_col = "pct_participants"
        if not ev_df_f.empty and pct_col in ev_df_f.columns and "cluster_id" in ev_df_f.columns and "ano" in ev_df_f.columns:
            ev_wide = ev_df_f.pivot_table(index="ano", columns="cluster_id", values=pct_col, aggfunc="first").reset_index()
            fig_ev = go.Figure()
            for c in ev_wide.columns:
                if c == "ano":
                    continue
                fig_ev.add_trace(go.Scatter(x=ev_wide["ano"], y=ev_wide[c], name=f"Cluster {c}", mode="lines+markers"))
            fig_ev.update_layout(xaxis_title="Ano", yaxis_title="% participantes", height=400)
            st.plotly_chart(fig_ev, use_container_width=True)
            if st.button("Explicar este gráfico", key="exp_clusters_evol"):
                st.info(
                    explain_chart(
                        "Evolução dos clusters por UF",
                        ev_df_f[["ano", "cluster_id", pct_col]],
                        {"uf": uf_sel_ev},
                    )
                )
    st.caption("**1 insight aplicável:** A evolução dos clusters por UF ao longo dos anos indica onde perfis de maior ou menor desempenho estão crescendo — use para direcionar políticas por território.")
section_divider()

# =============================================================================
# SECTION G — Plano de Fidelidade (Feature Produto)
# =============================================================================
section_header_anchor("G — Plano de Fidelidade (Feature Produto)", "sec-fidelity", level=2)
col_t1, col_t2, col_t3 = st.columns(3)
with col_t1:
    bronze_max = st.number_input("Bronze (máx. score)", 0, 100, 39, key="bronze")
with col_t2:
    silver_max = st.number_input("Silver (máx. score)", 0, 100, 69, key="silver")
with col_t3:
    st.caption("Gold: score > Silver máx.")

def tier_from_score(s, b_max, s_max):
    if s <= b_max:
        return "Bronze"
    if s <= s_max:
        return "Silver"
    return "Gold"

if has_kpis:
    df_t = con.execute("SELECT * FROM gold.kpis_uf_ano").fetchdf()
    df_t["ano"] = df_t["ano"].astype(int)
    anos_t = sorted(df_t["ano"].unique())
    if len(anos_t) >= 2:
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
        uf_sel_t = st.selectbox("Ver tier recomendado por UF", ["—"] + tier_df["uf"].tolist(), key="tier_uf")
        if uf_sel_t != "—":
            row = tier_df[tier_df["uf"] == uf_sel_t].iloc[0]
            st.metric("UF", uf_sel_t)
            st.metric("Score Radar", row["score"])
            st.metric("Tier recomendado", row["tier"])
    else:
        st.caption("Necessário mais de um ano para calcular score.")
st.markdown("**Benefícios por tier:**")
c1, c2, c3 = st.columns(3)
with c1:
    tier_card_dark("Bronze", "Bronze", ["Relatórios agregados (KPIs por UF/ano).", "Score Radar (top 10 UFs).", "Dashboards públicos."])
with c2:
    tier_card_dark("Silver", "Silver", ["Silver agregado ou amostras.", "Quality e null report.", "Score + segmentação por perfil/ano."])
with c3:
    tier_card_dark("Gold", "Gold", ["Gold completo (star schema, KPIs, clusters).", "LLM Analyst e previsões.", "Mapa de Perfis e decisão."])
section_divider()

# =============================================================================
# SECTION H — LLM Analyst Bot (Grounded) + Instant SQL Engine (Layer 0)
# =============================================================================
section_header_anchor("H — LLM Analyst Bot (Grounded)", "sec-llm", level=2)
st.caption("O bot consulta apenas tabelas Gold (e Silver). Resposta ancorada no resultado da query.")

# -------------------------------------------------------------------------
# Instant SQL Engine (Layer 0) — chips de intents
# -------------------------------------------------------------------------
st.markdown("#### Instant SQL Engine — consultas rápidas (Layer 0)")
st.caption(
    "Escolha um atalho de consulta; geramos o SQL de forma segura sobre as tabelas Gold/Silver "
    "e mostramos o resultado imediatamente."
)

main_intents_order = [
    "top_ufs_media_objetiva",
    "ufs_melhoraram_matematica",
    "media_redacao_por_ano",
    "media_objetiva_por_ano",
    "presenca_por_ano",
    "top_ufs_redacao_800",
    "pior_ano_media_objetiva",
    "gap_renda_media_objetiva",
    "tamanho_clusters",
    "cluster_crescimento_por_uf",
]

intent_labels = {
    "top_ufs_media_objetiva": "Top UFs média objetiva",
    "ufs_melhoraram_matematica": "UFs que mais melhoraram em matemática",
    "media_redacao_por_ano": "Média redação por ano",
    "media_objetiva_por_ano": "Média objetiva por ano",
    "presenca_por_ano": "Presença plena por ano",
    "top_ufs_redacao_800": "Top UFs redação ≥ 800",
    "pior_ano_media_objetiva": "Pior ano média objetiva",
    "gap_renda_media_objetiva": "Gap renda × média objetiva",
    "tamanho_clusters": "Tamanho dos clusters",
    "cluster_crescimento_por_uf": "Crescimento cluster por UF",
}

chosen_intent_id = None
instant_df = None
instant_sql = None
instant_error = None

cols_intents = st.columns(5)
for idx, intent_id in enumerate(main_intents_order):
    if intent_id not in INTENT_CATALOG:
        continue
    col = cols_intents[idx % len(cols_intents)]
    label = intent_labels.get(intent_id, INTENT_CATALOG[intent_id]["description"])
    with col:
        if st.button(label, key=f"intent_{intent_id}"):
            chosen_intent_id = intent_id

if chosen_intent_id:
    intent = INTENT_CATALOG[chosen_intent_id]
    params: dict = {}
    for name in intent.get("required_params", []):
        if name == "year_start":
            params[name] = year_min
        elif name == "year_end":
            params[name] = year_max
        elif name in {"year", "ano"}:
            params[name] = year_max
        elif name == "uf":
            params[name] = uf_filter if uf_filter != "Todos" else "BR"
        elif name == "limit":
            params[name] = intent.get("default_limit", 10)
        elif name == "cluster_id":
            params[name] = 0
        else:
            params[name] = intent.get("default_limit", 10)

    try:
        frozen = tuple(sorted(params.items()))
        instant_df, instant_sql, duration = cached_run_intent(chosen_intent_id, frozen)
    except Exception as e:
        instant_error = str(e)

if instant_error:
    st.error(f"Erro ao executar intent '{chosen_intent_id}': {instant_error}")
    from difflib import get_close_matches

    ids = list(INTENT_CATALOG.keys())
    suggestions = [
        i
        for i in get_close_matches(chosen_intent_id or "", ids, n=4, cutoff=0.0)
        if i != chosen_intent_id
    ][:3]
    if suggestions:
        st.caption("Tente também estes intents:")
        st.write(", ".join(f"`{s}`" for s in suggestions))
elif instant_sql is not None and instant_df is not None:
    st.caption("SQL instantâneo (camada 0):")
    st.code(instant_sql, language="sql")
    st.dataframe(instant_df.head(15), use_container_width=True, hide_index=True)
    # Performance + logging
    try:
        if "duration" in locals() and duration > 2.0:
            st.warning(f"Consulta levou {duration:.2f}s. Considere restringir o intervalo de anos ou a UF.")
        log_query(
            question=intent_labels.get(chosen_intent_id, chosen_intent_id),
            intent=chosen_intent_id,
            sql=instant_sql,
            rows_returned=len(instant_df),
            duration_sec=duration if "duration" in locals() else 0.0,
        )
    except Exception:
        pass

st.markdown("---")

# -------------------------------------------------------------------------
# LLM Analyst Bot (já existente)
# -------------------------------------------------------------------------
if "llm_q_input" not in st.session_state:
    st.session_state["llm_q_input"] = ""
demo_questions = [
    "Quais UFs melhoraram mais em matemática de 2021 a 2024?",
    "Qual a média de redação por ano no Brasil?",
    "Top 5 UFs com maior média objetiva em 2024?",
]
for i, q in enumerate(demo_questions):
    if st.button(f"Demo: {q[:50]}…", key=f"demo_q_{i}"):
        st.session_state["llm_q_input"] = q
        st.rerun()
question = st.text_input(
    "Pergunta",
    placeholder="Ex.: Quais UFs melhoraram mais em matemática?",
    key="llm_q_input",
)
run = st.button("Executar")
if run and question.strip():
    try:
        from assistant.app import run_bot

        plot_path = REPORTS_DIR / "figures" / "assistant_result.png"
        plot_path.parent.mkdir(parents=True, exist_ok=True)
        start_q = time.perf_counter()
        out = run_bot(question.strip(), con, out_plot_path=plot_path)
        duration_q = time.perf_counter() - start_q
        if out.get("error"):
            st.error(out["error"])
        else:
            df_res = out.get("result_df")
            if duration_q > 2.0:
                st.warning(f"Resposta levou {duration_q:.2f}s (LLM + consulta). Considere restringir anos/UF para acelerar.")
            st.code(out.get("sql") or "(nenhum)", language="sql")
            if df_res is not None and not df_res.empty:
                st.dataframe(
                    df_res.head(20),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.write(out.get("table_str") or "(sem tabela)")
            st.write(out.get("interpretation") or "(nenhuma)")
            if out.get("plot_path"):
                st.image(out["plot_path"], use_container_width=True)
        # Logging
        try:
            sql_logged = out.get("sql") or ""
            rows = len(out.get("result_df")) if out.get("result_df") is not None else 0
            log_query(
                question=question.strip(),
                intent="llm_bot",
                sql=sql_logged,
                rows_returned=rows,
                duration_sec=duration_q,
            )
        except Exception:
            pass
    except Exception as e:
        st.error(f"Erro ao executar o bot: {e}")
elif run:
    st.warning("Digite uma pergunta.")

section_divider()

# ----- Back to top -----
st.markdown('[↑ Voltar ao topo](#top)')
st.caption("Dados: data/gold | data/silver")
