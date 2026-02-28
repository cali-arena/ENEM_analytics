"""
ENEM Opportunity & Equity Radar (2020–2024) — Streamlit dashboard.

Reads Parquet from data/gold (and optional silver reports) via DuckDB.
Se data/gold não existir, roda o pipeline DuckDB ao abrir (desde que haja CSVs em data/raw).
Run: streamlit run app/app.py  (from repo root)
"""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from app.db import get_connection, table_exists, list_loaded_tables
from app.components import section_header, not_generated_yet, tier_card, kpi_row


def _gold_has_data() -> bool:
    """Verifica se já existe tabela gold (kpis_uf_ano) em data/gold."""
    p = ROOT / "data" / "gold" / "kpis_uf_ano"
    if not p.exists():
        return False
    if p.is_dir():
        return any(p.rglob("*.parquet"))
    return True


def _raw_has_data() -> bool:
    """Verifica se há CSVs em data/raw para rodar o pipeline."""
    raw = ROOT / "data" / "raw"
    return raw.exists() and bool(list(raw.glob("*.csv")))


def _clusters_has_data() -> bool:
    """Verifica se cluster_profiles e cluster_evolution existem em data/gold."""
    p1 = ROOT / "data" / "gold" / "cluster_profiles.parquet"
    p2 = ROOT / "data" / "gold" / "cluster_evolution_uf_ano.parquet"
    return p1.exists() and p2.exists()


# Page config
st.set_page_config(page_title="ENEM Radar", layout="wide", initial_sidebar_state="expanded")

# Se não há Gold, gera automaticamente (se houver raw) e recarrega
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
        st.warning("Nenhum CSV em data/raw. Coloque os arquivos ENEM_2020.csv a ENEM_2024.csv em data/raw e recarregue a página.")
        st.stop()

# Se há Gold mas faltam clusters (Cohorts & Clusters), gera e recarrega
if _gold_has_data() and not _clusters_has_data():
    with st.spinner("Gerando clusters (Cohorts & Clusters)..."):
        try:
            from scripts.run_clusters_duckdb import main as run_clusters
            run_clusters()
        except Exception as e:
            st.warning(f"Clusters não gerados: {e}. A página Cohorts & Clusters pode ficar vazia.")
        else:
            st.rerun()

# DuckDB connection (cached)
@st.cache_resource
def get_db():
    return get_connection()

con = get_db()
loaded = list_loaded_tables(con)
has_kpis = table_exists(con, "gold.kpis_uf_ano")
has_cluster_profiles = table_exists(con, "gold.cluster_profiles")
has_cluster_evolution = table_exists(con, "gold.cluster_evolution_uf_ano")

# Se os Parquets de cluster existem no disco mas a conexão (cache antigo) não os carregou, forçar nova conexão
if _clusters_has_data() and not has_cluster_profiles:
    get_db.clear()
    st.rerun()


# ----- Sidebar navigation -----
st.sidebar.title("ENEM Opportunity & Equity Radar")
st.sidebar.caption("2020–2024 | Gold + Silver")
page = st.sidebar.radio(
    "Página",
    [
        "1. Overview Nacional",
        "2. Radar de Prioridade",
        "3. Cohorts & Clusters",
        "4. Plano de Fidelidade",
        "5. LLM Analyst Bot",
    ],
    label_visibility="collapsed",
)
if not loaded:
    st.sidebar.warning("Nenhuma tabela Gold/Silver carregada. Verifique data/gold e data/silver.")

# ----- Page 1: Overview Nacional -----
if page == "1. Overview Nacional":
    section_header("Overview Nacional (2020–2024)", 1)
    if not has_kpis:
        not_generated_yet("Tabela gold.kpis_uf_ano não encontrada.")
        st.stop()
    df = con.execute("SELECT * FROM gold.kpis_uf_ano").fetchdf()
    df["ano"] = df["ano"].astype(int)
    anos = sorted(df["ano"].dropna().unique().tolist())
    ufs = ["Todos"] + sorted(df["sg_uf_residencia"].dropna().unique().tolist())
    ufs = [u for u in ufs if u and str(u).strip() and str(u) != "NA"]
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        year_range = st.slider("Anos", min_value=min(anos) if anos else 2020, max_value=max(anos) if anos else 2024, value=(min(anos), max(anos)))
    with col_f2:
        uf_filter = st.selectbox("UF", ufs)
    df = df[df["ano"].between(year_range[0], year_range[1])]
    if uf_filter != "Todos":
        df = df[df["sg_uf_residencia"] == uf_filter]
    if df.empty:
        st.warning("Nenhum dado para os filtros selecionados.")
        st.stop()
    # KPIs
    agg_cols = {"media_objetiva": "mean", "media_redacao": "mean", "count_participantes": "sum"}
    if "pct_presence_full" in df.columns:
        agg_cols["pct_presence_full"] = "mean"
    agg = df.groupby("ano").agg(agg_cols).reset_index()
    if "pct_presence_full" in agg.columns:
        agg = agg.rename(columns={"pct_presence_full": "pct_presence"})
    else:
        agg["pct_presence"] = 100.0
    st.subheader("KPIs agregados por ano")
    kpi_row([
        ("Média objetiva (último ano)", f"{agg['media_objetiva'].iloc[-1]:.1f}" if len(agg) else "—"),
        ("Média redação (último ano)", f"{agg['media_redacao'].iloc[-1]:.1f}" if len(agg) else "—"),
        ("Presença média (%)", f"{agg['pct_presence'].iloc[-1]:.1f}%" if len(agg) else "—"),
    ])
    # Trend charts
    st.subheader("Tendência (média por ano)")
    fig_trend = go.Figure()
    fig_trend.add_trace(go.Scatter(x=agg["ano"], y=agg["media_objetiva"], name="Média objetiva", mode="lines+markers"))
    fig_trend.add_trace(go.Scatter(x=agg["ano"], y=agg["media_redacao"], name="Média redação", mode="lines+markers"))
    fig_trend.update_layout(xaxis_title="Ano", yaxis_title="Nota média", height=350)
    st.plotly_chart(fig_trend, use_container_width=True)
    # By UF: bar or choropleth
    st.subheader("Por UF (média objetiva no período)")
    by_uf = df.groupby("sg_uf_residencia").agg(media_objetiva=("media_objetiva", "mean"), count=("count_participantes", "sum")).reset_index()
    by_uf = by_uf[by_uf["sg_uf_residencia"].notna() & (by_uf["sg_uf_residencia"].astype(str).str.strip() != "")]
    by_uf = by_uf[by_uf["sg_uf_residencia"].astype(str) != "NA"].sort_values("media_objetiva", ascending=False)
    if by_uf.empty:
        st.caption("Sem dados por UF para os filtros atuais.")
    else:
        fig_uf = px.bar(by_uf.head(27), x="sg_uf_residencia", y="media_objetiva", labels={"sg_uf_residencia": "UF", "media_objetiva": "Média objetiva"})
        fig_uf.update_layout(height=400, xaxis_tickangle=-45)
        st.plotly_chart(fig_uf, use_container_width=True)
        st.caption("Mapa choropleth (Brasil) pode ser adicionado com GeoJSON; aqui exibimos barras por UF.")

# ----- Page 2: Radar de Prioridade -----
elif page == "2. Radar de Prioridade":
    section_header("Radar de Prioridade (Score 0–100)", 1)
    if not has_kpis:
        not_generated_yet("Tabela gold.kpis_uf_ano não encontrada.")
        st.stop()
    df = con.execute("SELECT * FROM gold.kpis_uf_ano").fetchdf()
    df["ano"] = df["ano"].astype(int)
    # Weights
    st.subheader("Pesos da fórmula")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        w_queda = st.slider("Queda tendência", 0.0, 1.0, 0.30, 0.05)
    with c2:
        w_presenca = st.slider("Presença baixa", 0.0, 1.0, 0.25, 0.05)
    with c3:
        w_gap = st.slider("Gap socioeconômico", 0.0, 1.0, 0.25, 0.05)
    with c4:
        w_cluster = st.slider("Cluster risco crescendo", 0.0, 1.0, 0.20, 0.05)
    total_w = w_queda + w_presenca + w_gap + w_cluster
    if abs(total_w - 1.0) > 0.01:
        st.caption(f"Pesos somam {total_w:.2f}; normalizando para 1.0.")
        w_queda, w_presenca, w_gap, w_cluster = w_queda/total_w, w_presenca/total_w, w_gap/total_w, w_cluster/total_w
    # Compute components per UF (last vs first year in data)
    anos = sorted(df["ano"].unique())
    if len(anos) < 2:
        st.warning("Precisamos de pelo menos 2 anos para calcular tendência.")
        st.stop()
    first_year, last_year = min(anos), max(anos)
    early = df[df["ano"] == first_year].set_index("sg_uf_residencia")
    late = df[df["ano"] == last_year].set_index("sg_uf_residencia")
    ufs = list(set(early.index) & set(late.index))
    ufs = [u for u in ufs if u and str(u) != "NA"]
    max_media = df["media_objetiva"].max()
    scores = []
    for uf in ufs:
        m_early = early.loc[uf, "media_objetiva"] if uf in early.index else None
        m_late = late.loc[uf, "media_objetiva"] if uf in late.index else None
        p_early = early.loc[uf, "pct_presence_full"] if (uf in early.index and "pct_presence_full" in early.columns) else 100
        p_late = late.loc[uf, "pct_presence_full"] if (uf in late.index and "pct_presence_full" in late.columns) else 100
        queda = 100 * (1 - (m_late - m_early) / (max_media * 0.1 + 1e-6)) if m_early is not None and m_late is not None else 50
        queda = max(0, min(100, queda))
        presenca_baixa = 100 - (p_late if p_late is not None else 100)
        presenca_baixa = max(0, min(100, presenca_baixa))
        gap = 100 * (1 - (m_late or 0) / (max_media or 1)) if max_media else 50
        gap = max(0, min(100, gap))
        cluster_risk = 0  # placeholder unless cluster_evolution exists
        if has_cluster_evolution:
            try:
                ev = con.execute(
                    "SELECT ano, uf, cluster_id, pct_participants FROM gold.cluster_evolution_uf_ano WHERE ano IN (" + str(first_year) + ", " + str(last_year) + ")"
                ).fetchdf()
                if not ev.empty and "cluster_id" in ev.columns and "uf" in ev.columns:
                    ev_uf = ev[ev["uf"] == uf] if "uf" in ev.columns else ev
                    if not ev_uf.empty:
                        early_pct = ev_uf[ev_uf["ano"] == first_year]["pct_participants"].sum()
                        late_pct = ev_uf[ev_uf["ano"] == last_year]["pct_participants"].sum()
                        cluster_risk = min(100, max(0, 50 + (late_pct - early_pct) * 100))
            except Exception:
                pass
        score = w_queda * queda + w_presenca * presenca_baixa + w_gap * gap + w_cluster * cluster_risk
        score = max(0, min(100, score))
        scores.append({"uf": uf, "score": round(score, 1), "queda": round(queda, 1), "presenca_baixa": round(presenca_baixa, 1), "gap": round(gap, 1), "cluster_risk": cluster_risk})
    score_df = pd.DataFrame(scores).sort_values("score", ascending=False)
    st.subheader("Ranking Top 10 UF (prioridade)")
    st.dataframe(score_df.head(10), use_container_width=True, hide_index=True)
    fig_radar = px.bar(score_df.head(15), x="uf", y="score", labels={"uf": "UF", "score": "Score (0–100)"})
    fig_radar.update_layout(height=400)
    st.plotly_chart(fig_radar, use_container_width=True)
    # Decision note
    top_uf = score_df.iloc[0]["uf"] if len(score_df) else "—"
    st.subheader("Recomendações e nota de decisão")
    st.write(
        f"**Áreas de foco sugeridas:** UFs no topo do ranking (ex.: {top_uf}) apresentam maior combinação de tendência de queda, presença baixa, gap socioeconômico e/ou crescimento de cluster de risco. "
        "Recomenda-se priorizar políticas de reforço (ex.: matemática e redação), campanhas de presença e ações voltadas à redução de desigualdade nessas UFs. "
        "Os pesos da fórmula podem ser ajustados no painel acima conforme o critério da secretaria ou da instituição."
    )

# ----- Page 3: Cohorts & Clusters -----
elif page == "3. Cohorts & Clusters":
    section_header("Cohorts & Clusters — Mapa de Perfis", 1)
    if not has_cluster_profiles:
        not_generated_yet("Tabelas gold.cluster_profiles e/ou gold.cluster_evolution_uf_ano não encontradas. Execute: python scripts/run_clusters_duckdb.py (ou recarregue o app para gerar automaticamente).")
        st.stop()
    profiles = con.execute("SELECT * FROM gold.cluster_profiles").fetchdf()
    st.subheader("Resumo por cluster")
    st.dataframe(profiles, use_container_width=True, hide_index=True)
    if not profiles.empty and "size" in profiles.columns:
        fig_dist = px.scatter(
            profiles, x="cluster_id", y="size",
            size="size", color="cluster_id",
            title="Distribuição de participantes por cluster (dispersão)",
            labels={"cluster_id": "Cluster", "size": "Tamanho"}
        )
        fig_dist.update_layout(showlegend=True, height=400)
        st.plotly_chart(fig_dist, use_container_width=True)
    if has_cluster_evolution:
        ev_df = con.execute("SELECT * FROM gold.cluster_evolution_uf_ano").fetchdf()
        if "ano" in ev_df.columns:
            ev_df["ano"] = ev_df["ano"].astype(int)
        uf_col = "uf" if "uf" in ev_df.columns else ev_df.columns[1]
        ufs_ev = ["Todos"] + sorted(ev_df[uf_col].dropna().unique().tolist())
        uf_sel = st.selectbox("Evolução por UF", ufs_ev, key="cluster_uf")
        if uf_sel != "Todos":
            ev_df = ev_df[ev_df[uf_col] == uf_sel]
        pct_col = "pct_participants" if "pct_participants" in ev_df.columns else None
        if not pct_col and "pct_participants" in ev_df.columns:
            pct_col = "pct_participants"
        if not ev_df.empty and pct_col and "cluster_id" in ev_df.columns and "ano" in ev_df.columns:
            ev_wide = ev_df.pivot_table(index="ano", columns="cluster_id", values=pct_col, aggfunc="first").reset_index()
            fig_ev = go.Figure()
            for c in ev_wide.columns:
                if c == "ano":
                    continue
                fig_ev.add_trace(go.Scatter(x=ev_wide["ano"], y=ev_wide[c], name=f"Cluster {c}", mode="lines+markers"))
            fig_ev.update_layout(xaxis_title="Ano", yaxis_title="% participantes", title="Evolução 2020→2024" + (f" — {uf_sel}" if uf_sel != "Todos" else ""), height=400)
            st.plotly_chart(fig_ev, use_container_width=True)
    else:
        st.caption("Evolução por UF disponível quando gold.cluster_evolution_uf_ano existir.")

# ----- Page 4: Plano de Fidelidade -----
elif page == "4. Plano de Fidelidade":
    section_header("Plano de Fidelidade (Feature Produto)", 1)
    st.subheader("Limiares do tier (configuráveis)")
    col_t1, col_t2, col_t3 = st.columns(3)
    with col_t1:
        bronze_max = st.number_input("Bronze (máx. score)", 0, 100, 39, key="bronze")
    with col_t2:
        silver_max = st.number_input("Silver (máx. score)", 0, 100, 69, key="silver")
    with col_t3:
        st.caption("Gold: score > Silver máx.")
    def tier_from_score(s):
        if s <= bronze_max:
            return "Bronze"
        if s <= silver_max:
            return "Silver"
        return "Gold"
    if has_kpis:
        df = con.execute("SELECT * FROM gold.kpis_uf_ano").fetchdf()
        df["ano"] = df["ano"].astype(int)
        anos = sorted(df["ano"].unique())
        if len(anos) >= 2:
            first_year, last_year = min(anos), max(anos)
            early = df[df["ano"] == first_year].set_index("sg_uf_residencia")
            late = df[df["ano"] == last_year].set_index("sg_uf_residencia")
            ufs = [u for u in set(early.index) & set(late.index) if u and str(u) != "NA"]
            max_media = df["media_objetiva"].max()
            tier_list = []
            for uf in ufs:
                m_early = early.loc[uf, "media_objetiva"] if uf in early.index else None
                m_late = late.loc[uf, "media_objetiva"] if uf in late.index else None
                queda = 100 * (1 - (m_late - m_early) / (max_media * 0.1 + 1e-6)) if m_early is not None and m_late is not None else 50
                score = 0.3 * max(0, min(100, queda)) + 0.25 * 0 + 0.25 * (100 * (1 - (m_late or 0) / (max_media or 1))) + 0.2 * 0
                score = max(0, min(100, score))
                tier_list.append({"uf": uf, "score": round(score, 1), "tier": tier_from_score(score)})
            tier_df = pd.DataFrame(tier_list).sort_values("score", ascending=False)
            uf_sel = st.selectbox("Ver tier recomendado por UF", ["—"] + tier_df["uf"].tolist(), key="tier_uf")
            if uf_sel != "—":
                row = tier_df[tier_df["uf"] == uf_sel].iloc[0]
                st.metric("UF", uf_sel)
                st.metric("Score Radar", row["score"])
                st.metric("Tier recomendado", row["tier"])
        else:
            st.caption("Necessário mais de um ano para calcular score.")
    st.subheader("Benefícios por tier")
    tier_card("Bronze", "Bronze", ["Acesso a relatórios agregados (KPIs por UF/ano).", "Score Radar em nível agregado (ex.: top 10 UFs).", "Dashboards públicos ou resumos."])
    tier_card("Silver", "Silver", ["Tabelas Silver agregadas ou amostras anonimizadas.", "Quality e null report para transparência.", "Score Radar + segmentação por perfil e ano."])
    tier_card("Gold", "Gold", ["Gold completo (star schema, KPIs, clusters).", "LLM Analyst e previsões/embeddings quando contratado.", "Mapa de Perfis e decisão tática/operacional."])

# ----- Page 5: LLM Analyst Bot -----
elif page == "5. LLM Analyst Bot":
    section_header("LLM Analyst Bot", 1)
    st.caption("O bot consulta apenas tabelas Gold (e relatórios Silver). Resposta sempre ancorada no resultado da query.")
    question = st.text_input("Pergunta", placeholder="Ex.: Quais UFs melhoraram mais em matemática de 2021 a 2024?")
    run = st.button("Executar")
    if run and question.strip():
        try:
            from assistant.app import run_bot
            plot_path = ROOT / "reports" / "figures" / "assistant_result.png"
            plot_path.parent.mkdir(parents=True, exist_ok=True)
            out = run_bot(question.strip(), con, out_plot_path=plot_path)
            if out.get("error"):
                st.error(out["error"])
            else:
                st.subheader("SQL gerado")
                st.code(out.get("sql") or "(nenhum)", language="sql")
                st.subheader("Resultado")
                if out.get("result_df") is not None and not out["result_df"].empty:
                    st.dataframe(out["result_df"].head(20), use_container_width=True, hide_index=True)
                else:
                    st.write(out.get("table_str") or "(sem tabela)")
                st.subheader("Interpretação")
                st.write(out.get("interpretation") or "(nenhuma)")
                if out.get("plot_path"):
                    st.subheader("Gráfico")
                    st.image(out["plot_path"], use_container_width=True)
        except Exception as e:
            st.error(f"Erro ao executar o bot: {e}")
            st.caption("Verifique se OPENAI_API_KEY está definido para perguntas fora das demos, ou use uma das perguntas demo.")
    elif run:
        st.warning("Digite uma pergunta.")

# Footer
st.sidebar.divider()
st.sidebar.caption("Dados: data/gold | data/silver")
