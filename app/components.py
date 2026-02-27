"""
ENEM Radar — Shared Streamlit UI components.
"""
import streamlit as st


def section_header(title: str, level: int = 2):
    if level == 1:
        st.markdown(f"# {title}")
    else:
        st.markdown(f"## {title}")


def section_header_anchor(title: str, anchor_id: str, level: int = 2):
    """Section header with an id for TOC anchor links."""
    tag = "h1" if level == 1 else "h2"
    st.markdown(f'<{tag} id="{anchor_id}">{title}</{tag}>', unsafe_allow_html=True)


def section_divider():
    """Horizontal divider between sections (dark theme)."""
    st.markdown(
        '<hr style="border: 0; height: 1px; background: linear-gradient(90deg, transparent, #ff3b3b40, transparent); margin: 2rem 0;">',
        unsafe_allow_html=True,
    )


def data_sources_badge(loaded: list, has_kpis: bool):
    """Badge showing which data sources are loaded."""
    n = len(loaded)
    if n == 0:
        label, color = "Nenhuma tabela", "#666"
    elif has_kpis:
        label, color = f"Gold + Silver ({n} tabelas)", "#2e7d32"
    else:
        label, color = f"{n} tabelas (sem KPIs)", "#f9a825"
    st.markdown(
        f'<span style="display: inline-block; padding: 0.2rem 0.6rem; border-radius: 4px; '
        f'background: {color}22; color: {color}; border: 1px solid {color}; font-size: 0.8rem;">{label}</span>',
        unsafe_allow_html=True,
    )


def toc_links(section_ids: list[tuple[str, str]]):
    """Mini TOC as markdown anchor links (scroll on click)."""
    links = " · ".join(f'[{label}](#{anchor_id})' for label, anchor_id in section_ids)
    st.markdown(f"**Navegação:** {links}")


def not_generated_yet(message: str = "Dados ainda não gerados. Execute os pipelines Bronze → Silver → Gold."):
    st.info(f"ℹ️ {message}")
    st.caption("Artefatos esperados: `data/gold/` (ex.: kpis_uf_ano, fato_desempenho) e opcionalmente `data/silver/`.")
    st.markdown("**Para gerar as tabelas, na raiz do projeto execute:**")
    st.code(
        "python scripts/run_bronze.py\n"
        "python pipelines/silver_cleaning_pipeline.py\n"
        "python pipelines/gold_star_schema.py",
        language="bash",
    )
    st.caption("Ou use: `make bronze` → `make silver` → `make gold` (ou `make all`). No Windows, se o Spark falhar, use: `python scripts/run_lakehouse_duckdb.py` (DuckDB, gera Parquet para cloud).")
    st.caption("**Se você já rodou o pipeline:** feche o dashboard (Ctrl+C no terminal) e suba de novo com `streamlit run app/app.py` para as tabelas serem carregadas.")


def tier_card(tier: str, title: str, benefits: list):
    """Render a tier benefit card (Bronze/Silver/Gold)."""
    colors = {"Bronze": "#cd7f32", "Silver": "#c0c0c0", "Gold": "#ffd700"}
    c = colors.get(tier, "#888")
    st.markdown(
        f"""
        <div style="border-left: 4px solid {c}; padding: 0.75rem 1rem; margin: 0.5rem 0; background: #f8f9fa; border-radius: 4px;">
        <strong>{title}</strong>
        <ul style="margin: 0.5rem 0 0 1rem; padding-left: 1rem;">
        {''.join(f'<li>{b}</li>' for b in benefits)}
        </ul>
        </div>
        """,
        unsafe_allow_html=True,
    )


def tier_card_dark(tier: str, title: str, benefits: list):
    """Tier card for dark theme (one-page)."""
    colors = {"Bronze": "#cd7f32", "Silver": "#c0c0c0", "Gold": "#ffd700"}
    c = colors.get(tier, "#888")
    st.markdown(
        f"""
        <div class="tier-card" style="border-left: 4px solid {c}; padding: 0.75rem 1rem; margin: 0.5rem 0; background: rgba(255,255,255,0.05); border-radius: 4px;">
        <strong>{title}</strong>
        <ul style="margin: 0.5rem 0 0 1rem; padding-left: 1rem;">
        {''.join(f'<li>{b}</li>' for b in benefits)}
        </ul>
        </div>
        """,
        unsafe_allow_html=True,
    )


def kpi_row(values: list[tuple[str, str]]):
    """Display a row of KPI columns (label, value)."""
    cols = st.columns(len(values))
    for i, (label, value) in enumerate(values):
        with cols[i]:
            st.metric(label, value)


def inject_dark_theme_css():
    """Inject CSS for dark theme, accent #ff3b3b, cards, section spacing."""
    st.markdown(
        """
        <style>
        /* Dark background */
        .stApp { background: #0e1117; }
        /* Accent */
        [data-testid="stMetricValue"] { color: #ff3b3b; }
        .stSlider > div > div > div { background: #ff3b3b !important; }
        /* Section spacing */
        h1, h2, h3 { margin-top: 1rem; margin-bottom: 0.5rem; }
        /* Cards */
        div[data-testid="stMetric"] {
            background: rgba(255,255,255,0.05);
            border: 1px solid rgba(255,59,59,0.2);
            border-radius: 8px;
            padding: 1rem;
        }
        div[data-testid="stMetric"]:hover { border-color: #ff3b3b40; }
        /* Expander */
        .streamlit-expanderHeader { background: rgba(255,255,255,0.03); border-radius: 4px; }
        </style>
        """,
        unsafe_allow_html=True,
    )
