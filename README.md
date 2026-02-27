# ENEM Opportunity & Equity Radar

## What is the product

**ENEM Opportunity & Equity Radar** is an analytics and decision-support stack over Brazilian ENEM (Exame Nacional do Ensino Médio) microdata (2020–2024). It provides:

- **Bronze/Silver/Gold** data layers: raw ingestion → cleaned, validated Silver → star-schema Gold (fact + dimensions + KPIs).
- **ML and clustering**: temporal ML (e.g. redação prediction), optional autoencoder embeddings and cluster profiles.
- **Streamlit dashboard**: Overview, Radar, Clusters, Fidelidade, and an **LLM Analyst Bot** that answers questions with SQL over Gold, returning results plus grounded interpretations (no invented numbers).
- **Reproducible pipelines** and a **live demo script** (2–3 min) for presentations.

All execution is **local** by default (no cloud dependency). Data source: [Microdados ENEM – INEP](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enem).

---

## Architecture

- **Raw (data/raw)**: CSVs per year, from INEP download scripts.
- **Bronze (data/bronze)**: Parquet per year, canonical schema + metadata; produced from raw.
- **Silver (data/silver)**: Cleaned participant table + quality/null reports; contract-driven (schema + mapping).
- **Gold (data/gold)**: Star schema (fato_desempenho, dim_tempo, dim_geografia, dim_perfil, kpis_uf_ano, distribuicoes_notas) + optional cluster_profiles, cluster_evolution_uf_ano, participant_embeddings.
- **App**: Streamlit over DuckDB views on Gold (and Silver reports). Bot: allowlisted SQL, DuckDB execution, interpretation from query results only.

```
raw (CSV) → bronze (Parquet) → silver (Parquet) → gold (Parquet)
                ↓                    ↓                  ↓
           pipelines/          silver_cleaning    gold_star_schema
           data_quality       _pipeline.py       ml/* (optional)
```

---

## Quickstart (local)

From the repo root:

```bash
# 1. Venv (recommended)
python -m venv .venv
.venv\Scripts\activate   # Windows
# source .venv/bin/activate   # Linux/macOS

# 2. Dependencies
pip install -r requirements.txt

# 3. (Option A – demo dataset, recomendado para testes rápidos)
python scripts/download_demo_data.py

# 4. Start one-page dashboard (executive)
streamlit run app/app_one_page.py

# 5. (Option B – full dataset, se quiser rodar tudo)
python scripts/01_coleta_enem.py
python scripts/02_coleta_enem_2024_completo.py
make bronze
make silver
make gold
python scripts/run_clusters_duckdb.py

# 6. Sanity check (opcional)
python scripts/smoke_test.py

# 7. Live demo (2–3 min)
make demo
# or: python demo/run_live_demo.py
```

---

## How to run pipelines in order

Run these **in sequence**; later steps depend on earlier outputs.

| Step   | Command        | Input              | Output                    |
|--------|----------------|--------------------|---------------------------|
| Raw    | Coleta scripts | (download)         | `data/raw/ENEM_*.csv`     |
| Bronze | `make bronze`  | `data/raw/`        | `data/bronze/enem_*.parquet` |
| Silver | `make silver`  | `data/bronze/`     | `data/silver/`            |
| Gold   | `make gold`    | `data/silver/`     | `data/gold/`              |
| ML     | `make ml`      | `data/gold/`       | `models/`, `reports/`     |
| Clusters | `make clusters` | `data/gold/`     | `data/gold/cluster_*.parquet` |

- **Bronze**: `make bronze` or `python scripts/run_bronze.py` (reads `data/raw/ENEM_*.csv`, writes `data/bronze/enem_*.parquet`).
- **Silver**: `python pipelines/silver_cleaning_pipeline.py`
- **Gold**: `python pipelines/gold_star_schema.py`
- **ML**: `python ml/train_ml_temporal.py` (and optionally `ml/train_autoencoder_embeddings.py`)
- **Clusters**: `python ml/cluster_profiles.py` (expects Gold + optional embeddings)

---

## How to run Streamlit

From repo root:

```bash
# Legacy multi-page sidebar app
streamlit run app/app.py

# One-page executive dashboard (recomendado)
streamlit run app/app_one_page.py
```

Or:

```bash
make app
```

Then open the URL shown in the terminal (e.g. `http://localhost:8501`). Use the sidebar (legacy app) or o header com navegação em pills (one-page) para trocar de seção.

---

## How to run the demo (2–3 min script)

The demo runs three fixed questions through the Analyst Bot (SQL over Gold, DuckDB, interpretation from results):

```bash
python demo/run_live_demo.py
```

Or:

```bash
make demo
```

For a **presenter script** (what to say, what to click, timing), see **demo/demo_script.md**.

---

## Folder structure

```
spark/
├── app/                    # Streamlit app
│   ├── app.py              # Main app (5 pages)
│   ├── components.py       # Shared UI components
│   └── db.py               # DuckDB connection, gold.* / silver.* views
├── assistant/              # LLM Analyst Bot
│   ├── bot.py              # run(), get_connection(), demo SQL
│   ├── sql_guard.py        # Allowlist, SELECT-only, LIMIT
│   ├── system_prompt.txt   # LLM system prompt
│   └── app.py              # Legacy CLI entry (optional)
├── config.py               # ANOS, DIR_RAW, DIR_PROCESSED, BASE_URL
├── contracts/              # Silver schema + mapping
│   ├── schema_canonico.yml
│   └── mapping_por_ano.yml
├── data/
│   ├── raw/                # ENEM_2020.csv … ENEM_2024.csv
│   ├── bronze/             # enem_2020.parquet … enem_2024.parquet
│   ├── silver/             # enem_participante, quality_report, null_report
│   └── gold/               # fato_desempenho, dim_*, kpis_uf_ano, distribuicoes_notas, …
├── demo/
│   ├── run_live_demo.py    # 3 questions, prints SQL + table + interpretation
│   ├── demo_script.md      # Presenter script (2–3 min)
│   └── gold_demo_queries.py
├── docs/                   # Architecture, CRISP-DM, modeling notes
├── ml/
│   ├── train_ml_temporal.py
│   ├── train_autoencoder_embeddings.py
│   └── cluster_profiles.py
├── pipelines/
│   ├── data_quality_pipeline.py   # Bronze (+ optional silver/gold in one go)
│   ├── silver_cleaning_pipeline.py
│   ├── gold_star_schema.py
│   └── validate_mapping.py
├── scripts/
│   ├── 01_coleta_enem.py
│   ├── 02_coleta_enem_2024_completo.py
│   ├── 02_spark_pipeline.py       # Alternative: raw → processed (legacy path)
│   ├── 03_analise_visualizacao.py
│   ├── run_all.sh                 # Run bronze → silver → gold → ml → clusters
│   └── smoke_test.py              # Parquet paths + DuckDB + 1 KPI query
├── models/                 # ML artifacts (e.g. baseline_model, strong_model)
├── reports/                 # Figures, metrics (e.g. reports/figures/)
├── Makefile                 # bronze, silver, gold, ml, clusters, app, demo
├── requirements.txt
└── README.md
```

---

## Smoke test

After building Gold (and optionally ML/clusters), run:

```bash
python scripts/smoke_test.py
```

This checks that required Gold Parquet paths exist, loads key tables in DuckDB, and runs one simple KPI query. Exit code is non-zero on failure.
