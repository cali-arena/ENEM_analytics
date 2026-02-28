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

# R2 + DuckDB: Parquet on Cloudflare R2 (see "Dashboard on R2" below)
streamlit run app/app_s3_duckdb.py
```

Or:

```bash
make app
```

Then open the URL shown in the terminal (e.g. `http://localhost:8501`). Use the sidebar (legacy app) or o header com navegação em pills (one-page) para trocar de seção.

---

## Dashboard on R2 (Parquet on Cloudflare R2 + DuckDB)

A separate **insanely fast** app reads Parquet directly from Cloudflare R2 (S3-compatible) via DuckDB `httpfs`, with no Postgres and no full-download step. Ideal for Streamlit Cloud with data already in R2.

### Run locally

1. **Secrets:** Copy `.streamlit/secrets.toml.example` to `.streamlit/secrets.toml` and set:
   - `R2_ACCESS_KEY`, `R2_SECRET_KEY`
   - `R2_ENDPOINT` (e.g. `https://<account_id>.r2.cloudflarestorage.com`)
   - `R2_BUCKET` (e.g. `datalake`)
   - `R2_REGION` = `auto` (optional)

2. **Run:**
   ```bash
   streamlit run app/app_s3_duckdb.py
   ```

   Alternatively use env vars instead of `secrets.toml` (e.g. `export R2_ACCESS_KEY=...`), or a `.env` file with `python-dotenv` loaded before Streamlit.

### Deploy on Streamlit Cloud

1. **App settings:** Point the app to your repo and set **Main file path** to `app/app_s3_duckdb.py` (for the R2 dashboard) or `app/app_one_page.py` (for the one-page ENEM dashboard).

2. **Configure R2 in Streamlit Cloud (same values as your local `secrets.toml`):**
   - Go to [share.streamlit.io](https://share.streamlit.io) and open your app.
   - Click **Settings** (or the ⋮ menu) → **Secrets** (or **Environment variables**).
   - Add these **keys** and the **values** you use locally (from `.streamlit/secrets.toml`):

   | Key | Example value |
   |-----|----------------|
   | `R2_ACCESS_KEY` | Your R2 access key ID |
   | `R2_SECRET_KEY` | Your R2 secret access key |
   | `R2_ENDPOINT` | `https://6fba91cb7ac1ead629102ed6944f7cb4.r2.cloudflarestorage.com` (your real URL) |
   | `R2_BUCKET` | `datalake` |
   | `R2_REGION` | `auto` (optional) |

   - Save. Streamlit Cloud will redeploy; the app will read these as `st.secrets["R2_ACCESS_KEY"]` etc. (no `secrets.toml` file is used on Cloud).

3. Deploy. On first load the R2 app runs a self-test against `s3://<bucket>/silver/**/*.parquet` and `gold/**/*.parquet`. If that fails, see troubleshooting below.

### R2 layout

- **Paths:** `s3://<bucket>/silver/...`, `s3://<bucket>/gold/...` (bronze optional).
- Override prefixes with secrets: `R2_SILVER_PREFIX`, `R2_GOLD_PREFIX`, `R2_BRONZE_PREFIX`.

### Demo no R2 (dados mockados)

Se o bucket estiver vazio e o app mostrar "Nenhum dado para os filtros" ou KPIs vazios, use dados sintéticos:

```bash
pip install boto3 pandas pyarrow
python scripts/build_and_upload_demo_r2.py
```

O script gera Parquets mínimos (kpis_uf_ano, cluster_profiles, cluster_evolution_uf_ano, quality_report, null_report) e envia para o R2. Credenciais: `.streamlit/secrets.toml` ou variáveis `R2_ACCESS_KEY`, `R2_SECRET_KEY`, `R2_ENDPOINT`, `R2_BUCKET`. Depois, no app, clique em **Refresh cache** e recarregue a página.

### Troubleshooting (R2 + DuckDB)

| Issue | What to check |
|-------|----------------|
| **Connection / 403 Forbidden** | Credentials are valid but token has no permission. In Cloudflare Dashboard → R2 → **Manage R2 API Tokens**: create a token with **Object Read** (and list) on the bucket; use the new Access Key and Secret in Secrets. |
| **Endpoint** | Use the full URL: `https://<account_id>.r2.cloudflarestorage.com`. The app uses path-style URLs (`s3_url_style='path'`). |
| **No data / empty** | Bucket and prefixes: files must be under `silver/` and `gold/` (or your custom prefixes). Use `**/*.parquet` so DuckDB can scan. |
| **Hive partition mismatch** | If you see "Hive partition mismatch between file ... and ...", your `gold/` (or `silver/`) has both root-level Parquet files and Hive-partitioned dirs (e.g. `ano=2020/`). The app uses `hive_partitioning=0` so both layouts work; partition columns from paths are not auto-added. |
| **Streamlit “run failed”** | Check Cloud logs for the exact DuckDB or R2 error; often endpoint typo or missing secret. |
| **“Não foi possível preparar dados Gold” / “DEMO_GOLD_URL”** | The app running is `app_one_page.py` (expects local data or demo zip). To use **R2** with the data you uploaded: set **Main file path** to `app/app_s3_duckdb.py` and keep R2 secrets. |

### Integração LLM: caixa de pergunta → banco Gold → mensagem formatada

Fluxo já implementado no app R2 (`app_s3_duckdb.py`), seção **H — LLM Analyst Bot**:

1. **Caixa de pergunta** — O usuário digita na "Pergunta (LLM)" e clica em **Executar LLM**.
2. **LLM gera SQL** — O modelo (OpenAI ou DeepSeek) recebe a pergunta e o schema do gold e devolve um `SELECT` em DuckDB.
3. **Banco Gold** — O SQL é executado no DuckDB que lê Parquet do R2 (`gold/kpis_uf_ano`, etc.); os dados vêm do data lake no R2, não de outro banco.
4. **Decoder de mensagem** — O resultado (DataFrame) é passado por `decode_llm_result_to_message()` e exibido como texto formatado (resumo + principais linhas) antes da tabela.

**R2:** O R2 neste projeto é o storage do **data lake** (Parquet silver/gold). **Não é obrigatório** armazenar as respostas do LLM no R2; o decoder só formata a resposta para exibir na UI. Se quiser persistir histórico de perguntas/respostas, aí pode gravar JSON (ou Parquet) no R2 ou em outro store.

**DeepSeek:** Pode usar a API DeepSeek (compatível com OpenAI). Defina `DEEPSEEK_API_KEY` (e opcionalmente `DEEPSEEK_BASE_URL`, padrão `https://api.deepseek.com`). O app usa a lib `openai`; não precisa de lib específica da DeepSeek para armazenar nada — DeepSeek é só o provedor do modelo. Se tanto `OPENAI_API_KEY` quanto `DEEPSEEK_API_KEY` estiverem definidos, o app usa OpenAI primeiro.

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

## Hospedagem grátis do demo (Streamlit Cloud + GitHub)

Para rodar o dashboard no **Streamlit Community Cloud** sem pagar nada, use **GitHub Releases** para hospedar o `demo_gold.zip` (dados mínimos). Tudo fica no mesmo repositório.

### 1. Gerar o zip de demo (local, uma vez)

Na raiz do repo:

```bash
python scripts/build_demo_gold.py
```

Isso cria **demo_gold.zip** na raiz (~10 KB), com Parquets mínimos (kpis_uf_ano, dims, clusters, etc.) sem precisar de CSVs em `data/raw`.

### 2. Subir o zip em um Release do GitHub

1. Abra o repositório no GitHub (ex.: `cali-arena/ENEM_analytics`).
2. **Releases** → **Create a new release**.
3. Tag (ex.: `v0.1.0`) e título (ex.: `Demo data for Streamlit Cloud`).
4. Em **Assets**, anexe o arquivo **demo_gold.zip** (arrastar ou upload).
5. Publique o release.
6. Copie a URL do asset. Formato:
   `https://github.com/<user>/<repo>/releases/download/<tag>/demo_gold.zip`  
   Ex.: `https://github.com/cali-arena/ENEM_analytics/releases/download/v0.1.0/demo_gold.zip`

### 3. Configurar o Streamlit Cloud

1. No [Streamlit Community Cloud](https://share.streamlit.io), crie (ou edite) o app apontando para o repo.
2. **Settings** do app → **Secrets** / **Environment variables**.
3. Adicione:
   - **DEMO_GOLD_URL** = `https://github.com/.../releases/download/v0.1.0/demo_gold.zip` (a URL que você copiou).
4. Salve e faça **Rerun** do app.

Na primeira execução o app baixa o zip, extrai em `data/gold` e o dashboard carrega com os dados de demo. Tudo **grátis** (GitHub + Streamlit Cloud).

---

## Folder structure

```
spark/
├── app/                    # Streamlit app
│   ├── app.py              # Main app (5 pages)
│   ├── app_one_page.py     # One-page executive dashboard
│   ├── app_s3_duckdb.py    # R2 + DuckDB dashboard (Parquet on S3)
│   ├── components.py       # Shared UI components
│   ├── db.py               # DuckDB connection, gold.* / silver.* views
│   └── lib/                # R2 + DuckDB helpers
│       ├── storage_r2.py   # R2 path/config from secrets
│       └── duckdb_conn.py   # DuckDB + httpfs init
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
