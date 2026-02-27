# Arquitetura — ENEM Opportunity & Equity Radar (2020–2024)

**Visão de ponta a ponta: ingestão → lakehouse → produto → assistente.**

---

## 1. Fluxo end-to-end

```
  CSV (INEP)     Bronze          Silver           Gold           Produto
  ───────────►  ───────►       ───────►        ───────►       ─────────────
  Raw CSV       Parquet         Parquet         Parquet        Streamlit
  (por ano)     raw/ano         enem_           star schema    dashboards
                imutável        participante     + KPIs         + LLM Analyst
                                valid cohort     decisão        (DuckDB/SparkSQL)
```

- **CSV → Bronze:** Ingestão fiel do que o INEP entrega (um dataset por ano). Sem transformação de negócio; preservação da “verdade bruta” e rastreabilidade.
- **Bronze → Silver:** Contrato de schema (canônico + mapeamento por ano), validação (presença, faixas de nota, códigos), filtro de coorte válida. Silver = verdade organizada e confiável para analytics.
- **Silver → Gold:** Modelagem dimensional (star schema), KPIs agregados (UF/ano, distribuições), tabelas para decisão. Gold = verdade transformada em decisão.
- **Gold → Produto:** Dashboards (Streamlit) e assistente analítico (LLM) que consultam apenas Gold (e opcionalmente relatórios Silver). LLM gera SQL contra allowlist; DuckDB ou SparkSQL executam; resposta sempre ancorada em resultado de query.

---

## 2. Diagrama ASCII (componentes e responsabilidades)

```
                    ┌─────────────────────────────────────────────────────────────────┐
                    │                     ENEM OPPORTUNITY & EQUITY RADAR               │
                    └─────────────────────────────────────────────────────────────────┘
                                                     │
    ┌──────────────┐    ┌──────────────┐    ┌───────▼───────┐    ┌──────────────┐    ┌──────────────┐
    │   CSV/INEP   │───►│    BRONZE    │───►│    SILVER     │───►│     GOLD     │───►│   PRODUTO    │
    │  (por ano)   │    │   Parquet    │    │   Parquet     │    │  Star+KPIs   │    │ Streamlit +  │
    │  scripts/    │    │  raw/ano     │    │ enem_partici- │    │ pipelines/   │    │ LLM Analyst  │
    │ 01_coleta    │    │ imutável     │    │ pante +       │    │ gold_star_   │    │ assistant/   │
    └──────────────┘    └──────────────┘    │ quality_      │    │ schema.py    │    └──────────────┘
                                           │ report        │    └───────┬───────┘           │
                                           └──────────────┘            │                   │
                                                │                      │                   │
                                                │                 ┌────▼────┐         ┌────▼────┐
                                                │                 │  ML/DL  │         │ DuckDB  │
                                                │                 │ train_  │         │ or      │
                                                │                 │ ml_*    │         │ SparkSQL│
                                                │                 │ cluster_│         │ SQL only│
                                                │                 │ profiles│         │ allowlist
                                                │                 └─────────┘         └─────────┘
                                                │
                                           null_report,
                                           checksums
```

---

## 3. Componentes e responsabilidades

| Componente | Responsabilidade | Artefatos principais |
|------------|------------------|----------------------|
| **Coleta (CSV)** | Download e gravação dos microdados INEP por ano; encoding e delimitador preservados. | `data/raw/` ou equivalente; scripts `01_coleta_enem.py`, `02_coleta_*`. |
| **Bronze** | Armazenar exatamente o que foi ingerido; sem limpeza de negócio. Metadados (ano, ingest_ts, source_file) permitem auditoria. | Parquet por ano em `data/bronze/` (ou raw). |
| **Silver** | Aplicar contrato (schema canônico + mapeamento por ano), validar presença, notas, códigos; filtrar coorte válida; gerar quality_report e null_report. | `data/silver/enem_participante/`, `quality_report`, `null_report`. |
| **Gold** | Star schema (fato + dims) e agregados (KPIs por UF/ano, distribuições); embeddings e clusters como produto de modelagem. | `fato_desempenho`, `dim_*`, `kpis_uf_ano`, `distribuicoes_notas`, `cluster_*`, `participant_embeddings`. |
| **Spark** | Motor de processamento para Bronze → Silver → Gold; leitura de CSV/Parquet; agregações e joins. | Pipelines em `pipelines/`, scripts em `scripts/`. |
| **Parquet** | Formato padrão em Bronze, Silver e Gold; particionamento por `ano` onde aplicável; compatível com Spark e DuckDB. | Todos os diretórios `data/*/`. |
| **DuckDB / SparkSQL** | Execução de consultas analíticas sobre Gold (e Silver quando permitido). Bot usa DuckDB em memória; demos podem usar SparkSQL. | `assistant/app.py`, `demo/run_live_demo.py`. |
| **ML/DL** | Modelagem: regressão temporal (train 2020–2023, test 2024), explainability, autoencoder (embeddings), clustering (perfis). Entrada e saída ancoradas em Gold. | `ml/train_ml_temporal.py`, `explainability_report.py`, `train_autoencoder_embeddings.py`, `cluster_profiles.py`. |
| **Streamlit** | Dashboards de visualização sobre Gold (e relatórios Silver quando relevante). | App Streamlit (se implementado); referência na arquitetura. |
| **LLM Analyst** | Assistente que responde apenas com SQL + resultado + interpretação; allowlist de tabelas; sem números inventados. | `assistant/app.py`, `system_prompt.txt`; output: SQL, tabela, interpretação, opcional gráfico. |

---

## 4. Slide version

- **Uma frase:** De CSV (INEP) a Bronze (raw) → Silver (verdade organizada) → Gold (decisão) → Streamlit + LLM, com Spark/Parquet no núcleo e DuckDB/SparkSQL + allowlist no bot.
- **Diagrama:** ASCII acima (CSV → Bronze → Silver → Gold → Produto; Gold alimenta ML/DL e DuckDB/SparkSQL).
- **Componentes:** Coleta → Bronze → Silver (contrato + quality/null) → Gold (star + KPIs + clusters) → Streamlit + LLM; Spark processa; Parquet persiste; DuckDB/SparkSQL consultam; ML/DL só consome Gold.
