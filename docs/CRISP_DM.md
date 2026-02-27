# CRISP-DM — ENEM Opportunity & Equity Radar (2020–2024)

**Mapeamento dos estágios CRISP-DM para o plano de 10 passos do projeto, com entregas e evidências.**

---

## 1. Mapeamento estágio → passo

| # | CRISP-DM | Passo no projeto | Entregas | Por que importa | Evidência (arquivos/tabelas) |
|---|----------|-------------------|----------|------------------|------------------------------|
| 1 | **Business Understanding** | Definição do problema, usuários e perguntas de negócio | Documento de entendimento; KPIs requeridos (crescimento por UF, risco por perfil, gap por área) | Alinha dados e modelo ao valor de negócio; define o que Gold deve suportar | `docs/CRISP_DM_STAGE_1_BUSINESS_UNDERSTANDING.md` |
| 2 | **Data Understanding** | Profiling dos dados brutos (estrutura, nulos, distribuições por ano) | Relatório de entendimento; identificação de drift de schema e de qualidade | Base para contrato Silver e regras de validação; evita surpresas em produção | `docs/DATA_UNDERSTANDING_REPORT.md`; pipeline `pipelines/data_understanding_profiling.py` |
| 3 | **Data Preparation (contrato)** | Schema canônico e mapeamento por ano; validação Bronze vs contrato | `schema_canonico`, `mapping_por_ano`; script de validação | Garante comparabilidade multi-ano e reprodutibilidade Silver/Gold | `docs/DATA_CONTRACT_SLIDE_SUMMARY.md`; `pipelines/validate_mapping.py`; YAML de contrato |
| 4 | **Data Preparation (Silver)** | Limpeza, tipagem, filtro de coorte válida; relatórios de qualidade | Silver Parquet (enem_participante); quality_report; null_report | Fonte única e confiável para Gold e modelos; auditoria de exclusões | `data/silver/`; `pipelines/silver_cleaning_pipeline.py`, `data_quality_pipeline.py`; `docs/SILVER_LAYER_REPORT.md` |
| 5 | **Modeling (modelagem de dados)** | Gold: star schema (fato + dims) e agregados | fato_desempenho, dim_tempo, dim_geografia, dim_perfil, kpis_uf_ano, distribuicoes_notas | Camada de decisão estável; suporte a dashboards e ML sem reprocessar Silver | `pipelines/gold_star_schema.py`; `data/gold/`; `docs/STAR_SCHEMA_DIAGRAM.md` |
| 6 | **Modeling (ML)** | Modelo de regressão temporal (ex.: redação); train 2020–2023, test 2024 | Modelos (baseline + strong); métricas; previsões 2024 | Predição com validação temporal; sem vazamento de futuro | `ml/train_ml_temporal.py`; `models/`; `reports/ml_metrics.json`, `predictions_2024.parquet` |
| 7 | **Modeling (explainability e avaliação)** | Importância de variáveis; análise de erro por grupo (UF, perfil) | Relatórios de explainability; métricas por slice; CSVs e figuras | Transparência e justiça; saber onde o modelo é confiável | `ml/explainability_report.py`; `ml/error_analysis_by_group.ipynb`; `reports/top_5_features.json`, `error_by_uf.csv`; `figures/` |
| 8 | **Modeling (DL / clustering)** | Embeddings (autoencoder) e clusters (KMeans); perfis e evolução | Autoencoder; participant_embeddings; cluster_profiles; cluster_evolution_uf_ano | Produto “Mapa de Perfis”; suporte a segmentação e evolução por UF/ano | `ml/train_autoencoder_embeddings.py`; `ml/cluster_profiles.py`; `data/gold/participant_embeddings.parquet`, `cluster_*.parquet` |
| 9 | **Evaluation** | Avaliação de modelos e de Gold; consistência de métricas e recuperação pós-pandemia | Relatórios de avaliação; resumo para slides | Garantir que métricas e tendências são interpretáveis e auditáveis | `docs/EVALUATION_SLIDE_SUMMARY.md`; `docs/INSIGHT_PACK.md`; notebooks de avaliação |
| 10 | **Deployment** | Assistente analítico (LLM) e demo ao vivo; dashboards Streamlit (referência) | Bot que retorna SQL + resultado + interpretação; demo com 3 perguntas | Decisão baseada em evidência; sem alucinação de números | `assistant/app.py`, `system_prompt.txt`; `demo/run_live_demo.py`; `docs/ARQUITETURA.md` |

---

## 2. Resumo por fase

- **Fases 1–2 (Understanding):** Entender o problema e os dados brutos; documentar e gerar evidência de profiling.
- **Fases 3–4 (Preparation):** Contrato + Silver; verdade organizada e auditável.
- **Fase 5 (Data Modeling):** Gold em star schema; base para tudo que segue.
- **Fases 6–8 (Modeling):** ML temporal, explainability, slices, DL e clusters; todos consumindo Gold.
- **Fase 9 (Evaluation):** Avaliação de modelos e da camada Gold; material para apresentação.
- **Fase 10 (Deployment):** Produto utilizável (bot + demo); arquitetura documentada.

---

## 3. Slide version

- **Título:** CRISP-DM mapeado em 10 passos — ENEM Opportunity & Equity Radar.
- **Tabela resumo:** 10 linhas (Business Understanding → Deployment) com: Passo | Entregas | Por que importa | Evidência.
- **Mensagem:** Do entendimento ao deploy, cada estágio gera entregas e artefatos rastreáveis (docs, pipelines, Parquet, modelos, reports).
