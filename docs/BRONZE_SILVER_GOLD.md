# Bronze, Silver e Gold — ENEM Opportunity & Equity Radar (2020–2024)

**Comparação slide-ready das camadas: função, valor e artefatos. Inclui como garantimos auditoria.**

---

## 1. Frase de síntese

**“Bronze guarda a verdade. Silver organiza a verdade. Gold transforma a verdade em decisão.”**

- **Bronze:** Preserva o que foi entregue (fonte); sem alteração de negócio; verdade bruta e auditável.
- **Silver:** Aplica contrato, validação e filtros; verdade única, organizada e confiável para analytics.
- **Gold:** Agrega e modela para decisão (KPIs, star schema, clusters); verdade pronta para produto e LLM.

---

## 2. Tabela comparativa (slide-ready)

| Aspecto | Bronze | Silver | Gold |
|---------|--------|--------|------|
| **Função** | Ingestão fiel; armazenar exatamente o que a fonte entrega. | Limpeza, contrato de schema, validação, coorte válida. | Modelagem de negócio: star schema, KPIs, embeddings, clusters. |
| **Valor** | Rastreabilidade; reprocessamento sem perda; auditoria da origem. | Dados comparáveis entre anos; população bem definida; base confiável. | Decisão: dashboards, ML, clustering, assistente analítico. |
| **Artefatos** | Parquet raw por ano (ex.: `data/bronze/` ou raw); metadados (ano, ingest_ts, source_file). | Parquet canônico (`enem_participante`); `quality_report`; `null_report`. | `fato_desempenho`, `dim_*`, `kpis_uf_ano`, `distribuicoes_notas`, `cluster_*`, `participant_embeddings`. |
| **Quem consome** | Pipelines Silver; profiling e validação de contrato. | Pipelines Gold; modelos (quando necessário); relatórios de transparência. | Streamlit, LLM Analyst, modelos (treino/inferência), relatórios executivos. |
| **Transformação** | Nenhuma de negócio; no máximo metadados de ingestão. | Schema único, tipos, regras de presença/nota/códigos; filtro de coorte. | Joins, agregações, surrogate keys; métricas derivadas; ML/DL outputs. |

---

## 3. Como garantimos auditoria

- **Checksums / integridade da fonte:** Onde aplicável, checksums ou metadados dos arquivos ingeridos (ex.: nome do arquivo, ano, tamanho) são registrados na ingestão. Bronze não altera conteúdo; qualquer reprocessamento pode ser comparado à mesma fonte.
- **quality_report (Silver):** Relatório gerado pelo pipeline Silver (ex.: contagem de linhas por ano, % retido após filtros, métricas de qualidade por coluna). Permite auditar quantos registros foram removidos e por qual etapa (ex.: presença, faixa de nota).
- **null_report (Silver):** Relatório de nulidade por coluna (e por ano, se disponível). Documenta onde ainda há ausência de resposta (ex.: Q006) e apoia decisões de uso ou exclusão em análises e modelos.
- **Reprodutibilidade:** Contrato (schema canônico + mapeamento por ano) e pipelines versionados garantem que Bronze + mesmo código produzam o mesmo Silver e, em seguida, o mesmo Gold. Evidência: scripts em `pipelines/`, relatórios em `data/silver/` e `reports/`.

---

## 4. Slide version

- **Frase:** “Bronze guarda a verdade. Silver organiza a verdade. Gold transforma a verdade em decisão.”
- **Tabela:** 5 linhas (Função | Valor | Artefatos | Consumidor | Transformação) × 3 colunas (Bronze | Silver | Gold).
- **Auditoria:** Checksums/metadados na ingestão; quality_report e null_report no Silver; pipelines e contrato versionados para reprodutibilidade.
