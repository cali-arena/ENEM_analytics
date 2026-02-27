# ENEM Opportunity & Equity Radar (2020–2024) — Slide Narrative (Final)

**Use this text in Canva/PPT. Executive + technical credibility; no fluff.**

---

## Slide 1 — Title + One-liner

**Title:** ENEM Opportunity & Equity Radar (2020–2024)

**One-liner:**  
De cinco anos de microdados ENEM a uma plataforma de decisão: Lakehouse (Bronze/Silver/Gold), ML temporal, mapa de perfis e assistente analítico com IA—tudo ancorado em dados reais, sem números inventados.

---

## Slide 2 — Problem → User → Value

**Problema (3 bullets)**  
- Desigualdade educacional e tendências de desempenho não são observáveis em escala em um único quadro comparável.  
- Microdados ENEM são públicos mas fragmentados por ano, schema e formato; não há visão multi-ano padronizada.  
- Decisões tornam-se reativas e mal direcionadas: instituições, EdTechs e secretarias não conseguem responder de forma confiável “onde o desempenho cresceu ou caiu?” ou “quais perfis têm maior risco?”.

**Usuário (3 bullets)**  
- EdTechs e cursinhos: onde priorizar oferta, quais regiões e perfis atacar, quais disciplinas reforçar.  
- Universidades (públicas e privadas): onde recrutar, como calibrar cortes e bolsas, quais perfis apoiar.  
- Secretarias de educação e analistas de política: onde alocar recursos, quais áreas precisam de intervenção, como medir equidade.

**Valor (3 business questions)**  
- **Onde o desempenho mais cresceu ou caiu por UF/município?** → Geografia da melhoria e do declínio para targeting e política.  
- **Quais perfis de candidato têm maior risco de ausência ou baixo desempenho?** → Segmentação para apoio e comunicação.  
- **Quais áreas de conhecimento mostram gaps de desempenho crescentes?** → Detecção de desigualdade por disciplina (ex.: matemática vs humanas).

---

## Slide 3 — Architecture (Lakehouse + ML + LLM + Dashboard)

**Diagrama (ASCII)**

```
     CSV (INEP)        Bronze         Silver          Gold           Produto
     ──────────►     ───────►      ───────►       ───────►       ───────────
     Raw/ano         Parquet        Parquet         Star+KPIs      Streamlit
     scripts/        imutável       enem_partici-   fato_desem-     + LLM
     01_coleta       data/bronze    pante +         penho, dim_*,   Analyst Bot
                                    quality_report  kpis_uf_ano     (DuckDB,
                                         │          distribuicoes   allowlist)
                                         │               │
                                         │          ┌────┴────┐
                                         │          │ ML / DL │
                                         │          │ train_  │
                                         │          │ ml_*    │
                                         │          │ cluster_│
                                         │          │ profiles│
                                         │          └─────────┘
```

**Stack**  
- **Ingestão:** Scripts de coleta (CSV INEP por ano).  
- **Lakehouse:** Bronze (Parquet raw) → Silver (contrato, validação, coorte válida) → Gold (star schema, KPIs, distribuições).  
- **Processamento:** Spark (pipelines); Parquet em todas as camadas.  
- **ML:** Treino temporal (2020–2023, teste 2024); autoencoder (embeddings); clustering (perfis); entrada e saída em Gold.  
- **Consulta:** DuckDB (ou SparkSQL) sobre Gold; LLM Analyst com SQL em allowlist, interpretação só a partir do resultado.  
- **Produto:** Dashboard Streamlit (Overview, Radar, Clusters, Fidelidade, LLM Analyst Bot).

---

## Slide 4 — Bronze vs Silver vs Gold

**Frase:**  
*“Bronze guarda a verdade. Silver organiza a verdade. Gold transforma a verdade em decisão.”*

| Camada  | Função | Valor | Artefatos |
|---------|--------|-------|-----------|
| **Bronze** | Ingestão fiel; armazenar exatamente o que a fonte entrega. | Rastreabilidade; reprocessamento sem perda; auditoria da origem. | Parquet por ano (`data/bronze/enem_*.parquet`); metadados (ano, ingest_ts, source_file). |
| **Silver** | Limpeza, contrato de schema, validação (presença, notas, códigos), coorte válida. | Dados comparáveis entre anos; população bem definida; base confiável para analytics. | Parquet canônico (`enem_participante`); `quality_report`; `null_report`. |
| **Gold** | Modelagem de negócio: star schema, KPIs, embeddings, clusters. | Decisão: dashboards, ML, clustering, assistente analítico. | `fato_desempenho`, `dim_*`, `kpis_uf_ano`, `distribuicoes_notas`, `cluster_*`, `participant_embeddings`. |

---

## Slide 5 — Gold Star Schema

**Conteúdo:**  
- **Fact:** `fato_desempenho` — grain participante × ano; chaves surrogadas `id_tempo`, `id_geo`, `id_perfil`; notas (cn, ch, lc, mt, redação), media_objetiva, flags de presença.  
- **Dims:** `dim_tempo` (ano, ciclo pré/pós pandemia), `dim_geografia` (UF), `dim_perfil` (sexo, faixa renda).  
- **KPI/agregados:** `kpis_uf_ano` (count, médias, % top 800 redação, % presença plena); `distribuicoes_notas` (percentis por ano+UF).

**Diagrama Star Schema (ASCII)**

```
                    dim_tempo (id_tempo, ano, ciclo)
                           │
                           │ id_tempo
                           │
    dim_geografia ────────►│◄──────── dim_perfil
    (id_geo, sg_uf)       │         (id_perfil, tp_sexo, faixa_renda)
         id_geo           │                id_perfil
                           │
                    ┌──────▼──────┐
                    │   FATO_     │
                    │ DESEMPENHO  │
                    │ (notas,     │
                    │  media_obj) │
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
       kpis_uf_ano   distribuicoes   (cluster_*,
       (agregado)     _notas          embeddings)
```

---

## Slide 6 — Evaluation Insights (6 visuals)

Cada visual com **título** e **uma linha de insight acionável**.

1. **Distribution of Objective Mean Score (2020–2024)**  
   *Insight:* Se a distribuição subiu após 2021, recalibrar limiares de “bom desempenho” e comunicação; se a variância aumentou, direcionar apoio onde a cauda esquerda engrossou (por UF ou perfil).

2. **Redação Score Distribution (% ≥800)**  
   *Insight:* Se % ≥800 cresceu, reforçar redação em UFs ou perfis que não evoluíram; se caiu, priorizar escrita e feedback nos clusters de menor desempenho.

3. **Presence Rate by Year**  
   *Insight:* Calcular presença a partir do Silver por ano e perfil; se 2020–2021 mostram menor presença, direcionar campanhas de comparecimento em crises futuras e reportar “coorte válida” vs “todos inscritos” nos dashboards.

4. **Average Score by UF (Top 10 Growth 2020 vs 2024)**  
   *Insight:* Priorizar reforço de matemática e áreas objetivas em UFs que ainda rankeiam baixo apesar do crescimento; replicar práticas das UFs de maior crescimento em estados vizinhos.

5. **Gap by Income (Q006: Lowest vs Highest) Over Time**  
   *Insight:* Se o gap aumentou, direcionar bolsas e pré-vestibular para clusters de baixa renda (ex.: Q006 A–C) e monitorar o gap no próximo ciclo; se diminuiu, documentar e escalar políticas associadas a esse período.

6. **Performance by Gender (Mean Objective Score) Over Years**  
   *Insight:* Se um grupo fica para trás ou o gap cresce, priorizar apoio (conteúdo, mentoria, bolsas) para o grupo em desvantagem e vincular ações à área onde o gap é maior (ex.: matemática vs linguagens).

---

## Slide 7 — ML Market-grade

**Temporal split**  
- Treino: 2020–2023. Teste: 2024.  
- Evita vazamento de futuro; simula uso em produção (modelo treinado no passado, aplicado ao ano seguinte).  
- Alinha com o ciclo real de decisão (dados históricos → previsão para o próximo ENEM).

**Métricas (placeholders)**  
- **Baseline (Ridge):** RMSE = [X], R² = [Y], MAE = [Z].  
- **Strong (GBT):** RMSE = [X], R² = [Y], MAE = [Z].  
- **Validação temporal 2024:** Métricas reportadas em `reports/ml_metrics.json`; previsões em `reports/predictions_2024.parquet`.

**Top 5 features (placeholders)**  
1. [Ex.: media_objetiva ou nota agregada prévia]  
2. [Ex.: faixa_renda / Q006]  
3. [Ex.: id_geo / UF]  
4. [Ex.: ano ou ciclo_pre_pos_pandemia]  
5. [Ex.: tp_sexo ou outra variável de perfil]

*(Preencher com saída de explainability_report / top_5_features.json.)*

---

## Slide 8 — DL Product: Mapa de Perfis

**Conceito**  
- **Embeddings:** Autoencoder tabular sobre Gold → vetores por participante (`participant_embeddings.parquet`).  
- **Clusters:** KMeans sobre embeddings (treino 2020–2023, aplicação em 2024) → perfis interpretáveis em `cluster_profiles`.  
- **Evolução por UF/ano:** `cluster_evolution_uf_ano` — participação (%) de cada cluster por UF e ano; permite ver onde cada perfil cresceu ou diminuiu.

**Exemplo de insight (uma linha)**  
*“O Cluster [X] (ex.: baixa renda, alta presença, gap em matemática) aumentou sua participação na UF [Y] em [Z] pontos percentuais entre 2020 e 2024 (de A% para B%). Isso sugere maior demanda por reforço direcionado em matemática nesse estado, alinhado ao perfil do cluster.”*

---

## Slide 9 — Streamlit Product Pages

- **1. Overview Nacional:** KPIs nacionais por ano e UF (média objetiva, redação, participantes, opcional presença); filtros ano e UF; tendências e comparativos.  
- **2. Radar de Prioridade:** Score composto por UF (ex.: desempenho + crescimento + equidade); priorização visual para onde atuar.  
- **3. Cohorts & Clusters:** Visualização dos perfis de cluster (cluster_profiles) e evolução por UF/ano (cluster_evolution_uf_ano).  
- **4. Plano de Fidelidade:** Tiers de desempenho (ex.: bronze/silver/gold por faixas de nota); uso para segmentação e comunicação.  
- **5. LLM Analyst Bot:** Pergunta em linguagem natural → SQL (SELECT, allowlist) → execução DuckDB sobre Gold → tabela + interpretação ancorada em pelo menos dois números do resultado; sem números inventados.

---

## Slide 10 — Live Demo Script (2–3 minutes)

**Passo a passo: o que mostrar, o que dizer, tempo.**

| # | Ação | O que dizer | Tempo |
|---|------|-------------|-------|
| 1 | Abrir app ou terminal; ir ao “LLM Analyst Bot”. | “Temos um analista com IA que responde perguntas sobre os dados Gold. Só devolve SQL, resultado real e interpretação que cita números do resultado—nada inventado.” | ~15 s |
| 2 | Rodar `python demo/run_live_demo.py` OU colar a primeira pergunta no bot e dar Run. | “Primeira pergunta: quais UFs mais melhoraram em matemática de 2021 a 2024?” | ~10 s |
| 3 | Mostrar SQL gerado (CTE por UF/ano, delta), tabela e interpretação. | “O SQL usa apenas tabelas permitidas. A interpretação é construída depois de rodar a query e referencia pelo menos dois números desta tabela.” | ~45 s |
| 4 | Segunda pergunta: perfis (sexo + renda) com maior participação 2020–2024. Mostrar SQL (fact + dim_perfil), tabela e interpretação. | “Segunda: quais perfis têm maior participação no ENEM? De novo, o texto só usa números que aparecem no resultado.” | ~45 s |
| 5 | Terceira pergunta: top 10 UFs em crescimento de redação ≥800 (2020→2024). Mostrar SQL, tabela e interpretação. | “Terceira: top 10 UFs em crescimento da proporção com redação ≥800. O bot está restrito a SELECT e tabelas allowlisted; nunca inventa números.” | ~45 s |
| 6 | Fechar. | “O mesmo bot está no dashboard em ‘LLM Analyst Bot’ para perguntas ad hoc. Tudo roda local: DuckDB sobre Gold, guard de SQL estrito.” | ~10 s |

**Total:** ~2,5 min.

---

## Slide 11 — Impact / Next Steps

**Quem usa**  
- EdTechs e cursinhos (onde atuar, quais disciplinas e perfis).  
- Universidades (recrutamento, cortes, bolsas).  
- Secretarias de educação (priorização de recursos, intervenções por área e perfil).  
- Analistas de política (medir equidade, gaps por renda/UF/gênero, simulação de políticas).

**Como muda decisões**  
- Decisões baseadas em KPIs reproduzíveis a partir do Gold (e Silver quando aplicável).  
- Geografia e perfil explícitos: crescimento/declínio por UF, risco por sexo/renda, evolução de clusters por estado.  
- Assistente analítico que responde em SQL + resultado + interpretação ancorada em dados, sem alucinação de números.

**O que vem a seguir**  
- Inclusão de mais anos (ex.: 2025) e atualização dos pipelines e do contrato.  
- Expansão do Gold (ex.: município quando disponível no Silver).  
- Operacionalização: agendamento de pipelines, API ou export para sistemas de BI; monitoramento de qualidade (Silver/Gold).  
- Uso do Mapa de Perfis e da evolução por UF em campanhas e políticas (ex.: “cluster X cresceu na UF Y” → reforço direcionado).

---

## Slide 12 — Final Impact Phrase

**Frase de fechamento:**

*“Não estamos apenas analisando o ENEM. Estamos criando um sistema de alocação educacional baseado em dados reais, com inteligência artificial aplicada.”*

---

*Fim do narrative. Copie e cole cada bloco no slide correspondente no Canva/PPT.*
