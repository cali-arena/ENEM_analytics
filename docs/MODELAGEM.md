# Modelagem — ENEM Opportunity & Equity Radar (2020–2024)

**Gold star schema, validação temporal em ML, produto de clusters (“Mapa de Perfis”) e formato de saída do LLM Analyst.**

---

## 1. Gold: star schema e tabelas principais

- **Layout:** Uma tabela de fato no centro (grain: participante × ano) e dimensões ligadas por chaves substitutas (surrogate keys). Agregados (KPIs, distribuições) são derivados da fato para consumo em dashboards e relatórios.
- **Fato:** `gold.fato_desempenho` — uma linha por (nu_inscricao, ano) com: id_tempo, id_geo, id_perfil; notas (cn, ch, lc, mt, redacao, media_objetiva); flags de presença (presenca_cn, presenca_ch, presenca_lc, presenca_mt).
- **Dimensões:**  
  - `gold.dim_tempo`: id_tempo, ano, ciclo_pre_pos_pandemia.  
  - `gold.dim_geografia`: id_geo, sg_uf_residencia.  
  - `gold.dim_perfil`: id_perfil, tp_sexo, faixa_renda (Q006).
- **Agregados:**  
  - `gold.kpis_uf_ano`: por ano e UF — count_participantes, media_redacao, media_objetiva, percentis, pct_top800_redacao, pct_presence_full.  
  - `gold.distribuicoes_notas`: distribuições e percentis por ano e UF.  
  - Opcionais: `gold.cluster_profiles`, `gold.cluster_evolution_uf_ano`, `gold.participant_embeddings` (produto de ML/DL).

---

## 2. ML: validação temporal (train 2020–2023, test 2024)

- **Rationale:** Em produção o modelo será usado para anos futuros (ex.: prever 2025 com dados até 2024). Treinar com 2020–2023 e avaliar em 2024 simula esse cenário e evita vazamento de informação futura.
- **Implementação:** Split temporal fixo; métricas (RMSE, MAE, R²) e análise de erro reportadas no ano de teste (2024). Features vêm apenas de Gold; nenhuma variável derivada do target.
- **Artefatos:** Modelos em `models/`; métricas e previsões em `reports/`; análise por grupo em `ml/error_analysis_by_group.ipynb`.

---

## 3. Produto de clusters: “Mapa de Perfis do ENEM”

- **Conceito:** Agrupar participantes em perfis a partir de embeddings (autoencoder sobre Gold); clusters estáveis para segmentação e acompanhamento.
- **Entrada:** `gold.participant_embeddings` (nu_inscricao, ano, embedding_vector).
- **Processo:** KMeans (k escolhido por silhouette) ajustado em 2020–2023; aplicado a 2024. Cada participante recebe cluster_id.
- **Saídas:**  
  - `gold.cluster_profiles`: por cluster — size, media_redacao, media_obj, renda_media, presence_rate, top_3_ufs.  
  - `gold.cluster_evolution_uf_ano`: por ano e UF — % de participantes em cada cluster.  
- **Uso:** Identificação de perfis típicos, evolução regional e por ano, suporte a ações segmentadas (ex.: reforço em matemática onde um perfil cresceu).

---

## 4. LLM Analyst: formato de saída obrigatório

O assistente analítico deve retornar **apenas** com evidência executada: SQL, resultado da query e interpretação ancorada nesse resultado. Formato esperado:

| Elemento | Obrigatório | Descrição |
|----------|-------------|-----------|
| **SQL** | Sim (ou null se pergunta não puder ser atendida) | Uma única instrução SELECT contra tabelas da allowlist (gold.*, silver.quality_report, silver.null_report). Sem DDL/DML; LIMIT aplicado (ex.: 200). |
| **Query Result** | Sim (tabela) | Resultado da execução do SQL (DuckDB ou SparkSQL). Exibido como tabela; sem números inventados. |
| **Interpretation** | Sim | Texto que descreve o que o usuário aprende com o resultado; deve citar valores ou resumos realmente presentes na tabela (ex.: “UF X teve o maior delta; valor Y”). Nunca inventar métricas. |
| **Plot** | Opcional | Gráfico sugerido (ex.: bar, line) com colunas x/y; gerado só quando o resultado for pequeno e adequado para visualização. |

O bot nunca responde sem executar uma query; se a pergunta não for respondível com as tabelas permitidas, retorna sql=null e explica em interpretation_plan (ex.: “Município não está na allowlist; só UF está disponível”).

---

## 5. Slide version

- **Gold:** Star schema — fato (desempenho) + dim_tempo, dim_geo, dim_perfil; KPIs e distribuições; opcionalmente clusters e embeddings.
- **ML:** Validação temporal = train 2020–2023, test 2024; evita vazamento e alinha com uso em produção.
- **Clusters:** “Mapa de Perfis do ENEM” — embeddings → KMeans → cluster_profiles + cluster_evolution_uf_ano.
- **LLM:** Saída = SQL + tabela + interpretação (sempre baseada no resultado); plot opcional; sem alucinação.
