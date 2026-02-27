# ENEM Radar — Streamlit dashboard

Dashboard do produto **ENEM Opportunity & Equity Radar (2020–2024)**. Lê Parquet de `data/gold` (e opcionalmente `data/silver`) via DuckDB.

## Como rodar

1. **Instalar dependências** (na raiz do repositório):
   ```bash
   pip install -r requirements.txt
   ```

2. **Gerar dados Gold** (se ainda não tiver):
   - Rodar pipelines Bronze → Silver → Gold (ex.: `pipelines/gold_star_schema.py`, scripts de coleta e Silver).
   - Para clusters e Radar completo: `ml/cluster_profiles.py` (e antes `ml/train_autoencoder_embeddings.py` se quiser embeddings).

3. **Subir o app**:
   ```bash
   streamlit run app/app.py
   ```
   Abra o URL que o Streamlit mostrar no terminal (geralmente http://localhost:8501).

   **Dashboard em uma página (relatório executivo):** sem sidebar, narrativa CRISP-DM + Lakehouse, mesmos dados:
   ```bash
   streamlit run app/app_one_page.py
   ```

## Páginas

- **1. Overview Nacional:** KPIs por ano, tendências, gráfico por UF (filtros: ano e UF).
- **2. Radar de Prioridade:** Score 0–100 por UF (pesos configuráveis), ranking top 10, nota de decisão.
- **3. Cohorts & Clusters:** Perfis por cluster e evolução 2020→2024 por UF (se existirem `cluster_profiles` e `cluster_evolution_uf_ano`).
- **4. Plano de Fidelidade:** Tiers Bronze/Silver/Gold por limiares de score e benefícios por tier.
- **5. LLM Analyst Bot:** Pergunta em texto; exibe SQL, tabela, interpretação e opcionalmente gráfico (allowlist Gold + Silver).

## Dados

- **DuckDB** registra como views tudo que existir em:
  - `data/gold/*` (fato_desempenho, dim_*, kpis_uf_ano, distribuicoes_notas, cluster_profiles, cluster_evolution_uf_ano)
  - `data/silver/quality_report`, `data/silver/null_report`
- Se alguma tabela Gold não existir, a página correspondente mostra mensagem “não gerado ainda” em vez de quebrar.

## Bot (página 5)

- Usa o mesmo fluxo de `assistant/app.py`: allowlist de tabelas, apenas SELECT, interpretação ancorada no resultado.
- Para perguntas fora das demos, defina `OPENAI_API_KEY`.
