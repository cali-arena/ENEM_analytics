# ENEM Analyst Bot

Grounded analytics assistant: answers business questions using **only** Gold (and optional Silver) tables. Every answer includes evidence from an executed query.

## Run locally

1. **Install**
   ```bash
   pip install duckdb openai
   ```
   (From repo root you can `pip install -r requirements.txt` and add `duckdb` if not present.)

2. **Data**
   Ensure Gold (and optionally Silver) Parquet exists:
   - `data/gold/fato_desempenho`, `dim_tempo`, `dim_geografia`, `dim_perfil`, `kpis_uf_ano`, `distribuicoes_notas`
   - Optional: `data/gold/cluster_profiles`, `cluster_evolution_uf_ano`
   - Optional: `data/silver/quality_report`, `null_report`

3. **Run with a question**
   ```bash
   python assistant/app.py "Biggest UF improvements in math 2021→2024"
   ```

4. **Interactive (demo questions)**
   ```bash
   python assistant/app.py
   ```
   Then type one of the 3 demo questions or your own. For custom questions you need `OPENAI_API_KEY` set.

5. **Optional: LLM for any question**
   ```bash
   set OPENAI_API_KEY=sk-...
   python assistant/app.py "Which UF had the highest average redação in 2024?"
   ```

## Output

- **Generated SQL** — Allowlisted tables only; SELECT only; LIMIT 200 enforced.
- **Query Result** — Table (from DuckDB).
- **Interpretation** — Text grounded in the result (references actual values).
- **Plot** — Optional; saved to `figures/assistant_result.png` when the result is small and chart_suggestion is returned.

## Demo questions (no API key)

1. Biggest UF improvements in math 2021→2024  
2. Profiles with highest absence risk 2020→2024  
3. Top UF growth in redação >= 800 (2020→2024)

## Safety

- Only allowlisted tables: `gold.fato_desempenho`, `gold.dim_*`, `gold.kpis_uf_ano`, `gold.distribuicoes_notas`, `gold.cluster_*`, `silver.quality_report`, `silver.null_report`.
- DDL/DML rejected (no CREATE, DROP, INSERT, etc.).
- Row output capped at 200.
