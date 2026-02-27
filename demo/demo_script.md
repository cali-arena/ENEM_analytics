# ENEM Analyst Bot — Live demo script (2–3 minutes)

Use this as a presenter script: what to say, what to run or click, and rough timing.

---

## Before the demo (setup)

- Ensure Gold layer is built: `make gold` (or full pipeline: `make bronze`, `make silver`, `make gold`).
- Run smoke test: `python scripts/smoke_test.py` (exit 0).
- Optional: start Streamlit for the “in-app” path: `make app` and keep it open.

---

## Option A: Terminal demo (no browser)

**Total: ~2–3 min.**

1. **Intro (15 s)**  
   *“We have an LLM-backed analyst that answers questions over ENEM Gold data. It only returns SQL plus the real query result and an interpretation that cites numbers from the result—no invented figures.”*

2. **Run the script (10 s)**  
   From repo root:
   ```bash
   python demo/run_live_demo.py
   ```

3. **Q1 — UFs que mais melhoraram em matemática (45 s)**  
   - Point to the first question in the output.  
   *“First question: which UFs improved most in math from 2021 to 2024?”*  
   - Show the generated SQL (CTE by UF and year, then delta).  
   - Show the result table (top rows).  
   *“The interpretation at the bottom is built after running the query and references at least two concrete numbers from this table.”*

4. **Q2 — Perfis com maior participação (45 s)**  
   - Scroll to the second block.  
   *“Second: which profiles—sex and income band—have the highest participation in ENEM 2020–2024?”*  
   - Briefly show SQL (fact + dim_perfil, GROUP BY) and the table.  
   *“Again, the text only uses numbers that appear in this result.”*

5. **Q3 — Top UFs em crescimento de redação ≥ 800 (45 s)**  
   - Scroll to the third block.  
   *“Third: top 10 UFs by growth in share of redação ≥ 800 from 2020 to 2024.”*  
   - Show SQL (CTE with redação >= 800 by UF/year, then growth) and the table.  
   *“The bot is restricted to allowlisted tables and SELECT-only; it never invents numbers.”*

6. **Wrap (10 s)**  
   *“The same bot is available in the Streamlit app under ‘LLM Analyst Bot’ for ad-hoc questions.”*

---

## Option B: Streamlit app demo

**Total: ~2–3 min.**

1. **Open app (already running)**  
   *“Dashboard is already running. I’ll use the LLM Analyst Bot page.”*

2. **Go to “LLM Analyst Bot” (sidebar)**  
   Click **LLM Analyst Bot** in the sidebar.

3. **Paste Q1 and run (30 s)**  
   - Paste: *“Quais UFs melhoraram mais em matemática de 2021 a 2024?”*  
   - Click Run (or Submit).  
   - Show SQL, result table, and interpretation.  
   *“Interpretation is computed from the result; it always references at least two numbers from the table.”*

4. **Second question (30 s)**  
   - Paste: *“Quais perfis (sexo + renda) têm maior participação no ENEM 2020–2024?”*  
   - Run. Show SQL + table + interpretation.

5. **Third question (30 s)**  
   - Paste: *“Top 10 UFs com maior crescimento de redação >= 800 de 2020 a 2024?”*  
   - Run. Show result and interpretation.

6. **Closing (15 s)**  
   *“All of this runs locally: DuckDB over Gold Parquet, optional LLM for open questions, and a strict SQL guard so we only run allowlisted, read-only queries.”*

---

## Timing summary

| Step              | Option A (terminal) | Option B (Streamlit) |
|-------------------|---------------------|----------------------|
| Intro             | ~15 s               | —                    |
| Run script / nav  | ~10 s               | ~15 s                |
| Q1                | ~45 s               | ~30 s                |
| Q2                | ~45 s               | ~30 s                |
| Q3                | ~45 s               | ~30 s                |
| Wrap              | ~10 s               | ~15 s                |
| **Total**         | **~2.5 min**        | **~2–2.5 min**       |

---

## If something fails

- **No result / error:** Confirm Gold exists (`python scripts/smoke_test.py`). If the bot uses the new module, ensure `assistant.bot` and DuckDB with underscore-view names are used (see README / assistant docs).
- **Slow run:** First run may load Parquet into DuckDB; subsequent queries are fast.
- **Demo mode:** With no LLM key, the bot uses template SQL for these three questions only; behavior is the same for the demo.
