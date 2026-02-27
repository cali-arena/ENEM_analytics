"""
ENEM Analyst Bot — Grounded LLM analytics assistant.

Receives a question → generates SQL (SELECT-only, allowlisted) → executes (DuckDB) →
returns SQL + result dataframe + interpretation (with ≥2 concrete numbers from result) + optional chart spec.
Never invents numbers. If no LLM key, uses template SQL for 3 demo questions only.

Demo questions and expected SQL shape (for fallback):
  Q1) "Quais UFs melhoraram mais em matemática de 2021 a 2024?"
      → SELECT uf, avg_mt_2021, avg_mt_2024, delta FROM (CTE by UF, ano) ORDER BY delta DESC LIMIT 15
  Q2) "Quais perfis (sexo + renda) têm maior participação no ENEM 2020–2024?"
      → SELECT tp_sexo, faixa_renda, COUNT(*), pct FROM gold_fato_desempenho JOIN gold_dim_perfil GROUP BY ... LIMIT 15
  Q3) "Top 10 UFs com maior crescimento de redação >= 800 de 2020 a 2024?"
      → SELECT uf, pct_2020, pct_2024, growth FROM (CTE redação>=800 by UF/year) ORDER BY growth DESC LIMIT 10
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]

from assistant.sql_guard import validate as validate_sql, MAX_LIMIT

# Parquet paths (same as config; views registered with underscore names)
DIR_GOLD = ROOT / "data" / "gold"
DIR_SILVER = ROOT / "data" / "silver"

VIEW_PATHS = {
    "gold_fato_desempenho": DIR_GOLD / "fato_desempenho",
    "gold_dim_tempo": DIR_GOLD / "dim_tempo",
    "gold_dim_geografia": DIR_GOLD / "dim_geografia",
    "gold_dim_perfil": DIR_GOLD / "dim_perfil",
    "gold_kpis_uf_ano": DIR_GOLD / "kpis_uf_ano",
    "gold_distribuicoes_notas": DIR_GOLD / "distribuicoes_notas",
    "gold_cluster_profiles": DIR_GOLD / "cluster_profiles",
    "gold_cluster_evolution_uf_ano": DIR_GOLD / "cluster_evolution_uf_ano",
    "silver_quality_report": DIR_SILVER / "quality_report",
    "silver_null_report": DIR_SILVER / "null_report",
}

# Demo questions (trigger substrings) → template SQL
DEMO_QUESTIONS = [
    "Quais UFs melhoraram mais em matemática de 2021 a 2024?",
    "Quais perfis (sexo + renda) têm maior participação no ENEM 2020–2024?",
    "Top 10 UFs com maior crescimento de redação >= 800 de 2020 a 2024?",
]

DEMO_SQL = {
    "Quais UFs melhoraram mais em matemática de 2021 a 2024?": """
WITH a AS (
  SELECT g.sg_uf_residencia AS uf, f.ano, AVG(f.mt) AS avg_mt
  FROM gold_fato_desempenho f
  JOIN gold_dim_geografia g ON f.id_geo = g.id_geo
  WHERE f.ano IN (2021, 2024) AND g.sg_uf_residencia != 'NA' AND f.mt IS NOT NULL
  GROUP BY g.sg_uf_residencia, f.ano
),
b AS (
  SELECT uf, MAX(CASE WHEN ano = 2021 THEN avg_mt END) AS a21, MAX(CASE WHEN ano = 2024 THEN avg_mt END) AS a24
  FROM a GROUP BY uf
)
SELECT uf, ROUND(a21, 2) AS avg_mt_2021, ROUND(a24, 2) AS avg_mt_2024, ROUND(a24 - a21, 2) AS delta
FROM b WHERE a21 IS NOT NULL AND a24 IS NOT NULL ORDER BY delta DESC LIMIT 15
""".strip(),
    "Quais perfis (sexo + renda) têm maior participação no ENEM 2020–2024?": """
SELECT p.tp_sexo AS sexo, p.faixa_renda AS renda, COUNT(*) AS participantes,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_cohort
FROM gold_fato_desempenho f
JOIN gold_dim_perfil p ON f.id_perfil = p.id_perfil
WHERE f.ano BETWEEN 2020 AND 2024
GROUP BY p.tp_sexo, p.faixa_renda
ORDER BY participantes DESC LIMIT 15
""".strip(),
    "Top 10 UFs com maior crescimento de redação >= 800 de 2020 a 2024?": """
WITH uf_year AS (
  SELECT g.sg_uf_residencia AS uf, f.ano, COUNT(*) AS n,
    SUM(CASE WHEN f.redacao >= 800 THEN 1 ELSE 0 END) AS n_800
  FROM gold_fato_desempenho f
  JOIN gold_dim_geografia g ON f.id_geo = g.id_geo
  WHERE g.sg_uf_residencia != 'NA' AND f.redacao IS NOT NULL AND f.ano IN (2020, 2024)
  GROUP BY g.sg_uf_residencia, f.ano
),
pct AS (
  SELECT uf, ano, 100.0 * n_800 / NULLIF(n, 0) AS pct_800 FROM uf_year
),
growth AS (
  SELECT uf, MAX(CASE WHEN ano = 2020 THEN pct_800 END) AS pct_2020,
    MAX(CASE WHEN ano = 2024 THEN pct_800 END) AS pct_2024,
    MAX(CASE WHEN ano = 2024 THEN pct_800 END) - MAX(CASE WHEN ano = 2020 THEN pct_800 END) AS growth_pct_pts
  FROM pct GROUP BY uf
)
SELECT uf, ROUND(pct_2020, 2) AS pct_800_2020, ROUND(pct_2024, 2) AS pct_800_2024, ROUND(growth_pct_pts, 2) AS growth_pct_pts
FROM growth WHERE pct_2020 IS NOT NULL AND pct_2024 IS NOT NULL ORDER BY growth_pct_pts DESC LIMIT 10
""".strip(),
}


def get_connection():
    """DuckDB in-memory connection with views named gold_*, silver_* (underscore convention)."""
    import duckdb
    con = duckdb.connect(database=":memory:")
    for view_name, path in VIEW_PATHS.items():
        if not path.exists():
            continue
        path_str = str(path / "**" / "*.parquet") if path.is_dir() else str(path)
        try:
            con.execute(
                "CREATE OR REPLACE VIEW {} AS SELECT * FROM read_parquet(?)".format(view_name),
                [path_str],
            )
        except Exception:
            try:
                con.execute(
                    "CREATE OR REPLACE VIEW {} AS SELECT * FROM read_parquet(?)".format(view_name),
                    [str(path)],
                )
            except Exception:
                pass
    return con


def _get_demo_sql(question: str) -> str | None:
    q = question.strip().lower()
    for dq in DEMO_QUESTIONS:
        if dq.lower() in q or q == dq.lower():
            return DEMO_SQL.get(dq)
    # Partial triggers
    if "uf" in q and "matemática" in q and ("2021" in q or "2024" in q):
        return DEMO_SQL.get(DEMO_QUESTIONS[0])
    if "perfil" in q and ("sexo" in q or "renda" in q) and ("participação" in q or "maior" in q):
        return DEMO_SQL.get(DEMO_QUESTIONS[1])
    if "redação" in q and "800" in q and ("crescimento" in q or "uf" in q):
        return DEMO_SQL.get(DEMO_QUESTIONS[2])
    return None


def _llm_generate(question: str) -> dict | None:
    """Call LLM; return {"sql", "chart", "interpretation_plan"} or None."""
    import os
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return None
    try:
        import openai
        client = openai.OpenAI(api_key=api_key)
        prompt_path = ROOT / "assistant" / "system_prompt.txt"
        system_content = prompt_path.read_text(encoding="utf-8") if prompt_path.exists() else ""
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_content},
                {"role": "user", "content": question},
            ],
            temperature=0,
        )
        text = response.choices[0].message.content.strip()
        if text.startswith("```"):
            text = re.sub(r"^```\w*\n?", "", text).rstrip("`")
        return json.loads(text)
    except Exception:
        return None


def _build_interpretation(df, interpretation_plan: str) -> str:
    """Build final interpretation from plan + result; must reference at least 2 concrete numbers."""
    import pandas as pd
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return (interpretation_plan or "No result.") + " (Query returned no rows.)"
    plan = (interpretation_plan or "Summary of the result.").strip()
    # Pick at least 2 concrete values from first row or from first two numeric columns
    nums = []
    for _, row in df.head(1).iterrows():
        for c in df.columns:
            v = row.get(c)
            if v is not None and isinstance(v, (int, float)) and not isinstance(v, bool):
                nums.append(f"{c}={v}")
            elif v is not None and isinstance(v, str) and len(str(v)) < 30:
                nums.append(f"{c}={v}")
    if len(nums) >= 2:
        return plan + " Result: " + "; ".join(nums[:4]) + "."
    # Fallback: first two columns
    cols = list(df.columns)[:3]
    vals = [str(df[c].iloc[0]) for c in cols if c in df.columns]
    return plan + " Values from result: " + ", ".join(f"{cols[i]}={vals[i]}" for i in range(min(2, len(vals)))) + "."


def run(
    question: str,
    con=None,
    out_plot_path: Path | None = None,
) -> dict[str, Any]:
    """
    Main entry: question → SQL (demo or LLM) → validate → execute → interpretation + chart spec.

    Returns dict with: sql, result_df, interpretation, chart (chart spec or null), error (if any).
    Interpretation is computed after the query and references at least 2 concrete numbers from the result.
    """
    result = {
        "sql": None,
        "result_df": None,
        "interpretation": None,
        "chart": None,
        "error": None,
    }
    if not question or not question.strip():
        result["error"] = "Empty question."
        return result

    sql = None
    chart_spec = None
    interpretation_plan = ""

    # 1) Get SQL: demo template or LLM
    sql = _get_demo_sql(question.strip())
    if sql is None:
        llm_out = _llm_generate(question.strip())
        if llm_out:
            sql = llm_out.get("sql")
            chart_spec = llm_out.get("chart")
            interpretation_plan = llm_out.get("interpretation_plan") or ""
            if sql is None and interpretation_plan:
                result["interpretation"] = interpretation_plan
                result["sql"] = None
                return result
        else:
            result["error"] = "No OPENAI_API_KEY and question does not match a demo. Use one of: " + "; ".join(DEMO_QUESTIONS)
            return result

    if not sql:
        result["error"] = "Could not generate SQL."
        return result

    # 2) Validate (guard)
    ok, final_sql = validate_sql(sql)
    if not ok:
        result["error"] = final_sql
        result["sql"] = sql
        return result
    result["sql"] = final_sql

    # 3) Execute
    if con is None:
        con = get_connection()
    try:
        df = con.execute(final_sql).fetchdf()
    except Exception as e:
        result["error"] = f"Query failed: {e}"
        return result

    result["result_df"] = df
    result["interpretation"] = _build_interpretation(df, interpretation_plan)
    result["chart"] = chart_spec

    # 4) Optional: write plot (caller can do it from chart spec + df)
    if out_plot_path and df is not None and not df.empty and chart_spec and chart_spec.get("type") not in (None, "none"):
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
            x_col = chart_spec.get("x")
            y_col = chart_spec.get("y")
            if x_col in df.columns and y_col in df.columns:
                fig, ax = plt.subplots()
                ax.bar(df[x_col].astype(str), df[y_col])
                ax.set_xlabel(x_col)
                ax.set_ylabel(y_col)
                ax.tick_params(axis="x", rotation=45)
                plt.tight_layout()
                out_plot_path.parent.mkdir(parents=True, exist_ok=True)
                plt.savefig(out_plot_path, dpi=120, bbox_inches="tight")
                plt.close()
        except Exception:
            pass

    return result
