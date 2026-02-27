"""
ENEM Analyst Bot — Grounded analytics assistant (CRISP-DM Deployment).

Answers business questions using ONLY Gold (and optional Silver quality) tables.
Every answer includes: Generated SQL, Query Result (table), Interpretation (grounded in result).
No hallucinated numbers; evidence from executed query only.

Run locally:
  pip install duckdb openai
  set OPENAI_API_KEY=...   (optional; without it, only demo questions use pre-written SQL)
  python assistant/app.py "Your question here"
  python assistant/app.py   (interactive mode with 3 demo questions)

Data: Registers Parquet from data/gold/* and data/silver/quality_report*, null_report*.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

# -----------------------------------------------------------------------------
# Paths and config
# -----------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from assistant.config import (
    ALLOWED_TABLES,
    TABLE_PATHS,
    MAX_ROWS,
    FORBIDDEN_SQL,
    DIR_GOLD,
    DIR_SILVER,
)

# -----------------------------------------------------------------------------
# Demo questions → pre-written SQL (no LLM needed for demo)
# -----------------------------------------------------------------------------
DEMO_QUESTIONS = [
    "Biggest UF improvements in math 2021→2024",
    "Profiles with highest absence risk 2020→2024",
    "Top UF growth in redação >= 800 (2020→2024)",
    "Quais UFs melhoraram mais em matemática de 2021 a 2024?",
    "Quais perfis (sexo + renda) têm maior risco de ausência no ENEM pós-2020?",
    "Mostre top 10 UFs com maior crescimento de redação >= 800 de 2020 a 2024.",
]

DEMO_SQL = {
    "Biggest UF improvements in math 2021→2024": """
WITH avg_by_uf AS (
  SELECT
    g.sg_uf_residencia AS uf,
    AVG(CASE WHEN f.ano = 2021 THEN f.mt END) AS a21,
    AVG(CASE WHEN f.ano = 2024 THEN f.mt END) AS a24
  FROM gold.fato_desempenho f
  JOIN gold.dim_geografia g ON f.id_geo = g.id_geo
  WHERE f.ano IN (2021, 2024) AND g.sg_uf_residencia != 'NA' AND f.mt IS NOT NULL
  GROUP BY g.sg_uf_residencia
)
SELECT uf, ROUND(a21, 2) AS avg_mt_2021, ROUND(a24, 2) AS avg_mt_2024, ROUND(a24 - a21, 2) AS delta
FROM avg_by_uf WHERE a21 IS NOT NULL AND a24 IS NOT NULL
ORDER BY delta DESC
LIMIT 15
""".strip(),
    "Profiles with highest absence risk 2020→2024": """
SELECT
  p.tp_sexo AS sexo,
  p.faixa_renda AS renda,
  COUNT(*) AS participantes,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_cohort
FROM gold.fato_desempenho f
JOIN gold.dim_perfil p ON f.id_perfil = p.id_perfil
WHERE f.ano BETWEEN 2020 AND 2024
GROUP BY p.tp_sexo, p.faixa_renda
ORDER BY participantes DESC
LIMIT 15
""".strip(),
    "Top UF growth in redação >= 800 (2020→2024)": """
WITH uf_year AS (
  SELECT
    g.sg_uf_residencia AS uf,
    f.ano,
    COUNT(*) AS n,
    SUM(CASE WHEN f.redacao >= 800 THEN 1 ELSE 0 END) AS n_800
  FROM gold.fato_desempenho f
  JOIN gold.dim_geografia g ON f.id_geo = g.id_geo
  WHERE g.sg_uf_residencia != 'NA' AND f.redacao IS NOT NULL AND f.ano IN (2020, 2024)
  GROUP BY g.sg_uf_residencia, f.ano
),
pct AS (
  SELECT uf, ano, 100.0 * n_800 / NULLIF(n, 0) AS pct_800 FROM uf_year
),
growth AS (
  SELECT
    uf,
    MAX(CASE WHEN ano = 2020 THEN pct_800 END) AS pct_2020,
    MAX(CASE WHEN ano = 2024 THEN pct_800 END) AS pct_2024,
    MAX(CASE WHEN ano = 2024 THEN pct_800 END) - MAX(CASE WHEN ano = 2020 THEN pct_800 END) AS growth_pct_pts
  FROM pct GROUP BY uf
)
SELECT uf, ROUND(pct_2020, 2) AS pct_800_2020, ROUND(pct_2024, 2) AS pct_800_2024, ROUND(growth_pct_pts, 2) AS growth_pct_pts
FROM growth
WHERE pct_2020 IS NOT NULL AND pct_2024 IS NOT NULL
ORDER BY growth_pct_pts DESC
LIMIT 15
""".strip(),
}
# Portuguese demo triggers → same SQL as English
for _pt, _en in [
    ("Quais UFs melhoraram mais em matemática de 2021 a 2024?", "Biggest UF improvements in math 2021→2024"),
    ("Quais perfis (sexo + renda) têm maior risco de ausência no ENEM pós-2020?", "Profiles with highest absence risk 2020→2024"),
    ("Mostre top 10 UFs com maior crescimento de redação >= 800 de 2020 a 2024.", "Top UF growth in redação >= 800 (2020→2024)"),
]:
    DEMO_SQL[_pt] = DEMO_SQL[_en]


def normalize_table_ref(s: str) -> str:
    """Lowercase and strip; for comparison."""
    return s.strip().lower()


def validate_sql(sql: str) -> tuple[bool, str]:
    """
    Enforce: only SELECT; no DDL/DML; only allowlisted tables; add LIMIT if missing.
    Returns (ok, final_sql or error_message).
    """
    raw = sql.strip()
    upper = raw.upper()
    for kw in FORBIDDEN_SQL:
        if kw in upper:
            return False, f"Rejected: SQL must not contain {kw}"
    # Allow standard SELECT and CTEs that start with WITH
    if not (upper.startswith("SELECT") or upper.startswith("WITH")):
        return False, "Rejected: Only SELECT queries are allowed."
    # Discover table references (FROM and JOIN ... ON)
    # Simple: find tokens like gold.xxx or silver.xxx
    refs = re.findall(r"(?:FROM|JOIN)\s+(\w+\.\w+)", raw, re.IGNORECASE)
    refs = [normalize_table_ref(r) for r in refs]
    allowed_set = {normalize_table_ref(t) for t in ALLOWED_TABLES}
    for r in refs:
        if r not in allowed_set:
            return False, f"Rejected: Table {r} is not in the allowlist."
    if "LIMIT" not in upper:
        raw = raw.rstrip("; \n") + f"\nLIMIT {MAX_ROWS}"
    else:
        # Cap existing LIMIT
        raw = re.sub(r"\bLIMIT\s+\d+", f"LIMIT {MAX_ROWS}", raw, flags=re.IGNORECASE)
    return True, raw


def init_duckdb():
    """Create DuckDB con and register Gold + Silver Parquet as views (if paths exist)."""
    import duckdb
    con = duckdb.connect(database=":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    for full_name, path in TABLE_PATHS.items():
        if not path.exists():
            continue
        path_str = str(path / "**" / "*.parquet") if path.is_dir() else str(path)
        try:
            con.execute("CREATE OR REPLACE VIEW {} AS SELECT * FROM read_parquet(?)".format(full_name), [path_str])
        except Exception:
            con.execute("CREATE OR REPLACE VIEW {} AS SELECT * FROM read_parquet(?)".format(full_name), [str(path)])
    return con


def execute_sql(con, sql: str):
    """Execute and return pandas DataFrame."""
    return con.execute(sql).fetchdf()


def _load_system_prompt() -> str:
    path = ROOT / "assistant" / "system_prompt.txt"
    if path.exists():
        return path.read_text(encoding="utf-8")
    return "Reply with a single JSON: {\"sql\": \"SELECT ...\", \"chart\": null or {\"type\":\"bar\"|\"line\", \"x\":\"col\", \"y\":\"col\"}, \"interpretation_plan\": \"...\"}. Only SELECT, allowlisted tables, LIMIT required."


def llm_get_sql(question: str) -> dict | None:
    """
    Call LLM to produce { "sql": "..." | null, "chart": {...} | null, "interpretation_plan": "..." }.
    Normalized to chart_suggestion + interpretation_guidance for downstream. Returns None if no API key or error.
    """
    import os
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return None
    try:
        import openai
        client = openai.OpenAI(api_key=api_key)
        system_content = _load_system_prompt()
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
        out = json.loads(text)
        # Normalize to keys used by run_bot
        out["chart_suggestion"] = out.get("chart")
        out["interpretation_guidance"] = out.get("interpretation_plan") or ""
        return out
    except Exception:
        return None


def format_table(df):
    """Return string table for console."""
    if df is None or df.empty:
        return "(No rows)"
    return df.to_string(index=False, max_rows=MAX_ROWS)


def write_interpretation(df, guidance: str) -> str:
    """Produce interpretation text grounded in the result (reference actual values)."""
    if df is None or df.empty:
        return "Query returned no rows. " + (guidance or "")
    lines = [guidance or "Summary of the query result:"]
    # Reference first row or key stats
    if len(df) == 1:
        row = df.iloc[0]
        lines.append("Single row: " + ", ".join(f"{k}={v}" for k, v in row.items()))
    else:
        cols = list(df.columns)
        lines.append(f"Result has {len(df)} rows and columns: {cols}.")
        if len(df) > 0 and len(cols) >= 2:
            first = df.iloc[0]
            lines.append(f"Top row: {cols[0]}={first.iloc[0]}, {cols[1]}={first.iloc[1]} (see full table above).")
    return " ".join(lines)


def maybe_plot(df, chart_suggestion: dict | None, out_path: Path | None):
    """If chart_suggestion and df is small and numeric, save a plot."""
    if not chart_suggestion or df is None or df.empty or len(df) > 50:
        return
    try:
        import matplotlib.pyplot as plt
        x_col = chart_suggestion.get("x")
        y_col = chart_suggestion.get("y")
        kind = (chart_suggestion.get("type") or "bar").lower()
        if x_col not in df.columns or y_col not in df.columns:
            return
        fig, ax = plt.subplots()
        if kind == "bar":
            ax.bar(df[x_col].astype(str), df[y_col])
        else:
            ax.plot(df[x_col].astype(str), df[y_col], marker="o")
        ax.set_xlabel(x_col)
        ax.set_ylabel(y_col)
        ax.tick_params(axis="x", rotation=45)
        plt.tight_layout()
        if out_path:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            plt.savefig(out_path, dpi=120, bbox_inches="tight")
        plt.close()
    except Exception:
        pass


def run_bot(question: str, con, out_plot_path: Path | None = None) -> dict:
    """
    Full flow: get SQL (LLM or demo) → validate → execute → table + interpretation + optional plot.
    Returns dict with keys: sql, result_df, table_str, interpretation, plot_path, error.
    """
    result = {"sql": None, "result_df": None, "table_str": None, "interpretation": None, "plot_path": None, "error": None}
    # 1) Get SQL
    sql = None
    chart_suggestion = None
    interpretation_guidance = ""
    for dq in DEMO_QUESTIONS:
        if dq.lower() in question.lower() or question.strip().lower() == dq.lower():
            sql = DEMO_SQL.get(dq)
            break
    if not sql:
        llm_out = llm_get_sql(question)
        if llm_out:
            sql = llm_out.get("sql")
            chart_suggestion = llm_out.get("chart_suggestion")
            interpretation_guidance = llm_out.get("interpretation_guidance") or ""
            if sql is None and interpretation_guidance:
                result["sql"] = None
                result["table_str"] = "(No query; question not answerable with available tables.)"
                result["interpretation"] = interpretation_guidance
                return result
        else:
            result["error"] = "No OPENAI_API_KEY set and question does not match a demo. Use one of: " + "; ".join(DEMO_QUESTIONS)
            return result
    if not sql:
        result["error"] = "Could not generate SQL."
        return result
    # 2) Validate
    ok, final_sql = validate_sql(sql)
    if not ok:
        result["error"] = final_sql
        result["sql"] = sql
        return result
    result["sql"] = final_sql
    # 3) Execute
    try:
        df = execute_sql(con, final_sql)
    except Exception as e:
        result["error"] = f"Query failed: {e}"
        return result
    result["result_df"] = df
    result["table_str"] = format_table(df)
    result["interpretation"] = write_interpretation(df, interpretation_guidance)
    # 4) Optional plot
    if out_plot_path and df is not None and not df.empty:
        maybe_plot(df, chart_suggestion, out_plot_path)
        if out_plot_path.exists():
            result["plot_path"] = str(out_plot_path)
    return result


def main():
    import os
    con = init_duckdb()
    fig_dir = ROOT / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)

    if len(sys.argv) > 1:
        question = " ".join(sys.argv[1:])
        out = run_bot(question, con, out_plot_path=fig_dir / "assistant_result.png")
        print("--- Generated SQL ---")
        print(out.get("sql") or "(none)")
        print("\n--- Query Result ---")
        print(out.get("table_str") or "(no table)")
        print("\n--- Interpretation ---")
        print(out.get("interpretation") or "(none)")
        if out.get("plot_path"):
            print("\n--- Plot ---")
            print("Saved:", out["plot_path"])
        if out.get("error"):
            print("\n--- Error ---")
            print(out["error"])
        sys.exit(0 if not out.get("error") else 1)

    # Interactive: offer 3 demo questions
    print("ENEM Analyst Bot (local). Ask a question or choose a demo.")
    print("Demo questions:")
    for i, q in enumerate(DEMO_QUESTIONS, 1):
        print(f"  {i}. {q}")
    print("  Or type your own question (requires OPENAI_API_KEY for non-demo).")
    try:
        question = input("\nQuestion: ").strip()
    except EOFError:
        question = ""
    if not question:
        # Default to first demo
        question = DEMO_QUESTIONS[0]
    out = run_bot(question, con, out_plot_path=fig_dir / "assistant_result.png")
    print("\n--- Generated SQL ---")
    print(out.get("sql") or "(none)")
    print("\n--- Query Result ---")
    print(out.get("table_str") or "(no table)")
    print("\n--- Interpretation ---")
    print(out.get("interpretation") or "(none)")
    if out.get("plot_path"):
        print("\nPlot saved:", out["plot_path"])
    if out.get("error"):
        print("\nError:", out["error"])
    sys.exit(0 if not out.get("error") else 1)


if __name__ == "__main__":
    main()
