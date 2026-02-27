"""
ENEM Analyst Bot — Live demo script.

Runs 3 questions, prints SQL, result table (top 15 rows), interpretation, and saves
optional plots to reports/figures/. Uses DuckDB over Gold Parquet (same as assistant/app.py).

Run from repo root:
  python demo/run_live_demo.py

No external APIs required; uses built-in demo SQL for the 3 questions.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from assistant.app import init_duckdb, run_bot

# -----------------------------------------------------------------------------
# 3 demo questions (presentation-ready). Q3 uses UF; municipality not in Gold schema.
# -----------------------------------------------------------------------------
DEMO_QUESTIONS = [
    "Quais UFs melhoraram mais em matemática de 2021 a 2024?",
    "Quais perfis (sexo + renda) têm maior risco de ausência no ENEM pós-2020?",
    "Mostre top 10 UFs com maior crescimento de redação >= 800 de 2020 a 2024.",
]

MAX_TABLE_ROWS = 15
SEP = "=" * 72
SEP_THIN = "-" * 72
REPORTS_FIGURES = ROOT / "reports" / "figures"


def _format_table(out: dict, max_rows: int) -> str:
    """Presentation-ready table: up to max_rows from result_df, or table_str fallback."""
    df = out.get("result_df")
    if df is not None and not df.empty:
        display = df.head(max_rows)
        return display.to_string(index=False)
    return out.get("table_str") or "(sem tabela)"


def main():
    REPORTS_FIGURES.mkdir(parents=True, exist_ok=True)
    con = init_duckdb()

    for i, question in enumerate(DEMO_QUESTIONS, 1):
        print("\n" + SEP)
        print(f"  Q{i}: {question}")
        print(SEP)

        plot_path = REPORTS_FIGURES / f"demo_q{i}.png"
        out = run_bot(question, con, out_plot_path=plot_path)

        if out.get("error"):
            print("\n[ERROR]", out["error"])
            continue

        print("\n--- SQL gerado ---")
        print(out.get("sql") or "(nenhum)")

        print("\n--- Resultado (até {} linhas) ---".format(MAX_TABLE_ROWS))
        print(_format_table(out, MAX_TABLE_ROWS))

        print("\n--- Interpretação ---")
        print(out.get("interpretation") or "(nenhuma)")

        if out.get("plot_path"):
            print("\n--- Gráfico ---")
            print("Salvo em:", out["plot_path"])

        print(SEP_THIN)

    print("\n" + SEP)
    print("  Demo concluído.")
    print(SEP + "\n")


if __name__ == "__main__":
    main()
    sys.exit(0)
