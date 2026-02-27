"""
Smoke test: required Gold Parquet paths exist, load in DuckDB, run one KPI query.
Exit 0 on success, non-zero on failure.

Usage: python scripts/smoke_test.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DIR_GOLD = ROOT / "data" / "gold"

# Required for dashboard and bot (directories with Parquet or single file)
REQUIRED_PATHS = [
    "fato_desempenho",
    "dim_tempo",
    "dim_geografia",
    "dim_perfil",
    "kpis_uf_ano",
    "distribuicoes_notas",
]


def _path_exists(path: Path) -> bool:
    if path.is_dir():
        return any(path.rglob("*.parquet"))
    return path.exists()


def main() -> int:
    missing = []
    for name in REQUIRED_PATHS:
        p = DIR_GOLD / name
        if not _path_exists(p):
            missing.append(str(p))
    if missing:
        print("Smoke test FAIL: missing paths:", missing)
        return 1

    import duckdb

    con = duckdb.connect(database=":memory:")

    # Register Gold tables (DuckDB read_parquet supports dir with **/*.parquet)
    for name in REQUIRED_PATHS:
        path = DIR_GOLD / name
        path_str = str(path / "**" / "*.parquet") if path.is_dir() else str(path)
        escaped = path_str.replace("'", "''")
        try:
            con.execute(f"CREATE OR REPLACE VIEW gold_{name} AS SELECT * FROM read_parquet('{escaped}')")
        except Exception:
            escaped_single = str(path).replace("'", "''")
            con.execute(f"CREATE OR REPLACE VIEW gold_{name} AS SELECT * FROM read_parquet('{escaped_single}')")

    # One simple KPI query: total participants by year from fato_desempenho
    try:
        df = con.execute("""
            SELECT ano, COUNT(*) AS n
            FROM gold_fato_desempenho
            GROUP BY ano
            ORDER BY ano
            LIMIT 10
        """).fetchdf()
    except Exception as e:
        print("Smoke test FAIL: KPI query error:", e)
        return 1

    if df is None or df.empty:
        print("Smoke test FAIL: KPI query returned no rows")
        return 1

    print("Smoke test OK: Gold paths present, DuckDB load OK, KPI query returned", len(df), "rows")
    print(df.to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
