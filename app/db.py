"""
ENEM Radar — DuckDB connection and table registration.

Reads Parquet from data/gold/* and data/silver/quality_report*, null_report*.
Registers them as DuckDB views (gold.*, silver.*). Missing tables are skipped.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIR_GOLD = (ROOT / "data" / "gold").resolve()
DIR_SILVER = (ROOT / "data" / "silver").resolve()

TABLE_PATHS = {
    "gold.fato_desempenho": DIR_GOLD / "fato_desempenho",
    "gold.dim_tempo": DIR_GOLD / "dim_tempo",
    "gold.dim_geografia": DIR_GOLD / "dim_geografia",
    "gold.dim_perfil": DIR_GOLD / "dim_perfil",
    "gold.kpis_uf_ano": DIR_GOLD / "kpis_uf_ano",
    "gold.distribuicoes_notas": DIR_GOLD / "distribuicoes_notas",
    "gold.cluster_profiles": DIR_GOLD / "cluster_profiles.parquet",
    "gold.cluster_evolution_uf_ano": DIR_GOLD / "cluster_evolution_uf_ano.parquet",
    "silver.quality_report": DIR_SILVER / "quality_report",
    "silver.null_report": DIR_SILVER / "null_report",
}


def get_connection():
    """Create DuckDB in-memory connection and register all existing Parquet tables as views."""
    import duckdb
    con = duckdb.connect(database=":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    for full_name, path in TABLE_PATHS.items():
        if not path.exists():
            continue
        if path.is_dir():
            path_str = str(path.resolve() / "**" / "*.parquet")
        else:
            path_str = str(path.resolve())
        # DuckDB read_parquet não aceita ? em CREATE VIEW; usar literal escapado
        path_escaped = path_str.replace("\\", "/").replace("'", "''")
        try:
            con.execute(
                "CREATE OR REPLACE VIEW {} AS SELECT * FROM read_parquet('{}')".format(
                    full_name, path_escaped
                ),
            )
        except Exception:
            try:
                path_escaped2 = str(path.resolve()).replace("\\", "/").replace("'", "''")
                con.execute(
                    "CREATE OR REPLACE VIEW {} AS SELECT * FROM read_parquet('{}')".format(
                        full_name, path_escaped2
                    ),
                )
            except Exception:
                pass
    return con


def table_exists(con, full_name: str) -> bool:
    """Check if a view exists (was registered)."""
    try:
        con.execute("SELECT 1 FROM {} LIMIT 0".format(full_name))
        return True
    except Exception:
        return False


def list_loaded_tables(con):
    """Return list of (schema, table) that were successfully loaded."""
    loaded = []
    for full_name, path in TABLE_PATHS.items():
        if path.exists() and table_exists(con, full_name):
            loaded.append(full_name)
    return loaded
