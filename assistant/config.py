"""
ENEM Analyst Bot — Allowlist and paths. No web calls; Gold + Silver (optional) only.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIR_GOLD = ROOT / "data" / "gold"
DIR_SILVER = ROOT / "data" / "silver"

# Allowlist: only these tables may appear in generated SQL (case-insensitive)
ALLOWED_TABLES = [
    "gold.fato_desempenho",
    "gold.dim_tempo",
    "gold.dim_geografia",
    "gold.dim_perfil",
    "gold.kpis_uf_ano",
    "gold.distribuicoes_notas",
    "gold.cluster_profiles",
    "gold.cluster_evolution_uf_ano",
    "silver.quality_report",
    "silver.null_report",
]

# Parquet paths (optional tables may not exist)
TABLE_PATHS = {
    "gold.fato_desempenho": DIR_GOLD / "fato_desempenho",
    "gold.dim_tempo": DIR_GOLD / "dim_tempo",
    "gold.dim_geografia": DIR_GOLD / "dim_geografia",
    "gold.dim_perfil": DIR_GOLD / "dim_perfil",
    "gold.kpis_uf_ano": DIR_GOLD / "kpis_uf_ano",
    "gold.distribuicoes_notas": DIR_GOLD / "distribuicoes_notas",
    "gold.cluster_profiles": DIR_GOLD / "cluster_profiles",
    "gold.cluster_evolution_uf_ano": DIR_GOLD / "cluster_evolution_uf_ano",
    "silver.quality_report": DIR_SILVER / "quality_report",
    "silver.null_report": DIR_SILVER / "null_report",
}

MAX_ROWS = 200
FORBIDDEN_SQL = ("CREATE", "DROP", "INSERT", "UPDATE", "DELETE", "ALTER", "TRUNCATE", "GRANT", "REVOKE")
