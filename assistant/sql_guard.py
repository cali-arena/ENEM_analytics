"""
ENEM Analyst Bot — SQL guard: allowlist, SELECT-only, LIMIT, no SELECT *.

Rejects: non-SELECT, multiple statements, PRAGMA/ATTACH/EXPORT/COPY, wildcard SELECT *.
Enforces: LIMIT <= 200, only allowlisted table names.
"""
import re

# Allowlisted tables (exact names used in FROM/JOIN). Case-insensitive check.
ALLOWED_TABLES = [
    "gold_fato_desempenho",
    "gold_dim_tempo",
    "gold_dim_geografia",
    "gold_dim_perfil",
    "gold_kpis_uf_ano",
    "gold_distribuicoes_notas",
    "gold_cluster_profiles",
    "gold_cluster_evolution_uf_ano",
    "silver_quality_report",
    "silver_null_report",
]

MAX_LIMIT = 200

# Forbidden keywords (statement types)
FORBIDDEN = (
    "CREATE", "DROP", "INSERT", "UPDATE", "DELETE", "ALTER", "TRUNCATE",
    "GRANT", "REVOKE", "PRAGMA", "ATTACH", "DETACH", "EXPORT", "COPY",
    "IMPORT", "BEGIN", "COMMIT", "ROLLBACK", "EXPLAIN", "VACUUM",
)

# Pattern: SELECT * or SELECT alias.* (block wildcard)
SELECT_STAR_PATTERN = re.compile(
    r"\bSELECT\s+(\w+\.)?\*",
    re.IGNORECASE | re.DOTALL,
)


def normalize_table_ref(name: str) -> str:
    return name.strip().lower()


def validate(sql: str) -> tuple[bool, str]:
    """
    Validate SQL. Returns (ok, final_sql_or_error_message).
    - Only SELECT.
    - Single statement (strip trailing ; allowed).
    - No forbidden keywords.
    - No SELECT * (explicit columns or aggregates only).
    - Only allowlisted tables.
    - LIMIT present and <= MAX_LIMIT.
    """
    if not sql or not sql.strip():
        return False, "Empty SQL."
    raw = sql.strip().rstrip(";")
    upper = raw.upper()

    # Single statement: no ; in the middle
    if ";" in raw:
        return False, "Multiple statements are not allowed."

    for kw in FORBIDDEN:
        if kw in upper:
            return False, f"Rejected: SQL must not contain {kw}."

    if not upper.startswith("SELECT"):
        return False, "Only SELECT statements are allowed."

    # Block SELECT *
    if SELECT_STAR_PATTERN.search(raw):
        return False, "SELECT * is not allowed; use explicit column names or aggregated outputs."

    # Table refs: FROM and JOIN <table>
    refs = re.findall(r"(?:FROM|JOIN)\s+(\w+)", raw, re.IGNORECASE)
    allowed_set = {normalize_table_ref(t) for t in ALLOWED_TABLES}
    for r in refs:
        if normalize_table_ref(r) not in allowed_set:
            return False, f"Rejected: table '{r}' is not in the allowlist."

    # LIMIT
    limit_match = re.search(r"\bLIMIT\s+(\d+)", raw, re.IGNORECASE)
    if limit_match:
        n = int(limit_match.group(1))
        if n > MAX_LIMIT:
            raw = re.sub(r"\bLIMIT\s+\d+", f"LIMIT {MAX_LIMIT}", raw, flags=re.IGNORECASE)
    else:
        raw = raw.rstrip() + f"\nLIMIT {MAX_LIMIT}"

    return True, raw
