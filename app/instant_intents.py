"""
Instant SQL Engine (Layer 0) — intent catalog + safe SQL builder.

All intents are SELECT-only, LIMIT-capped, and restricted to allowlisted
Gold/Silver tables registered via DuckDB (see app.db).
"""

from __future__ import annotations

import re
from typing import Dict, List

import pandas as pd

# Allowlisted tables (as used in DuckDB views from app.db)
ALLOWED_TABLES = {
    "gold.kpis_uf_ano",
    "gold.fato_desempenho",
    "gold.dim_geografia",
    "gold.dim_perfil",
    "gold.distribuicoes_notas",
    "gold.cluster_profiles",
    "gold.cluster_evolution_uf_ano",
    "silver.quality_report",
    "silver.null_report",
}


# ---------------------------------------------------------------------------
# Intent catalog (>= 20 intents)
# ---------------------------------------------------------------------------

INTENT_CATALOG: Dict[str, Dict] = {
    # 1) Top UFs por média objetiva no período
    "top_ufs_media_objetiva": {
        "id": "top_ufs_media_objetiva",
        "description": "Top UFs por média objetiva no período selecionado",
        "required_params": ["year_start", "year_end", "limit"],
        "sql_template": """
            SELECT
                sg_uf_residencia AS uf,
                ROUND(AVG(media_objetiva), 2) AS media_objetiva,
                SUM(count_participantes) AS participantes
            FROM gold.kpis_uf_ano
            WHERE ano BETWEEN {year_start} AND {year_end}
              AND sg_uf_residencia != 'NA'
            GROUP BY sg_uf_residencia
            ORDER BY media_objetiva DESC
            LIMIT {limit}
        """,
        "default_limit": 10,
    },
    # 2) UFs que mais melhoraram em matemática (MT) entre dois anos
    "ufs_melhoraram_matematica": {
        "id": "ufs_melhoraram_matematica",
        "description": "UFs que mais melhoraram em matemática entre dois anos",
        "required_params": ["year_start", "year_end", "limit"],
        "sql_template": """
            WITH uf_year AS (
              SELECT
                g.sg_uf_residencia AS uf,
                f.ano,
                AVG(f.mt) AS avg_mt
              FROM gold.fato_desempenho f
              JOIN gold.dim_geografia g ON f.id_geo = g.id_geo
              WHERE f.ano IN ({year_start}, {year_end})
                AND g.sg_uf_residencia != 'NA'
                AND f.mt IS NOT NULL
              GROUP BY g.sg_uf_residencia, f.ano
            )
            SELECT
              uf,
              ROUND(MAX(CASE WHEN ano = {year_start} THEN avg_mt END), 2) AS mt_inicio,
              ROUND(MAX(CASE WHEN ano = {year_end} THEN avg_mt END), 2) AS mt_fim,
              ROUND(
                MAX(CASE WHEN ano = {year_end} THEN avg_mt END)
                - MAX(CASE WHEN ano = {year_start} THEN avg_mt END),
                2
              ) AS delta
            FROM uf_year
            GROUP BY uf
            HAVING mt_inicio IS NOT NULL AND mt_fim IS NOT NULL
            ORDER BY delta DESC
            LIMIT {limit}
        """,
        "default_limit": 10,
    },
    # 3) Média de redação por ano (Brasil)
    "media_redacao_por_ano": {
        "id": "media_redacao_por_ano",
        "description": "Média de redação por ano (Brasil)",
        "required_params": ["year_start", "year_end"],
        "sql_template": """
            SELECT
              ano,
              ROUND(AVG(media_redacao), 2) AS media_redacao,
              SUM(count_participantes) AS participantes
            FROM gold.kpis_uf_ano
            WHERE ano BETWEEN {year_start} AND {year_end}
            GROUP BY ano
            ORDER BY ano
        """,
        "default_limit": 50,
    },
    # 4) Média objetiva por ano (Brasil)
    "media_objetiva_por_ano": {
        "id": "media_objetiva_por_ano",
        "description": "Média objetiva por ano (Brasil)",
        "required_params": ["year_start", "year_end"],
        "sql_template": """
            SELECT
              ano,
              ROUND(AVG(media_objetiva), 2) AS media_objetiva,
              SUM(count_participantes) AS participantes
            FROM gold.kpis_uf_ano
            WHERE ano BETWEEN {year_start} AND {year_end}
            GROUP BY ano
            ORDER BY ano
        """,
        "default_limit": 50,
    },
    # 5) Presença plena por ano (Brasil)
    "presenca_por_ano": {
        "id": "presenca_por_ano",
        "description": "Presença plena (%) por ano (Brasil)",
        "required_params": ["year_start", "year_end"],
        "sql_template": """
            SELECT
              ano,
              ROUND(AVG(pct_presence_full), 2) AS pct_presenca_media
            FROM gold.kpis_uf_ano
            WHERE ano BETWEEN {year_start} AND {year_end}
              AND pct_presence_full IS NOT NULL
            GROUP BY ano
            ORDER BY ano
        """,
        "default_limit": 50,
    },
    # 6) Top UFs em % redação >= 800 em um ano
    "top_ufs_redacao_800": {
        "id": "top_ufs_redacao_800",
        "description": "Top UFs em % de redação ≥ 800 em um ano",
        "required_params": ["year", "limit"],
        "sql_template": """
            SELECT
              sg_uf_residencia AS uf,
              ROUND(pct_top800_redacao, 2) AS pct_redacao_800,
              count_participantes AS participantes
            FROM gold.kpis_uf_ano
            WHERE ano = {year}
              AND sg_uf_residencia != 'NA'
            ORDER BY pct_redacao_800 DESC
            LIMIT {limit}
        """,
        "default_limit": 10,
    },
    # 7) Top UFs por número de participantes em um ano
    "top_ufs_participantes": {
        "id": "top_ufs_participantes",
        "description": "Top UFs por número de participantes em um ano",
        "required_params": ["year", "limit"],
        "sql_template": """
            SELECT
              sg_uf_residencia AS uf,
              count_participantes AS participantes
            FROM gold.kpis_uf_ano
            WHERE ano = {year}
              AND sg_uf_residencia != 'NA'
            ORDER BY participantes DESC
            LIMIT {limit}
        """,
        "default_limit": 10,
    },
    # 8) Pior ano em média objetiva (Brasil)
    "pior_ano_media_objetiva": {
        "id": "pior_ano_media_objetiva",
        "description": "Pior ano em média objetiva (Brasil)",
        "required_params": ["year_start", "year_end", "limit"],
        "sql_template": """
            SELECT
              ano,
              ROUND(AVG(media_objetiva), 2) AS media_objetiva
            FROM gold.kpis_uf_ano
            WHERE ano BETWEEN {year_start} AND {year_end}
            GROUP BY ano
            ORDER BY media_objetiva ASC
            LIMIT {limit}
        """,
        "default_limit": 5,
    },
    # 9) Melhor ano em média objetiva (Brasil)
    "melhor_ano_media_objetiva": {
        "id": "melhor_ano_media_objetiva",
        "description": "Melhor ano em média objetiva (Brasil)",
        "required_params": ["year_start", "year_end", "limit"],
        "sql_template": """
            SELECT
              ano,
              ROUND(AVG(media_objetiva), 2) AS media_objetiva
            FROM gold.kpis_uf_ano
            WHERE ano BETWEEN {year_start} AND {year_end}
            GROUP BY ano
            ORDER BY media_objetiva DESC
            LIMIT {limit}
        """,
        "default_limit": 5,
    },
    # 10) Gap de renda vs média objetiva (perfil por renda)
    "gap_renda_media_objetiva": {
        "id": "gap_renda_media_objetiva",
        "description": "Gap de média objetiva por faixa de renda (Q006) 2020–2024",
        "required_params": ["year_start", "year_end"],
        "sql_template": """
            SELECT
              p.faixa_renda,
              ROUND(AVG(f.media_objetiva), 2) AS media_obj,
              COUNT(*) AS participantes
            FROM gold.fato_desempenho f
            JOIN gold.dim_perfil p ON f.id_perfil = p.id_perfil
            WHERE f.ano BETWEEN {year_start} AND {year_end}
            GROUP BY p.faixa_renda
            HAVING participantes > 0
            ORDER BY media_obj DESC
        """,
        "default_limit": 50,
    },
    # 11) Gap por sexo na média objetiva
    "gap_sexo_media_objetiva": {
        "id": "gap_sexo_media_objetiva",
        "description": "Gap de média objetiva por sexo (2020–2024)",
        "required_params": ["year_start", "year_end"],
        "sql_template": """
            SELECT
              p.tp_sexo AS sexo,
              ROUND(AVG(f.media_objetiva), 2) AS media_obj,
              COUNT(*) AS participantes
            FROM gold.fato_desempenho f
            JOIN gold.dim_perfil p ON f.id_perfil = p.id_perfil
            WHERE f.ano BETWEEN {year_start} AND {year_end}
            GROUP BY p.tp_sexo
            HAVING participantes > 0
            ORDER BY media_obj DESC
        """,
        "default_limit": 10,
    },
    # 12) Percentis de redação por ano (Brasil)
    "distrib_redacao_percentis": {
        "id": "distrib_redacao_percentis",
        "description": "Percentis de redação por ano (Brasil)",
        "required_params": ["year_start", "year_end"],
        "sql_template": """
            SELECT
              ano,
              ROUND(redacao_p10, 1) AS p10,
              ROUND(redacao_p25, 1) AS p25,
              ROUND(redacao_p50, 1) AS p50,
              ROUND(redacao_p75, 1) AS p75,
              ROUND(redacao_p90, 1) AS p90
            FROM gold.distribuicoes_notas
            WHERE ano BETWEEN {year_start} AND {year_end}
            ORDER BY ano
        """,
        "default_limit": 50,
    },
    # 13) Percentis de média objetiva por ano (Brasil)
    "distrib_objetiva_percentis": {
        "id": "distrib_objetiva_percentis",
        "description": "Percentis de média objetiva por ano (Brasil)",
        "required_params": ["year_start", "year_end"],
        "sql_template": """
            SELECT
              ano,
              ROUND(objetiva_p10, 1) AS p10,
              ROUND(objetiva_p25, 1) AS p25,
              ROUND(objetiva_p50, 1) AS p50,
              ROUND(objetiva_p75, 1) AS p75,
              ROUND(objetiva_p90, 1) AS p90
            FROM gold.distribuicoes_notas
            WHERE ano BETWEEN {year_start} AND {year_end}
            ORDER BY ano
        """,
        "default_limit": 50,
    },
    # 14) Tamanho dos clusters (cluster_profiles)
    "tamanho_clusters": {
        "id": "tamanho_clusters",
        "description": "Tamanho dos clusters e presença média",
        "required_params": [],
        "sql_template": """
            SELECT
              cluster_id,
              size,
              ROUND(media_redacao, 1) AS media_redacao,
              ROUND(media_obj, 1) AS media_obj,
              ROUND(presence_rate * 100.0, 1) AS presence_pct
            FROM gold.cluster_profiles
            ORDER BY cluster_id
        """,
        "default_limit": 50,
    },
    # 15) Crescimento de cluster de risco por UF (delta % participantes)
    "cluster_crescimento_por_uf": {
        "id": "cluster_crescimento_por_uf",
        "description": "Crescimento do cluster de maior risco por UF (2020→2024)",
        "required_params": ["cluster_id", "limit"],
        "sql_template": """
            WITH uf_year AS (
              SELECT
                uf,
                ano,
                pct_participants
              FROM gold.cluster_evolution_uf_ano
              WHERE cluster_id = {cluster_id}
                AND ano IN (2020, 2024)
            )
            SELECT
              uf,
              ROUND(MAX(CASE WHEN ano = 2020 THEN pct_participants END) * 100.0, 2) AS pct_2020,
              ROUND(MAX(CASE WHEN ano = 2024 THEN pct_participants END) * 100.0, 2) AS pct_2024,
              ROUND(
                (MAX(CASE WHEN ano = 2024 THEN pct_participants END)
                 - MAX(CASE WHEN ano = 2020 THEN pct_participants END)) * 100.0,
                2
              ) AS delta_pct_pts
            FROM uf_year
            GROUP BY uf
            HAVING pct_2020 IS NOT NULL AND pct_2024 IS NOT NULL
            ORDER BY delta_pct_pts DESC
            LIMIT {limit}
        """,
        "default_limit": 10,
    },
    # 16) Mix de clusters por ano para uma UF
    "uf_cluster_mix_ano": {
        "id": "uf_cluster_mix_ano",
        "description": "Mix de clusters por ano para uma UF",
        "required_params": ["uf", "limit"],
        "sql_template": """
            SELECT
              ano,
              cluster_id,
              ROUND(pct_participants * 100.0, 2) AS pct_participantes
            FROM gold.cluster_evolution_uf_ano
            WHERE uf = {uf}
            ORDER BY ano, cluster_id
            LIMIT {limit}
        """,
        "default_limit": 50,
    },
    # 17) Linhas removidas por ano (quality_report)
    "quality_linhas_removidas_ano": {
        "id": "quality_linhas_removidas_ano",
        "description": "Linhas removidas por ano (Silver quality_report)",
        "required_params": [],
        "sql_template": """
            SELECT
              year AS ano,
              SUM(rows_removed) AS linhas_removidas
            FROM silver.quality_report
            GROUP BY year
            ORDER BY ano
        """,
        "default_limit": 50,
    },
    # 18) Top regras que mais filtraram (quality_report)
    "quality_top_regras": {
        "id": "quality_top_regras",
        "description": "Top regras que mais filtraram linhas (Silver quality_report)",
        "required_params": ["limit"],
        "sql_template": """
            SELECT
              rule_name,
              SUM(rows_removed) AS linhas_removidas
            FROM silver.quality_report
            GROUP BY rule_name
            ORDER BY linhas_removidas DESC
            LIMIT {limit}
        """,
        "default_limit": 10,
    },
    # 19) Top colunas com maior % de nulos (null_report)
    "null_top_colunas": {
        "id": "null_top_colunas",
        "description": "Top colunas com maior % de nulos (Silver null_report)",
        "required_params": ["limit"],
        "sql_template": """
            SELECT
              column_name,
              MAX(pct_null) AS pct_null_max
            FROM silver.null_report
            GROUP BY column_name
            ORDER BY pct_null_max DESC
            LIMIT {limit}
        """,
        "default_limit": 10,
    },
    # 20) Participação por UF e ano (contagem simples)
    "participacao_uf_ano": {
        "id": "participacao_uf_ano",
        "description": "Participação (n) por UF e ano",
        "required_params": ["year_start", "year_end", "limit"],
        "sql_template": """
            SELECT
              ano,
              sg_uf_residencia AS uf,
              count_participantes AS participantes
            FROM gold.kpis_uf_ano
            WHERE ano BETWEEN {year_start} AND {year_end}
              AND sg_uf_residencia != 'NA'
            ORDER BY ano, participantes DESC
            LIMIT {limit}
        """,
        "default_limit": 20,
    },
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sanitize_string(value: str) -> str:
    """Keep only safe characters for identifiers / UF codes."""
    s = str(value).strip()
    s = s.replace(" ", "_")
    return re.sub(r"[^A-Za-z0-9_\\-]", "", s)


FORBIDDEN_KEYWORDS = ("INSERT", "UPDATE", "DELETE", "DROP", "CREATE")


def validate_sql(sql: str) -> bool:
    """
    Basic SQL guard for Instant SQL Engine.

    Rules:
    - Deve começar com SELECT ou WITH (CTE)
    - Deve conter LIMIT
    - Não pode conter INSERT/UPDATE/DELETE/DROP/CREATE
    - Não pode usar SELECT *
    - Tabelas devem estar em ALLOWED_TABLES
    """
    if not sql or not sql.strip():
        return False
    raw = sql.strip()
    upper = raw.upper()

    # Start
    if not (upper.startswith("SELECT") or upper.startswith("WITH")):
        return False

    # LIMIT obrigatório
    if "LIMIT" not in upper:
        return False

    # Palavras proibidas
    for kw in FORBIDDEN_KEYWORDS:
        if kw in upper:
            return False

    # SELECT *
    if re.search(r"\\bSELECT\\s+(\\w+\\.)?\\*", raw, flags=re.IGNORECASE | re.DOTALL):
        return False

    # Tabelas allowlist
    refs = re.findall(r"(?:FROM|JOIN)\\s+([A-Za-z0-9_.]+)", raw, flags=re.IGNORECASE)
    allowed_lc = {t.lower() for t in ALLOWED_TABLES}
    for r in refs:
        if r.lower() not in allowed_lc:
            return False

    return True


def build_intent_sql(intent_id: str, params: dict) -> str:
    """Build and validate final SQL string for a given intent + params."""
    if intent_id not in INTENT_CATALOG:
        raise ValueError(f"Unknown intent_id: {intent_id}")

    intent = INTENT_CATALOG[intent_id]
    tmpl = intent["sql_template"]
    default_limit = int(intent.get("default_limit", 10))

    # Prepare safe params
    safe_params: dict = {}

    required: List[str] = intent.get("required_params", [])
    for name in required:
        if name not in params:
            raise ValueError(f"Missing required param '{name}' for intent '{intent_id}'")
        val = params[name]

        if name in {"year_start", "year_end", "year", "ano"}:
            try:
                safe_params[name] = str(int(val))
            except Exception:
                raise ValueError(f"Param '{name}' must be an integer year.")
        elif name == "limit":
            try:
                n = int(val)
            except Exception:
                n = default_limit
            if n <= 0:
                n = default_limit
            n = min(n, default_limit)
            safe_params[name] = str(n)
        elif name == "uf":
            s = _sanitize_string(val or "")
            safe_params[name] = f"'{s}'"
        elif name == "cluster_id":
            try:
                safe_params[name] = str(int(val))
            except Exception:
                raise ValueError("cluster_id must be integer.")
        else:
            # Generic string param
            s = _sanitize_string(val or "")
            safe_params[name] = f"'{s}'"

    # Ensure limit placeholder exists if template expects it
    if "{limit}" in tmpl and "limit" not in safe_params:
        safe_params["limit"] = str(default_limit)

    sql = tmpl.format(**safe_params)
    sql = sql.strip()

    # --- Safety checks ------------------------------------------------------
    if ";" in sql:
        raise ValueError("Multiple statements are not allowed.")

    upper = sql.lstrip().upper()
    if not (upper.startswith("SELECT") or upper.startswith("WITH")):
        raise ValueError("Only SELECT statements or CTEs (WITH) are allowed.")

    # Block SELECT *
    if re.search(r"\\bSELECT\\s+(\\w+\\.)?\\*", sql, flags=re.IGNORECASE | re.DOTALL):
        raise ValueError("SELECT * is not allowed; use explicit columns.")

    # Table allowlist
    refs = re.findall(r"(?:FROM|JOIN)\\s+([A-Za-z0-9_.]+)", sql, flags=re.IGNORECASE)
    allowed_lc = {t.lower() for t in ALLOWED_TABLES}
    for r in refs:
        if r.lower() not in allowed_lc:
            raise ValueError(f"Table '{r}' is not in the allowlist for Instant SQL.")

    # Enforce LIMIT <= default_limit (só adiciona LIMIT se ainda não existir — evita duplicado)
    if "LIMIT" not in sql.upper():
        sql = sql.rstrip() + f"\nLIMIT {default_limit}"
    else:
        m = re.search(r"\bLIMIT\s+(\d+)", sql, flags=re.IGNORECASE)
        if m:
            n = int(m.group(1))
            if n > default_limit:
                sql = re.sub(r"\bLIMIT\s+\d+", f"LIMIT {default_limit}", sql, flags=re.IGNORECASE, count=1)

    # Garantir um único LIMIT (evita "LINE N: LIMIT 10\nLIMIT 10" por qualquer duplicação)
    limit_matches = list(re.finditer(r"\bLIMIT\s+(\d+)", sql, flags=re.IGNORECASE))
    if len(limit_matches) > 1:
        first = limit_matches[0]
        sql = sql[: first.end()].rstrip()

    # Final validation
    if not validate_sql(sql):
        raise ValueError("Generated SQL failed validation for Instant SQL.")

    return sql


def run_intent(intent_id: str, params: dict, conn) -> pd.DataFrame:
    """
    Build SQL for an intent, validate it and execute using the given DuckDB connection.

    Returns a pandas.DataFrame (results already capped by LIMIT).
    """
    sql = build_intent_sql(intent_id, params)
    return conn.execute(sql).fetchdf()

