"""
Gold layer demo — SparkSQL queries showcasing the star schema.

ENEM Opportunity & Equity Radar (2020–2024). Reads Gold Parquet tables,
registers temp views, runs 3 demo queries and prints results.

Usage: python demo/gold_demo_queries.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DIR_GOLD = ROOT / "data" / "gold"
TABLES = [
    "fato_desempenho",
    "dim_tempo",
    "dim_geografia",
    "dim_perfil",
    "kpis_uf_ano",
    "distribuicoes_notas",
]


def main():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("ENEM-Gold-Demo-Queries")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    # Register Gold tables as temp views
    for name in TABLES:
        path = DIR_GOLD / name
        if path.exists():
            df = spark.read.parquet(str(path))
            view_name = name.replace("-", "_")
            df.createOrReplaceTempView(view_name)
            print(f"[OK] Registered view: {view_name} (rows: {df.count()})")
        else:
            print(f"[SKIP] Not found: {path}")

    # -------------------------------------------------------------------------
    # Query 1 — Top UFs with biggest improvement in Math (2020 → 2024)
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("Query 1 — Top UFs with biggest improvement in Math (2020 → 2024)")
    print("=" * 60)
    q1 = """
    WITH avg_by_uf_ano AS (
      SELECT
        g.sg_uf_residencia AS uf,
        f.ano,
        AVG(f.mt) AS avg_mt
      FROM fato_desempenho f
      JOIN dim_geografia g ON f.id_geo = g.id_geo
      WHERE f.ano IN (2020, 2024)
        AND g.sg_uf_residencia != 'NA'
        AND f.mt IS NOT NULL
      GROUP BY g.sg_uf_residencia, f.ano
    ),
    pivoted AS (
      SELECT
        uf,
        MAX(CASE WHEN ano = 2020 THEN avg_mt END) AS avg_2020,
        MAX(CASE WHEN ano = 2024 THEN avg_mt END) AS avg_2024
      FROM avg_by_uf_ano
      GROUP BY uf
    )
    SELECT
      uf,
      ROUND(avg_2020, 2) AS avg_mt_2020,
      ROUND(avg_2024, 2) AS avg_mt_2024,
      ROUND(avg_2024 - avg_2020, 2) AS delta_2020_2024
    FROM pivoted
    WHERE avg_2020 IS NOT NULL AND avg_2024 IS NOT NULL
    ORDER BY delta_2020_2024 DESC
    LIMIT 10
    """
    res1 = spark.sql(q1)
    res1.show(10, truncate=False)
    print(f"Row count: {res1.count()}")

    # -------------------------------------------------------------------------
    # Query 2 — Absence risk by profile (sexo + renda) post-pandemic
    # Gold holds only the valid cohort (all present), so "% not fully present"
    # would be 0. We show top 10 profiles by share of valid cohort instead;
    # for true absence risk run the same logic on Silver (pre-filter).
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("Query 2 — Profile (sexo + renda) post-pandemic: top 10 by share of valid cohort (2021–2024)")
    print("(Gold = valid-only; for % not fully present use Silver.)")
    print("=" * 60)
    q2 = """
    WITH profile_counts AS (
      SELECT
        p.tp_sexo AS sexo,
        p.faixa_renda AS renda,
        COUNT(*) AS participantes,
        SUM(COUNT(*)) OVER () AS total
      FROM fato_desempenho f
      JOIN dim_perfil p ON f.id_perfil = p.id_perfil
      WHERE f.ano BETWEEN 2021 AND 2024
        AND (p.tp_sexo != 'NA' OR p.faixa_renda != 'NA')
      GROUP BY p.tp_sexo, p.faixa_renda
    )
    SELECT
      sexo,
      renda,
      participantes,
      ROUND(100.0 * participantes / total, 2) AS pct_of_cohort
    FROM profile_counts
    ORDER BY participantes DESC
    LIMIT 10
    """
    res2 = spark.sql(q2)
    res2.show(10, truncate=False)
    print(f"Row count: {res2.count()}")

    # -------------------------------------------------------------------------
    # Query 3 — Redação excellence expansion (2020 → 2024)
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("Query 3 — Redação excellence expansion: UFs with strongest growth in % with redacao >= 800 (2020 → 2024)")
    print("=" * 60)
    q3 = """
    WITH uf_year_pct AS (
      SELECT
        g.sg_uf_residencia AS uf,
        f.ano,
        COUNT(*) AS n,
        SUM(CASE WHEN f.redacao >= 800 THEN 1 ELSE 0 END) AS n_800
      FROM fato_desempenho f
      JOIN dim_geografia g ON f.id_geo = g.id_geo
      WHERE g.sg_uf_residencia != 'NA'
        AND f.redacao IS NOT NULL
        AND f.ano IN (2020, 2024)
      GROUP BY g.sg_uf_residencia, f.ano
    ),
    pct AS (
      SELECT
        uf,
        ano,
        100.0 * n_800 / NULLIF(n, 0) AS pct_800
      FROM uf_year_pct
    ),
    growth AS (
      SELECT
        uf,
        MAX(CASE WHEN ano = 2020 THEN pct_800 END) AS pct_2020,
        MAX(CASE WHEN ano = 2024 THEN pct_800 END) AS pct_2024
      FROM pct
      GROUP BY uf
    )
    SELECT
      uf,
      ROUND(pct_2020, 2) AS pct_redacao_800_2020,
      ROUND(pct_2024, 2) AS pct_redacao_800_2024,
      ROUND(pct_2024 - pct_2020, 2) AS growth_pct_pts
    FROM growth
    WHERE pct_2020 IS NOT NULL AND pct_2024 IS NOT NULL
    ORDER BY growth_pct_pts DESC
    LIMIT 10
    """
    res3 = spark.sql(q3)
    res3.show(10, truncate=False)
    print(f"Row count: {res3.count()}")

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
