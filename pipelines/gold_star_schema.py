"""
Gold layer — Star schema for ENEM Opportunity & Equity Radar (2020–2024).

CRISP-DM: Modeling (Data Modeling / Star Schema). Reads Silver enem_participante,
builds fact table (fato_desempenho), dimensions (dim_tempo, dim_geografia, dim_perfil),
and aggregations (kpis_uf_ano, distribuicoes_notas). All Parquet, partitioned by ano where applicable.

Usage: python pipelines/gold_star_schema.py
"""
import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DIR_SILVER = ROOT / "data" / "silver"
DIR_GOLD = ROOT / "data" / "gold"
SILVER_PARTICIPANTE = DIR_SILVER / "enem_participante"
ANOS = [2020, 2021, 2022, 2023, 2024]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("ENEM-Gold-Star-Schema")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


# -----------------------------------------------------------------------------
# Dimensions
# -----------------------------------------------------------------------------

def build_dim_tempo(spark, silver_df) -> "pyspark.sql.DataFrame":
    """gold.dim_tempo: id_tempo, ano, ciclo_pre_pos_pandemia."""
    dim = silver_df.select("ano").distinct()
    dim = dim.withColumn(
        "ciclo_pre_pos_pandemia",
        F.when(F.col("ano").isin(2020, 2021), F.lit("pandemia"))
        .when(F.col("ano") >= 2022, F.lit("pos_pandemia"))
        .otherwise(F.lit("pre_pandemia")),
    )
    # id_tempo = ano as surrogate (1:1)
    dim = dim.withColumn("id_tempo", F.col("ano").cast("int")).select("id_tempo", "ano", "ciclo_pre_pos_pandemia")
    return dim


def build_dim_geografia(spark, silver_df) -> "pyspark.sql.DataFrame":
    """gold.dim_geografia: id_geo, sg_uf_residencia. UF only (no municipality in Silver canonical)."""
    dim = silver_df.select(F.col("SG_UF_RESIDENCIA").alias("sg_uf_residencia")).distinct()
    dim = dim.filter(F.col("sg_uf_residencia").isNotNull() & (F.trim(F.col("sg_uf_residencia")) != ""))
    w = Window.orderBy("sg_uf_residencia")
    dim = dim.withColumn("id_geo", F.dense_rank().over(w).cast("int"))
    dim = dim.select("id_geo", "sg_uf_residencia")
    return dim


def build_dim_perfil(spark, silver_df) -> "pyspark.sql.DataFrame":
    """gold.dim_perfil: id_perfil, tp_sexo, faixa_renda (from Q006)."""
    dim = silver_df.select(
        F.col("TP_SEXO").alias("tp_sexo"),
        F.coalesce(F.col("Q006"), F.lit("")).alias("faixa_renda"),
    ).distinct()
    w = Window.orderBy("tp_sexo", "faixa_renda")
    dim = dim.withColumn("id_perfil", F.dense_rank().over(w).cast("int"))
    dim = dim.select("id_perfil", "tp_sexo", "faixa_renda")
    return dim


# -----------------------------------------------------------------------------
# Fact table
# -----------------------------------------------------------------------------

def build_fact(
    spark,
    silver_df,
    dim_tempo,
    dim_geo,
    dim_perfil,
) -> "pyspark.sql.DataFrame":
    """gold.fato_desempenho. Grain: (nu_inscricao, ano). Surrogate keys from dims."""
    # Join Silver to dims to get surrogate keys
    silver = silver_df.withColumnRenamed("ano", "_ano")
    silver = silver.withColumnRenamed("SG_UF_RESIDENCIA", "_sg_uf")
    silver = silver.withColumnRenamed("TP_SEXO", "_tp_sexo")
    silver = silver.withColumnRenamed("Q006", "_q006")

    # Join to dims: use "NA" for null/empty so they match id_geo=0, id_perfil=0
    silver_join = silver.withColumn("_uf_key", F.coalesce(F.trim(silver["_sg_uf"]), F.lit("NA")))
    silver_join = silver_join.withColumn("_sexo_key", F.coalesce(F.trim(silver_join["_tp_sexo"]), F.lit("NA")))
    silver_join = silver_join.withColumn("_renda_key", F.coalesce(silver_join["_q006"], F.lit("NA")))
    fact = (
        silver_join
        .join(dim_tempo, silver_join["_ano"] == dim_tempo["ano"], "left")
        .join(dim_geo, silver_join["_uf_key"] == dim_geo["sg_uf_residencia"], "left")
        .join(
            dim_perfil,
            (silver_join["_sexo_key"] == dim_perfil["tp_sexo"]) & (silver_join["_renda_key"] == dim_perfil["faixa_renda"]),
            "left",
        )
    )
    # Fill null id_geo/id_perfil with 0 or 1 for "unknown" - use 0 as unknown surrogate
    fact = fact.withColumn("id_geo", F.coalesce(F.col("id_geo"), F.lit(0)))
    fact = fact.withColumn("id_perfil", F.coalesce(F.col("id_perfil"), F.lit(0)))

    fact = fact.select(
        F.col("NU_INSCRICAO").alias("nu_inscricao"),
        F.col("_ano").alias("ano"),
        F.col("id_tempo"),
        F.col("id_geo"),
        F.col("id_perfil"),
        F.col("NU_NOTA_CN").alias("cn"),
        F.col("NU_NOTA_CH").alias("ch"),
        F.col("NU_NOTA_LC").alias("lc"),
        F.col("NU_NOTA_MT").alias("mt"),
        F.col("NU_NOTA_REDACAO").alias("redacao"),
        (
            (F.col("NU_NOTA_CN") + F.col("NU_NOTA_CH") + F.col("NU_NOTA_LC") + F.col("NU_NOTA_MT")) / 4.0
        ).alias("media_objetiva"),
        F.col("TP_PRESENCA_CN").alias("presenca_cn"),
        F.col("TP_PRESENCA_CH").alias("presenca_ch"),
        F.col("TP_PRESENCA_LC").alias("presenca_lc"),
        F.col("TP_PRESENCA_MT").alias("presenca_mt"),
        F.current_timestamp().alias("ingest_ts"),
    )
    return fact


# -----------------------------------------------------------------------------
# Aggregations
# -----------------------------------------------------------------------------

def build_kpis(silver_df) -> "pyspark.sql.DataFrame":
    """gold.kpis_uf_ano: count_participantes, media_redacao, media_objetiva, p25/p50/p75, % top800_redacao, % presence_full."""
    media_obj = (
        F.col("NU_NOTA_CN") + F.col("NU_NOTA_CH") + F.col("NU_NOTA_LC") + F.col("NU_NOTA_MT")
    ) / 4.0
    df = silver_df.withColumn("_media_objetiva", media_obj)
    df = df.withColumn("_top800_redacao", F.when(F.col("NU_NOTA_REDACAO") >= 800, 1).otherwise(0))
    df = df.withColumn(
        "_presence_full",
        F.when(
            (F.col("TP_PRESENCA_CN") == 1)
            & (F.col("TP_PRESENCA_CH") == 1)
            & (F.col("TP_PRESENCA_LC") == 1)
            & (F.col("TP_PRESENCA_MT") == 1),
            1,
        ).otherwise(0),
    )
    # Coalesce UF for grouping (null -> "XX" or "NA")
    df = df.withColumn("_uf", F.coalesce(F.trim(F.col("SG_UF_RESIDENCIA")), F.lit("NA")))

    kpis = (
        df.groupBy("ano", "_uf")
        .agg(
            F.count("*").alias("count_participantes"),
            F.mean("NU_NOTA_REDACAO").alias("media_redacao"),
            F.mean("_media_objetiva").alias("media_objetiva"),
            F.expr("percentile_approx(NU_NOTA_REDACAO, 0.25, 10000)").alias("p25_redacao"),
            F.expr("percentile_approx(NU_NOTA_REDACAO, 0.5, 10000)").alias("p50_redacao"),
            F.expr("percentile_approx(NU_NOTA_REDACAO, 0.75, 10000)").alias("p75_redacao"),
            F.expr("percentile_approx(_media_objetiva, 0.25, 10000)").alias("p25_objetiva"),
            F.expr("percentile_approx(_media_objetiva, 0.5, 10000)").alias("p50_objetiva"),
            F.expr("percentile_approx(_media_objetiva, 0.75, 10000)").alias("p75_objetiva"),
            (F.sum("_top800_redacao") / F.count("*") * 100).alias("pct_top800_redacao"),
            (F.sum("_presence_full") / F.count("*") * 100).alias("pct_presence_full"),
        )
        .withColumnRenamed("_uf", "sg_uf_residencia")
    )
    return kpis


def build_distribuicoes(silver_df) -> "pyspark.sql.DataFrame":
    """gold.distribuicoes_notas: by year + UF, percentiles and bucket counts for redacao and media_objetiva."""
    media_obj = (
        F.col("NU_NOTA_CN") + F.col("NU_NOTA_CH") + F.col("NU_NOTA_LC") + F.col("NU_NOTA_MT")
    ) / 4.0
    df = silver_df.withColumn("_media_objetiva", media_obj)
    df = df.withColumn("_uf", F.coalesce(F.trim(F.col("SG_UF_RESIDENCIA")), F.lit("NA")))

    # Percentiles by ano + UF for redacao and media_objetiva
    dist = (
        df.groupBy("ano", "_uf")
        .agg(
            F.expr("percentile_approx(NU_NOTA_REDACAO, array(0.1, 0.25, 0.5, 0.75, 0.9), 10000)").alias("redacao_quantiles"),
            F.expr("percentile_approx(_media_objetiva, array(0.1, 0.25, 0.5, 0.75, 0.9), 10000)").alias("objetiva_quantiles"),
            F.count("*").alias("n"),
        )
        .withColumnRenamed("_uf", "sg_uf_residencia")
    )
    # Flatten quantiles for easier consumption: p10, p25, p50, p75, p90
    dist = dist.withColumn("redacao_p10", F.col("redacao_quantiles")[0])
    dist = dist.withColumn("redacao_p25", F.col("redacao_quantiles")[1])
    dist = dist.withColumn("redacao_p50", F.col("redacao_quantiles")[2])
    dist = dist.withColumn("redacao_p75", F.col("redacao_quantiles")[3])
    dist = dist.withColumn("redacao_p90", F.col("redacao_quantiles")[4])
    dist = dist.withColumn("objetiva_p10", F.col("objetiva_quantiles")[0])
    dist = dist.withColumn("objetiva_p25", F.col("objetiva_quantiles")[1])
    dist = dist.withColumn("objetiva_p50", F.col("objetiva_quantiles")[2])
    dist = dist.withColumn("objetiva_p75", F.col("objetiva_quantiles")[3])
    dist = dist.withColumn("objetiva_p90", F.col("objetiva_quantiles")[4])
    dist = dist.drop("redacao_quantiles", "objetiva_quantiles")
    return dist


# -----------------------------------------------------------------------------
# Write and validation
# -----------------------------------------------------------------------------

def write_gold_table(df, name: str, partition_by_ano: bool = False) -> Path:
    path = DIR_GOLD / name
    path.parent.mkdir(parents=True, exist_ok=True)
    if partition_by_ano and "ano" in df.columns:
        df.write.mode("overwrite").partitionBy("ano").parquet(str(path))
    else:
        df.write.mode("overwrite").parquet(str(path))
    logger.info("Written: %s", path)
    return path


def main():
    spark = get_spark()
    DIR_GOLD.mkdir(parents=True, exist_ok=True)

    if not SILVER_PARTICIPANTE.exists():
        logger.error("Silver table not found: %s", SILVER_PARTICIPANTE)
        spark.stop()
        sys.exit(1)

    silver_df = spark.read.parquet(str(SILVER_PARTICIPANTE))
    logger.info("Silver row count: %s", silver_df.count())

    dim_tempo = build_dim_tempo(spark, silver_df)
    dim_geo = build_dim_geografia(spark, silver_df)
    dim_geo_na = spark.createDataFrame([(0, "NA")], ["id_geo", "sg_uf_residencia"])
    dim_geo = dim_geo_na.union(
        dim_geo.withColumn("id_geo", F.col("id_geo") + 1)
    )
    dim_perfil = build_dim_perfil(spark, silver_df)
    dim_perfil_na = spark.createDataFrame([(0, "NA", "NA")], ["id_perfil", "tp_sexo", "faixa_renda"])
    dim_perfil = dim_perfil_na.union(
        dim_perfil.withColumn("id_perfil", F.col("id_perfil") + 1)
    )

    fact = build_fact(spark, silver_df, dim_tempo, dim_geo, dim_perfil)
    kpis = build_kpis(silver_df)
    dist = build_distribuicoes(silver_df)

    write_gold_table(dim_tempo, "dim_tempo")
    write_gold_table(dim_geo, "dim_geografia")
    write_gold_table(dim_perfil, "dim_perfil")
    write_gold_table(fact, "fato_desempenho", partition_by_ano=True)
    write_gold_table(kpis, "kpis_uf_ano", partition_by_ano=True)
    write_gold_table(dist, "distribuicoes_notas", partition_by_ano=True)

    # ---------- Validation ----------
    logger.info("========== GOLD VALIDATION ==========")
    for name in ["dim_tempo", "dim_geografia", "dim_perfil", "fato_desempenho", "kpis_uf_ano", "distribuicoes_notas"]:
        path = DIR_GOLD / name
        if path.exists():
            df = spark.read.parquet(str(path))
            logger.info("%s: row count = %s", name, df.count())
            logger.info("%s schema: %s", name, df.schema.simpleString())

    # Sample join: fact + dims
    fact_df = spark.read.parquet(str(DIR_GOLD / "fato_desempenho"))
    dim_t = spark.read.parquet(str(DIR_GOLD / "dim_tempo"))
    dim_g = spark.read.parquet(str(DIR_GOLD / "dim_geografia"))
    dim_p = spark.read.parquet(str(DIR_GOLD / "dim_perfil"))
    joined = (
        fact_df
        .join(dim_t, fact_df["id_tempo"] == dim_t["id_tempo"], "left")
        .join(dim_g, fact_df["id_geo"] == dim_g["id_geo"], "left")
        .join(dim_p, fact_df["id_perfil"] == dim_p["id_perfil"], "left")
    )
    logger.info("Fact + dims join count: %s (expected = fact count)", joined.count())
    logger.info("Fact count: %s", fact_df.count())

    spark.stop()
    logger.info("Gold star schema pipeline finished.")


if __name__ == "__main__":
    main()
</think>
Fixing the main() logic: simplifying dimension handling and completing the file.
<｜tool▁calls▁begin｜><｜tool▁call▁begin｜>
StrReplace