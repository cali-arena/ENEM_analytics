"""
Lakehouse Data Quality Pipeline – ENEM 2020–2024.

Bronze → Silver → Gold refinement with schema harmonization, validation,
and a unified Parquet dataset ready for ML modeling.

Usage: python pipelines/data_quality_pipeline.py

Constraints: PySpark only, no Pandas. Spark-native transformations.
"""
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# -----------------------------------------------------------------------------
# Paths and config
# -----------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DIR_RAW = "data/raw"
DIR_BRONZE = "data/bronze"
DIR_SILVER = "data/silver"
DIR_GOLD = "data/gold"
ANOS = [2020, 2021, 2022, 2023, 2024]

# CSV: project uses ENEM_2020.csv; also support user-assumed enem_2020.csv
def _raw_path(ano: int) -> Path:
    for name in (f"enem_{ano}.csv", f"ENEM_{ano}.csv"):
        p = ROOT / DIR_RAW / name
        if p.exists():
            return p
    return ROOT / DIR_RAW / f"enem_{ano}.csv"

# -----------------------------------------------------------------------------
# Canonical schema (model-ready + audit fields for Bronze)
# -----------------------------------------------------------------------------
CANONICAL_FIELDS = [
    ("NU_INSCRICAO", StringType(), True),
    ("SG_UF_RESIDENCIA", StringType(), True),
    ("TP_SEXO", StringType(), True),
    ("TP_PRESENCA_CN", IntegerType(), True),
    ("TP_PRESENCA_CH", IntegerType(), True),
    ("TP_PRESENCA_LC", IntegerType(), True),
    ("TP_PRESENCA_MT", IntegerType(), True),
    ("NU_NOTA_CN", DoubleType(), True),
    ("NU_NOTA_CH", DoubleType(), True),
    ("NU_NOTA_LC", DoubleType(), True),
    ("NU_NOTA_MT", DoubleType(), True),
    ("NU_NOTA_REDACAO", DoubleType(), True),
    ("Q006", StringType(), True),
    ("ano", IntegerType(), False),
]
CANONICAL_SCHEMA = StructType([
    StructField(name, dtype, nullable)
    for name, dtype, nullable in CANONICAL_FIELDS
])

# Bronze adds metadata
BRONZE_EXTRA = [
    ("ingestion_ts", TimestampType(), True),
    ("source_file", StringType(), True),
]
BRONZE_SCHEMA = StructType(
    [StructField(n, t, null) for n, t, null in CANONICAL_FIELDS + BRONZE_EXTRA]
)

# Valid ENEM score range
SCORE_MIN, SCORE_MAX = 0.0, 1000.0
REDACAO_MIN, REDACAO_MAX = 0.0, 1000.0

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_spark() -> SparkSession:
    """Build Spark session with sensible defaults for this pipeline."""
    return (
        SparkSession.builder.appName("ENEM-Lakehouse-DQ")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


# =============================================================================
# 1. Canonical schema & harmonization
# =============================================================================

def _safe_double(c: str):
    """Cast column to double; empty string and dot become null."""
    return F.when(
        F.trim(F.col(c)).isin("", ".", " "),
        F.lit(None).cast(DoubleType()),
    ).otherwise(F.col(c).cast(DoubleType()))


def _safe_int(c: str):
    """Cast column to int; empty string and dot become null."""
    return F.when(
        F.trim(F.col(c)).isin("", ".", " "),
        F.lit(None).cast(IntegerType()),
    ).otherwise(F.col(c).cast(IntegerType()))


def _safe_string(c: str):
    """Cast column to string; null for empty."""
    return F.when(
        F.trim(F.col(c)).isin("", ".", " "),
        F.lit(None).cast(StringType()),
    ).otherwise(F.trim(F.col(c)).cast(StringType()))


def harmonize_schema(df, ano: int, include_bronze_meta: bool = False):
    """
    Align DataFrame to canonical schema. Missing columns become null.
    ENEM uses SG_UF_PROVA (exam UF); we expose it as SG_UF_RESIDENCIA for canonical name.
    """
    # Map source columns to canonical (INEP uses SG_UF_PROVA, we alias to SG_UF_RESIDENCIA)
    alias_uf = "SG_UF_PROVA" if "SG_UF_PROVA" in df.columns else None
    cols = list(df.columns)

    # Build canonical columns with explicit types
    out = df.withColumn("ano", F.lit(ano).cast(IntegerType()))

    # NU_INSCRICAO
    if "NU_INSCRICAO" in cols:
        out = out.withColumn("NU_INSCRICAO", _safe_string("NU_INSCRICAO"))
    else:
        out = out.withColumn("NU_INSCRICAO", F.lit(None).cast(StringType()))

    # SG_UF_RESIDENCIA (from SG_UF_PROVA when present)
    if alias_uf:
        out = out.withColumn("SG_UF_RESIDENCIA", _safe_string(alias_uf))
    elif "SG_UF_RESIDENCIA" in cols:
        out = out.withColumn("SG_UF_RESIDENCIA", _safe_string("SG_UF_RESIDENCIA"))
    else:
        out = out.withColumn("SG_UF_RESIDENCIA", F.lit(None).cast(StringType()))

    # TP_SEXO
    if "TP_SEXO" in cols:
        out = out.withColumn("TP_SEXO", _safe_string("TP_SEXO"))
    else:
        out = out.withColumn("TP_SEXO", F.lit(None).cast(StringType()))

    # Presence flags (int: 0/1)
    for c in ("TP_PRESENCA_CN", "TP_PRESENCA_CH", "TP_PRESENCA_LC", "TP_PRESENCA_MT"):
        if c in cols:
            out = out.withColumn(c, _safe_int(c))
        else:
            out = out.withColumn(c, F.lit(None).cast(IntegerType()))

    # Scores
    for c in ("NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO"):
        if c in cols:
            out = out.withColumn(c, _safe_double(c))
        else:
            out = out.withColumn(c, F.lit(None).cast(DoubleType()))

    # Q006 (income bracket)
    if "Q006" in cols:
        out = out.withColumn("Q006", _safe_string("Q006"))
    else:
        out = out.withColumn("Q006", F.lit(None).cast(StringType()))

    canonical_names = [f[0] for f in CANONICAL_FIELDS]
    out = out.select(canonical_names)

    if include_bronze_meta:
        out = out.withColumn("ingestion_ts", F.lit(datetime.now(timezone.utc)))
        out = out.withColumn("source_file", F.lit(f"enem_{ano}.csv"))

    return out


# =============================================================================
# 2. Bronze layer
# =============================================================================

def load_bronze(spark: SparkSession, ano: int):
    """
    Read CSV for one year, add ano + metadata, cast to canonical schema,
    and return DataFrame. Also writes data/bronze/enem_{ano}.parquet.
    """
    path = _raw_path(ano)
    if not path.exists():
        raise FileNotFoundError(f"Raw file not found: {path}")

    logger.info("Bronze: reading %s", path)
    df = (
        spark.read.option("header", "true")
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .option("nullValue", "")
        .csv(str(path))
    )

    row_count_before = df.count()
    col_count = len(df.columns)

    # Harmonize to canonical + Bronze metadata
    df = harmonize_schema(df, ano, include_bronze_meta=True)

    # Null summary (canonical columns only)
    null_summary = {}
    for c in df.columns:
        if c in ("ingestion_ts", "source_file"):
            continue
        null_summary[c] = df.filter(F.col(c).isNull()).count()

    out_dir = ROOT / DIR_BRONZE
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"enem_{ano}.parquet"
    df.write.mode("overwrite").parquet(str(out_path))

    logger.info(
        "Bronze [%s]: rows=%s, columns=%s, written to %s",
        ano, row_count_before, col_count, out_path,
    )
    logger.info("Bronze [%s] null summary: %s", ano, null_summary)

    return df


# =============================================================================
# 3. Silver layer (cleaning + validation)
# =============================================================================

def clean_silver(spark: SparkSession):
    """
    Read all Bronze Parquets, apply presence + score validation, dedup,
    normalize TP_SEXO and SG_UF_RESIDENCIA. Write data/silver/enem_cleaned.parquet
    and data/silver/data_quality_report.json.
    """
    bronze_dir = ROOT / DIR_BRONZE
    if not bronze_dir.exists():
        raise FileNotFoundError(f"Bronze directory not found: {bronze_dir}")

    dfs = []
    for ano in ANOS:
        p = bronze_dir / f"enem_{ano}.parquet"
        if p.exists():
            dfs.append(spark.read.parquet(str(p)))
        else:
            logger.warning("Bronze file not found for year %s, skipping", ano)

    if not dfs:
        raise FileNotFoundError("No Bronze Parquet files found.")

    raw = dfs[0]
    for d in dfs[1:]:
        raw = raw.unionByName(d, allowMissingColumns=True)

    total_raw_rows = raw.count()
    logger.info("Silver: total raw rows from Bronze = %s", total_raw_rows)

    # Drop Bronze-only metadata for Silver (we keep canonical + ano for audit we can keep ingestion_ts if needed; spec says "audit logs" so keep in Bronze only and drop for Silver/Gold)
    canonical_cols = [f[0] for f in CANONICAL_FIELDS]
    if "ingestion_ts" in raw.columns:
        raw = raw.drop("ingestion_ts", "source_file")

    # Presence rule: keep only participants present in all 4 objective exams
    presence_rule = (
        (F.col("TP_PRESENCA_CN") == 1)
        & (F.col("TP_PRESENCA_CH") == 1)
        & (F.col("TP_PRESENCA_LC") == 1)
        & (F.col("TP_PRESENCA_MT") == 1)
    )
    df = raw.filter(presence_rule)
    after_presence = df.count()
    filtered_presence = total_raw_rows - after_presence

    # Score validation: scores > 0 and not null; redação 0–1000; objective 0–1000
    score_ok = (
        F.col("NU_NOTA_CN").isNotNull() & (F.col("NU_NOTA_CN") > 0)
        & (F.col("NU_NOTA_CH").isNotNull() & (F.col("NU_NOTA_CH") > 0))
        & (F.col("NU_NOTA_LC").isNotNull() & (F.col("NU_NOTA_LC") > 0))
        & (F.col("NU_NOTA_MT").isNotNull() & (F.col("NU_NOTA_MT") > 0))
        & (F.col("NU_NOTA_CN") <= SCORE_MAX) & (F.col("NU_NOTA_CN") >= SCORE_MIN)
        & (F.col("NU_NOTA_CH") <= SCORE_MAX) & (F.col("NU_NOTA_CH") >= SCORE_MIN)
        & (F.col("NU_NOTA_LC") <= SCORE_MAX) & (F.col("NU_NOTA_LC") >= SCORE_MIN)
        & (F.col("NU_NOTA_MT") <= SCORE_MAX) & (F.col("NU_NOTA_MT") >= SCORE_MIN)
    )
    redacao_ok = (
        F.col("NU_NOTA_REDACAO").isNotNull()
        & (F.col("NU_NOTA_REDACAO") >= REDACAO_MIN)
        & (F.col("NU_NOTA_REDACAO") <= REDACAO_MAX)
    )
    df = df.filter(score_ok & redacao_ok)
    after_scores = df.count()
    invalid_scores_removed = after_presence - after_scores

    # Remove rows with all scores null (should be rare after above)
    any_score = (
        F.col("NU_NOTA_CN").isNotNull()
        | F.col("NU_NOTA_CH").isNotNull()
        | F.col("NU_NOTA_LC").isNotNull()
        | F.col("NU_NOTA_MT").isNotNull()
        | F.col("NU_NOTA_REDACAO").isNotNull()
    )
    df = df.filter(any_score)

    # Duplicates: keep first per (NU_INSCRICAO, ano)
    before_dedup = df.count()
    df = df.dropDuplicates(["NU_INSCRICAO", "ano"])
    after_dedup = df.count()
    duplicates_removed = before_dedup - after_dedup

    # Invalid categorical: TP_SEXO only M/F
    df = df.filter(
        F.col("TP_SEXO").isNull()
        | F.upper(F.col("TP_SEXO")).isin("M", "F")
    )
    after_sex = df.count()
    invalid_cat_removed = after_dedup - after_sex

    # Normalize: TP_SEXO → M/F only; SG_UF_RESIDENCIA uppercase
    df = df.withColumn("TP_SEXO", F.upper(F.trim(F.col("TP_SEXO"))))
    df = df.withColumn(
        "SG_UF_RESIDENCIA",
        F.when(F.col("SG_UF_RESIDENCIA").isNotNull(), F.upper(F.trim(F.col("SG_UF_RESIDENCIA"))))
        .otherwise(F.col("SG_UF_RESIDENCIA")),
    )

    total_valid_rows = df.count()
    pct_kept = (total_valid_rows / total_raw_rows * 100) if total_raw_rows else 0

    # Persist quality report
    silver_dir = ROOT / DIR_SILVER
    silver_dir.mkdir(parents=True, exist_ok=True)
    report = {
        "total_raw_rows": total_raw_rows,
        "total_valid_rows": total_valid_rows,
        "pct_filtered": round(100 - pct_kept, 2),
        "pct_valid_kept": round(pct_kept, 2),
        "duplicates_removed": duplicates_removed,
        "filtered_presence_rule": filtered_presence,
        "invalid_scores_removed": invalid_scores_removed,
        "invalid_categorical_removed": invalid_cat_removed,
    }
    report_path = silver_dir / "data_quality_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    logger.info("Silver: data quality report written to %s", report_path)

    out_parquet = silver_dir / "enem_cleaned.parquet"
    df.write.mode("overwrite").parquet(str(out_parquet))
    logger.info("Silver: cleaned data written to %s (%s rows)", out_parquet, total_valid_rows)

    return df


# =============================================================================
# 4. Gold layer (model-ready + unified)
# =============================================================================

def build_gold(spark: SparkSession):
    """
    From Silver: add MEDIA_OBJETIVA, REDACAO_800_PLUS, optional Q006 ordinal.
    Remove unused columns. Partition by ano. Write data/gold/enem_model_dataset.parquet,
    then merge into data/gold/enem_2020_2024_unified.parquet.
    """
    silver_path = ROOT / DIR_SILVER / "enem_cleaned.parquet"
    if not silver_path.exists():
        raise FileNotFoundError(f"Silver data not found: {silver_path}")

    df = spark.read.parquet(str(silver_path))

    # MEDIA_OBJETIVA = mean of CN, CH, LC, MT
    df = df.withColumn(
        "MEDIA_OBJETIVA",
        (
            F.col("NU_NOTA_CN") + F.col("NU_NOTA_CH")
            + F.col("NU_NOTA_LC") + F.col("NU_NOTA_MT")
        ) / 4.0,
    )

    # REDACAO_800_PLUS: 1 if NU_NOTA_REDACAO >= 800 else 0
    df = df.withColumn(
        "REDACAO_800_PLUS",
        F.when(F.col("NU_NOTA_REDACAO") >= 800, 1).otherwise(0),
    )

    # Q006 ordinal: ENEM income bracket A, B, C, ... (ordinal 1, 2, 3, ...); no UDF.
    q006_ordinal = F.lit(0)
    for i, code in enumerate(["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q"], start=1):
        q006_ordinal = F.when(F.upper(F.trim(F.col("Q006"))) == code, i).otherwise(q006_ordinal)
    df = df.withColumn("Q006_ORDINAL", q006_ordinal)

    # Model-ready columns: keep canonical + derived, drop only truly unused
    gold_cols = [
        "NU_INSCRICAO", "SG_UF_RESIDENCIA", "TP_SEXO",
        "TP_PRESENCA_CN", "TP_PRESENCA_CH", "TP_PRESENCA_LC", "TP_PRESENCA_MT",
        "NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO",
        "Q006", "Q006_ORDINAL", "ano",
        "MEDIA_OBJETIVA", "REDACAO_800_PLUS",
    ]
    df = df.select(gold_cols)

    gold_dir = ROOT / DIR_GOLD
    gold_dir.mkdir(parents=True, exist_ok=True)

    # Partition by ano
    model_path = gold_dir / "enem_model_dataset.parquet"
    df.write.mode("overwrite").partitionBy("ano").parquet(str(model_path))
    logger.info("Gold: model dataset written to %s (partitioned by ano)", model_path)

    # Final merge: single file set, same column order, no schema drift
    unified_path = gold_dir / "enem_2020_2024_unified.parquet"
    df.write.mode("overwrite").parquet(str(unified_path))
    logger.info("Gold: unified dataset written to %s", unified_path)

    return df


# =============================================================================
# 5. Validation & main
# =============================================================================

def print_validation(spark: SparkSession):
    """Print total participants per year, mean score per year, % valid rows kept."""
    silver_path = ROOT / DIR_SILVER / "enem_cleaned.parquet"
    if not silver_path.exists():
        logger.warning("Silver Parquet not found; skipping validation stats.")
        return

    df = spark.read.parquet(str(silver_path))
    report_path = ROOT / DIR_SILVER / "data_quality_report.json"
    with open(report_path, encoding="utf-8") as f:
        report = json.load(f)

    logger.info("========== VALIDATION ==========")
    logger.info("Total raw rows (Bronze): %s", report["total_raw_rows"])
    logger.info("Total valid rows (Silver): %s", report["total_valid_rows"])
    logger.info("%% valid rows kept: %s%%", report["pct_valid_kept"])
    logger.info("Duplicates removed: %s", report["duplicates_removed"])
    logger.info("Invalid scores removed: %s", report["invalid_scores_removed"])

    # Participants per year
    by_ano = df.groupBy("ano").agg(F.count("*").alias("participants"))
    logger.info("Participants per year (after cleaning):")
    for row in by_ano.orderBy("ano").collect():
        logger.info("  %s: %s", row["ano"], row["participants"])

    # Mean score per year (average of MEDIA_OBJETIVA concept: (CN+CH+LC+MT)/4)
    df_media = df.withColumn(
        "media_obj",
        (F.col("NU_NOTA_CN") + F.col("NU_NOTA_CH") + F.col("NU_NOTA_LC") + F.col("NU_NOTA_MT")) / 4.0,
    )
    mean_by_ano = df_media.groupBy("ano").agg(F.mean("media_obj").alias("mean_score"))
    logger.info("Mean objective score (CN+CH+LC+MT)/4 per year:")
    for row in mean_by_ano.orderBy("ano").collect():
        logger.info("  %s: %.2f", row["ano"], row["mean_score"] or 0)


def main():
    spark = get_spark()
    (ROOT / DIR_BRONZE).mkdir(parents=True, exist_ok=True)
    (ROOT / DIR_SILVER).mkdir(parents=True, exist_ok=True)
    (ROOT / DIR_GOLD).mkdir(parents=True, exist_ok=True)

    # Bronze: per-year Parquets
    for ano in ANOS:
        try:
            load_bronze(spark, ano)
        except FileNotFoundError as e:
            logger.warning("Skip Bronze %s: %s", ano, e)

    # Silver: cleaned + quality report
    clean_silver(spark)

    # Gold: model dataset + unified
    build_gold(spark)

    # Validation
    print_validation(spark)

    spark.stop()
    logger.info("Pipeline finished.")


if __name__ == "__main__":
    main()
