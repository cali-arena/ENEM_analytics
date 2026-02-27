"""
Silver layer — ENEM Opportunity & Equity Radar (2020–2024).

CRISP-DM: Data Preparation (Part 2). Applies schema_canonico.yml and
mapping_por_ano.yml, enforces types and validation rules, tracks filtered
rows per rule per year, and writes enem_participante + quality_report + null_report.

Usage: python pipelines/silver_cleaning_pipeline.py
"""
import logging
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    raise ImportError("PyYAML required: pip install pyyaml")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

CONTRACTS_DIR = ROOT / "contracts"
SCHEMA_CANONICO_PATH = CONTRACTS_DIR / "schema_canonico.yml"
MAPPING_POR_ANO_PATH = CONTRACTS_DIR / "mapping_por_ano.yml"
DIR_BRONZE = ROOT / "data" / "bronze"
DIR_SILVER = ROOT / "data" / "silver"
ANOS = [2020, 2021, 2022, 2023, 2024]
BRONZE_META = {"ano", "ingest_ts", "source_file", "row_count"}

# Valid Brazilian state codes (2 letters)
VALID_UFS = {
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS",
    "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
}
SCORE_MIN, SCORE_MAX = 0.0, 1000.0
PRESENCE_VALID = {0, 1, 2}
SEXO_VALID = {"M", "F"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Contract loading
# -----------------------------------------------------------------------------

def load_canonical_schema(path: Path) -> dict:
    """Return dict: column_name -> { type, required }."""
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    out = {}
    for col in data.get("columns", []):
        name = col.get("name")
        if name:
            out[name] = {"type": col.get("type", "string"), "required": bool(col.get("required", False))}
    return out


def load_mapping(path: Path) -> dict:
    """Return { year: { mappings: {raw: canonical}, null_source_columns: [] } }."""
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    out = {}
    for key, val in data.items():
        if not key.startswith("year_") or not isinstance(val, dict):
            continue
        year = int(key.replace("year_", ""))
        mappings = {k: v for k, v in val.items() if k != "null_source_columns" and isinstance(v, str)}
        null_sources = val.get("null_source_columns") or []
        out[year] = {"mappings": mappings, "null_source_columns": null_sources}
    return out


# -----------------------------------------------------------------------------
# Spark session
# -----------------------------------------------------------------------------

def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("ENEM-Silver-Cleaning")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


# -----------------------------------------------------------------------------
# Read Bronze and apply mapping
# -----------------------------------------------------------------------------

def read_bronze_year(spark: SparkSession, ano: int) -> "pyspark.sql.DataFrame":
    """Read Bronze Parquet for one year. Exclude metadata columns for mapping."""
    path = DIR_BRONZE / f"enem_{ano}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Bronze not found: {path}")
    df = spark.read.parquet(str(path))
    return df


def apply_mapping(df, ano: int, mapping: dict, canonical_columns: list) -> "pyspark.sql.DataFrame":
    """Select and rename raw columns to canonical; add null for null_source columns."""
    mappings = mapping.get("mappings", {})
    null_sources = mapping.get("null_source_columns") or []
    # Build reverse: canonical -> raw
    canon_to_raw = {v: k for k, v in mappings.items()}
    exprs = []
    for c in canonical_columns:
        if c in null_sources:
            exprs.append(F.lit(None).cast(StringType()).alias(c))
        elif c in canon_to_raw:
            raw = canon_to_raw[c]
            if raw in df.columns:
                exprs.append(F.col(raw).alias(c))
            else:
                exprs.append(F.lit(None).cast(StringType()).alias(c))
        else:
            exprs.append(F.lit(None).cast(StringType()).alias(c))
    return df.select(exprs).withColumn("ano", F.lit(ano).cast(IntegerType()))


# -----------------------------------------------------------------------------
# Type enforcement
# -----------------------------------------------------------------------------

def _cast_column(df, col_name: str, canon_type: str):
    if canon_type == "string":
        return df.withColumn(col_name, F.trim(F.coalesce(F.col(col_name).cast("string"), F.lit(""))).cast(StringType()))
    if canon_type == "int":
        return df.withColumn(col_name, F.col(col_name).cast(IntegerType()))
    if canon_type == "double":
        return df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
    return df


def apply_type_enforcement(df, canonical_schema: dict) -> "pyspark.sql.DataFrame":
    """Cast each column to canonical type. Empty string for string -> null for strictness we treat as null."""
    for col_name, spec in canonical_schema.items():
        t = spec.get("type", "string")
        df = _cast_column(df, col_name, t)
    return df


# -----------------------------------------------------------------------------
# Validation rules (each returns updated DataFrame; we track count before/after externally)
# -----------------------------------------------------------------------------

def rule_presence_allowed(df) -> "pyspark.sql.DataFrame":
    """Keep only TP_PRESENCA_* in [0,1,2]; invalid -> null."""
    for c in ["TP_PRESENCA_CN", "TP_PRESENCA_CH", "TP_PRESENCA_LC", "TP_PRESENCA_MT"]:
        if c in df.columns:
            df = df.withColumn(
                c,
                F.when(F.col(c).cast(IntegerType()).isin(0, 1, 2), F.col(c).cast(IntegerType())).otherwise(F.lit(None).cast(IntegerType())),
            )
    return df


def rule_normalize_uf(df) -> "pyspark.sql.DataFrame":
    """Uppercase; invalid state code -> null."""
    if "SG_UF_RESIDENCIA" not in df.columns:
        return df
    uf_upper = F.upper(F.trim(F.col("SG_UF_RESIDENCIA")))
    valid_expr = F.when(uf_upper.isin(list(VALID_UFS)), uf_upper).otherwise(F.lit(None).cast(StringType()))
    return df.withColumn("SG_UF_RESIDENCIA", valid_expr)


def rule_normalize_sex(df) -> "pyspark.sql.DataFrame":
    """Normalize to M/F; invalid -> null (flagged by null)."""
    if "TP_SEXO" not in df.columns:
        return df
    sex_upper = F.upper(F.trim(F.col("TP_SEXO")))
    valid_expr = F.when(sex_upper.isin(list(SEXO_VALID)), sex_upper).otherwise(F.lit(None).cast(StringType()))
    return df.withColumn("TP_SEXO", valid_expr)


def rule_scores_null_when_absent(df) -> "pyspark.sql.DataFrame":
    """If presence != 1 for that area, set score to null."""
    area_to_presence = {
        "NU_NOTA_CN": "TP_PRESENCA_CN",
        "NU_NOTA_CH": "TP_PRESENCA_CH",
        "NU_NOTA_LC": "TP_PRESENCA_LC",
        "NU_NOTA_MT": "TP_PRESENCA_MT",
    }
    for score_col, pres_col in area_to_presence.items():
        if score_col in df.columns and pres_col in df.columns:
            df = df.withColumn(
                score_col,
                F.when(F.col(pres_col) == 1, F.col(score_col).cast(DoubleType())).otherwise(F.lit(None).cast(DoubleType())),
            )
    return df


def rule_scores_in_range(df) -> "pyspark.sql.DataFrame":
    """Scores and redação must be in [0, 1000] or null. Out of range -> null."""
    score_cols = ["NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO"]
    for c in score_cols:
        if c in df.columns:
            df = df.withColumn(
                c,
                F.when(
                    F.col(c).isNotNull() & (F.col(c) >= SCORE_MIN) & (F.col(c) <= SCORE_MAX),
                    F.col(c).cast(DoubleType()),
                ).otherwise(F.lit(None).cast(DoubleType())),
            )
    return df


def rule_dedup(df) -> "pyspark.sql.DataFrame":
    """Remove duplicated NU_INSCRICAO per year."""
    return df.dropDuplicates(["NU_INSCRICAO", "ano"])


def rule_valid_modeling_cohort(df) -> "pyspark.sql.DataFrame":
    """Keep only participants present in ALL four objective exams (TP_PRESENCA_* == 1)."""
    return df.filter(
        (F.col("TP_PRESENCA_CN") == 1)
        & (F.col("TP_PRESENCA_CH") == 1)
        & (F.col("TP_PRESENCA_LC") == 1)
        & (F.col("TP_PRESENCA_MT") == 1)
    )


# -----------------------------------------------------------------------------
# Per-year processing with audit
# -----------------------------------------------------------------------------

def process_one_year(
    spark: SparkSession,
    ano: int,
    mapping_by_year: dict,
    canonical_schema: dict,
    canonical_columns: list,
    audit_rows: list,
) -> tuple["pyspark.sql.DataFrame", int]:
    """Read Bronze for year, apply mapping + types + rules; record audit per rule. Return (cleaned DF, initial_row_count)."""
    df = read_bronze_year(spark, ano)
    initial_count = df.count()
    mapping = mapping_by_year.get(ano, {"mappings": {}, "null_source_columns": []})
    df = apply_mapping(df, ano, mapping, canonical_columns)
    df = apply_type_enforcement(df, canonical_schema)

    total_at_start = df.count()

    def record(rule_name: str, before: int, after: int):
        removed = before - after
        pct = (removed / before * 100) if before else 0
        audit_rows.append((rule_name, ano, removed, round(pct, 2)))

    # Rule 1: presence allowed values (normalize invalid to null; no row drop)
    df = rule_presence_allowed(df)
    n = df.count()
    record("presence_allowed_values", total_at_start, n)
    before = n

    # Rule 2: normalize UF
    df = rule_normalize_uf(df)
    n = df.count()
    record("normalize_uf", before, n)
    before = n

    # Rule 3: normalize sex
    df = rule_normalize_sex(df)
    n = df.count()
    record("normalize_sex", before, n)
    before = n

    # Rule 4: scores null when absent
    df = rule_scores_null_when_absent(df)
    n = df.count()
    record("scores_null_when_absent", before, n)
    before = n

    # Rule 5: scores in range [0, 1000]
    df = rule_scores_in_range(df)
    n = df.count()
    record("scores_in_range", before, n)
    before = n

    # Rule 6: deduplicate
    df = rule_dedup(df)
    n = df.count()
    record("dedup_nu_inscricao_ano", before, n)
    before = n

    # Rule 7: valid modeling cohort (present in all four exams)
    df = rule_valid_modeling_cohort(df)
    n = df.count()
    record("valid_modeling_cohort_all_present", before, n)

    return df, initial_count


# -----------------------------------------------------------------------------
# Null report
# -----------------------------------------------------------------------------

def build_null_report(spark, silver_df) -> "pyspark.sql.DataFrame":
    """Null % per column per year."""
    cols = [c for c in silver_df.columns if c != "ano"]
    exprs = [F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in cols]
    null_counts = silver_df.groupBy("ano").agg(*exprs)
    total_per_year = silver_df.groupBy("ano").count().withColumnRenamed("count", "total")
    joined = null_counts.join(total_per_year, "ano")
    # Reshape to long: ano, column_name, null_count, total, pct_null
    rows = []
    for row in joined.collect():
        ano = row["ano"]
        total = row["total"]
        for c in cols:
            null_count = row[c]
            pct = (null_count / total * 100) if total else 0
            rows.append((ano, c, null_count, total, round(pct, 2)))
    return spark.createDataFrame(rows, ["ano", "column_name", "null_count", "total_count", "pct_null"])


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    spark = get_spark()
    DIR_SILVER.mkdir(parents=True, exist_ok=True)

    canonical_schema = load_canonical_schema(SCHEMA_CANONICO_PATH)
    canonical_columns = list(canonical_schema.keys())
    mapping_by_year = load_mapping(MAPPING_POR_ANO_PATH)

    audit_rows = []
    dfs = []
    total_before = 0
    total_after = 0

    for ano in ANOS:
        if not (DIR_BRONZE / f"enem_{ano}.parquet").exists():
            logger.warning("Bronze missing for %s, skip", ano)
            continue
        logger.info("Processing year %s ...", ano)
        df, initial_count = process_one_year(spark, ano, mapping_by_year, canonical_schema, canonical_columns, audit_rows)
        total_before += initial_count
        total_after += df.count()
        dfs.append(df)

    if not dfs:
        logger.error("No Bronze data found. Abort.")
        spark.stop()
        sys.exit(1)

    silver_df = dfs[0]
    for d in dfs[1:]:
        silver_df = silver_df.unionByName(d, allowMissingColumns=True)

    # Order columns for consistent schema
    silver_df = silver_df.select(canonical_columns)

    # Write enem_participante partitioned by ano
    participante_path = DIR_SILVER / "enem_participante"
    silver_df.write.mode("overwrite").partitionBy("ano").parquet(str(participante_path))
    logger.info("Written: %s", participante_path)

    # Quality report
    quality_df = spark.createDataFrame(
        [(r[0], r[1], r[2], r[3]) for r in audit_rows],
        ["rule_name", "year", "rows_removed", "percentage_removed"],
    )
    quality_path = DIR_SILVER / "quality_report.parquet"
    quality_df.write.mode("overwrite").parquet(str(quality_path))
    logger.info("Written: %s", quality_path)

    # Null report
    null_df = build_null_report(spark, silver_df)
    null_path = DIR_SILVER / "null_report.parquet"
    null_df.write.mode("overwrite").parquet(str(null_path))
    logger.info("Written: %s", null_path)

    # Summary
    final_count = silver_df.count()
    pct_kept = (final_count / total_before * 100) if total_before else 0
    logger.info("========== SILVER SUMMARY ==========")
    logger.info("Total rows before: %s", total_before)
    logger.info("Total rows after:  %s", final_count)
    logger.info("%% kept: %.2f%%", pct_kept)

    # Top 3 filtering rules by total rows removed
    rule_totals = {}
    for rule_name, year, removed, pct in audit_rows:
        rule_totals[rule_name] = rule_totals.get(rule_name, 0) + removed
    top3 = sorted(rule_totals.items(), key=lambda x: -x[1])[:3]
    logger.info("Top 3 filtering rules (by rows removed):")
    for rule, removed in top3:
        logger.info("  %s: %s", rule, removed)

    spark.stop()
    logger.info("Silver pipeline finished.")


if __name__ == "__main__":
    main()
