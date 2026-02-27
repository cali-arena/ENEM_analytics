"""
CRISP-DM Data Understanding – Bronze profiling pipeline.

Reads raw ENEM CSVs (2020–2024), creates Bronze Parquet tables with metadata,
generates data profiling statistics, detects schema drift, and outputs reports.

No cleaning, no filtering, no Silver. Spark-native only.
Usage: python pipelines/data_understanding_profiling.py
"""
import json
import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DIR_RAW = "data/raw"
DIR_BRONZE = "data/bronze"
DIR_REPORTS = "data/reports"
ANOS = [2020, 2021, 2022, 2023, 2024]

# Critical columns for profiling (may exist as SG_UF_PROVA in raw)
SCORE_COLS = ["NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO"]
PRESENCE_COLS = ["TP_PRESENCA_CN", "TP_PRESENCA_CH", "TP_PRESENCA_LC", "TP_PRESENCA_MT"]
PROFILE_CRITICAL = SCORE_COLS + PRESENCE_COLS + ["SG_UF_RESIDENCIA", "SG_UF_PROVA", "TP_SEXO", "Q006"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _raw_path(ano: int) -> Path:
    for name in (f"enem_{ano}.csv", f"ENEM_{ano}.csv"):
        p = ROOT / DIR_RAW / name
        if p.exists():
            return p
    return ROOT / DIR_RAW / f"enem_{ano}.csv"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("ENEM-Data-Understanding-Profiling")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


# =============================================================================
# 1. Bronze ingestion
# =============================================================================

def ingest_bronze(spark: SparkSession, ano: int):
    """
    Read CSV for one year, add metadata (ano, ingest_ts, source_file, row_count),
    save as data/bronze/enem_{ano}.parquet. No cleaning or schema enforcement.
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
        .option("inferSchema", "true")
        .csv(str(path))
    )

    source_file = path.name
    df = df.withColumn("ano", F.lit(ano).cast("int"))
    df = df.withColumn("ingest_ts", F.current_timestamp())
    df = df.withColumn("source_file", F.lit(source_file))

    # row_count = total rows in this dataset (same value per row)
    w = Window.partitionBy()
    df = df.withColumn("row_count", F.count("*").over(w))

    out_dir = ROOT / DIR_BRONZE
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"enem_{ano}.parquet"
    df.write.mode("overwrite").parquet(str(out_path))
    logger.info("Bronze written: %s", out_path)
    return df


# =============================================================================
# 2. Data profiling per year
# =============================================================================

def _spark_type_name(dt):
    return dt.simpleString() if hasattr(dt, "simpleString") else str(dt)


def profile_numeric(df, col_name):
    """Null count, % null, min, max for a numeric column."""
    total = df.count()
    null_count = df.filter(F.col(col_name).isNull()).count()
    pct_null = (null_count / total * 100) if total else 0
    row = df.agg(
        F.count(col_name).alias("non_null_count"),
        F.min(col_name).alias("min_val"),
        F.max(col_name).alias("max_val"),
        F.mean(col_name).alias("mean_val"),
        F.stddev(col_name).alias("stddev_val"),
    ).collect()[0]
    return {
        "null_count": null_count,
        "pct_null": round(pct_null, 2),
        "min": row["min_val"],
        "max": row["max_val"],
        "mean": round(row["mean_val"], 2) if row["mean_val"] is not None else None,
        "stddev": round(row["stddev_val"], 2) if row["stddev_val"] is not None else None,
    }


def profile_categorical(df, col_name):
    """Null count, % null, approx distinct count."""
    total = df.count()
    null_count = df.filter(F.col(col_name).isNull()).count()
    pct_null = (null_count / total * 100) if total else 0
    approx_distinct = df.agg(F.approx_count_distinct(col_name).alias("d")).collect()[0]["d"]
    return {
        "null_count": null_count,
        "pct_null": round(pct_null, 2),
        "approx_distinct": approx_distinct,
    }


def iqr_outlier_counts(df, col_name):
    """Count of rows below Q1-1.5*IQR or above Q3+1.5*IQR. Spark-native."""
    # percentile_approx returns array [q1, q3] at 0.25, 0.75
    perc = df.agg(
        F.expr(f"percentile_approx(cast({col_name} as double), array(0.25, 0.75), 10000)").alias("p"),
    ).collect()[0]["p"]
    if not perc or len(perc) < 2:
        return {"outlier_count": 0, "pct_outlier": 0.0, "q1": None, "q3": None}
    q1, q3 = float(perc[0]), float(perc[1])
    iqr = q3 - q1
    if iqr <= 0:
        return {"outlier_count": 0, "pct_outlier": 0.0, "q1": q1, "q3": q3}
    low = q1 - 1.5 * iqr
    high = q3 + 1.5 * iqr
    outlier_count = df.filter(
        (F.col(col_name).isNotNull()) & ((F.col(col_name) < low) | (F.col(col_name) > high))
    ).count()
    total = df.count()
    pct = (outlier_count / total * 100) if total else 0
    return {
        "outlier_count": outlier_count,
        "pct_outlier": round(pct, 2),
        "q1": round(q1, 2),
        "q3": round(q3, 2),
        "lower_bound": round(low, 2),
        "upper_bound": round(high, 2),
    }


def is_numeric(dt):
    from pyspark.sql.types import DoubleType, FloatType, IntegerType, LongType, DecimalType
    return type(dt) in (DoubleType, FloatType, IntegerType, LongType, DecimalType)


def profile_dataframe(spark: SparkSession, df, ano: int):
    """
    Compute profiling stats for the DataFrame: row count, column count, types,
    null counts and % per column, min/max numeric, approx distinct categorical,
    IQR outlier counts for score columns.
    """
    total_rows = df.count()
    cols = [c for c in df.columns if c not in ("ano", "ingest_ts", "source_file", "row_count")]
    schema = df.schema
    col_types = {f.name: _spark_type_name(f.dataType) for f in schema if f.name in cols}

    null_stats = {}
    numeric_stats = {}
    categorical_stats = {}
    outlier_stats = {}

    for f in schema:
        name = f.name
        if name in ("ano", "ingest_ts", "source_file", "row_count"):
            continue
        # Null counts
        null_count = df.filter(F.col(name).isNull()).count()
        pct_null = (null_count / total_rows * 100) if total_rows else 0
        null_stats[name] = {"null_count": null_count, "pct_null": round(pct_null, 2)}

        if is_numeric(f.dataType):
            try:
                numeric_stats[name] = profile_numeric(df, name)
            except Exception as e:
                logger.warning("Numeric profile failed for %s: %s", name, e)
        else:
            try:
                categorical_stats[name] = profile_categorical(df, name)
            except Exception as e:
                logger.warning("Categorical profile failed for %s: %s", name, e)

    # IQR outliers for score columns that exist and are numeric
    name_to_type = {f.name: f.dataType for f in schema}
    for c in SCORE_COLS:
        if c in df.columns and is_numeric(name_to_type.get(c)):
            try:
                outlier_stats[c] = iqr_outlier_counts(df, c)
            except Exception as e:
                logger.warning("IQR failed for %s: %s", c, e)

    return {
        "ano": ano,
        "row_count": total_rows,
        "column_count": len(cols),
        "col_types": col_types,
        "null_stats": null_stats,
        "numeric_stats": numeric_stats,
        "categorical_stats": categorical_stats,
        "outlier_stats": outlier_stats,
    }


# =============================================================================
# 3. Schema drift detection (2024 vs 2020–2023)
# =============================================================================

def detect_schema_drift(schemas_by_year):
    """
    Compare 2024 schema to 2020–2023. Return missing_columns_2024, extra_columns_2024,
    type_mismatches, column_count_diff.
    """
    schema_2024 = schemas_by_year.get(2024)
    if not schema_2024:
        return {"missing_columns_2024": [], "extra_columns_2024": [], "type_mismatches": {}, "column_count_diff": {}}

    # Reference: 2023 (or latest available before 2024)
    ref_ano = None
    for a in [2023, 2022, 2021, 2020]:
        if a in schemas_by_year:
            ref_ano = a
            break
    if ref_ano is None:
        return {"missing_columns_2024": [], "extra_columns_2024": [], "type_mismatches": {}, "column_count_diff": schemas_by_year}

    ref_cols = set(schemas_by_year[ref_ano]["columns"])
    ref_types = schemas_by_year[ref_ano]["types"]
    cols_2024 = set(schemas_by_year[2024]["columns"])
    types_2024 = schemas_by_year[2024]["types"]

    missing_in_2024 = sorted(ref_cols - cols_2024)
    extra_in_2024 = sorted(cols_2024 - ref_cols)

    type_mismatches = {}
    common = ref_cols & cols_2024
    for c in common:
        t_ref = ref_types.get(c)
        t_24 = types_2024.get(c)
        if t_ref and t_24 and t_ref != t_24:
            type_mismatches[c] = {"reference": t_ref, "2024": t_24}

    column_count_diff = {str(ano): schemas_by_year[ano]["column_count"] for ano in sorted(schemas_by_year.keys())}

    # Order: list columns in reference vs 2024 (to detect order differences)
    ref_order = [c for c in schemas_by_year[ref_ano].get("column_order", [])]
    order_2024 = [c for c in schemas_by_year[2024].get("column_order", [])]
    if not ref_order and ref_cols:
        ref_order = sorted(ref_cols)
    if not order_2024 and cols_2024:
        order_2024 = sorted(cols_2024)

    return {
        "missing_columns_2024": missing_in_2024,
        "extra_columns_2024": extra_in_2024,
        "type_mismatches": type_mismatches,
        "column_count_diff": column_count_diff,
        "column_order_reference": ref_order,
        "column_order_2024": order_2024,
    }


# =============================================================================
# 4. Profiling summary (for Parquet/CSV report)
# =============================================================================

def build_profiling_summary_df(spark, profile_results):
    """
    One row per year: ano, row_count, col_count, %_null_scores, %_null_presence, %_null_income.
    """
    rows = []
    for pr in profile_results:
        ano = pr["ano"]
        row_count = pr["row_count"]
        col_count = pr["column_count"]
        null_stats = pr.get("null_stats", {})

        def avg_pct_null(keys):
            present = [null_stats[k]["pct_null"] for k in keys if k in null_stats]
            return round(sum(present) / len(present), 2) if present else None

        pct_null_scores = avg_pct_null(SCORE_COLS)
        pct_null_presence = avg_pct_null(PRESENCE_COLS)
        pct_null_income = null_stats.get("Q006", {}).get("pct_null") if "Q006" in null_stats else None

        rows.append((ano, row_count, col_count, pct_null_scores, pct_null_presence, pct_null_income))

    return spark.createDataFrame(rows, ["ano", "row_count", "col_count", "pct_null_scores", "pct_null_presence", "pct_null_income"])


# =============================================================================
# 5. Console output
# =============================================================================

def print_validation(profile_results, drift_report):
    """Rows per year, top 5 columns by missing %, columns changed in 2024, score stats."""
    logger.info("========== ROWS PER YEAR ==========")
    for pr in profile_results:
        logger.info("  %s: %s rows", pr["ano"], pr["row_count"])

    # Top 5 columns with highest missing % (aggregate across years)
    all_null_pct = {}
    for pr in profile_results:
        for col_name, stats in pr.get("null_stats", {}).items():
            pct = stats["pct_null"]
            all_null_pct[col_name] = max(all_null_pct.get(col_name, 0), pct)
    top5_missing = sorted(all_null_pct.items(), key=lambda x: -x[1])[:5]
    logger.info("========== TOP 5 COLUMNS BY MISSING %% (max across years) ==========")
    for col_name, pct in top5_missing:
        logger.info("  %s: %s%%", col_name, pct)

    logger.info("========== SCHEMA CHANGES IN 2024 ==========")
    logger.info("  Missing in 2024: %s", drift_report.get("missing_columns_2024", []))
    logger.info("  Extra in 2024: %s", drift_report.get("extra_columns_2024", []))
    logger.info("  Type mismatches: %s", drift_report.get("type_mismatches", {}))

    logger.info("========== DESCRIPTIVE STATS FOR SCORE COLUMNS (2024) ==========")
    pr_24 = next((p for p in profile_results if p["ano"] == 2024), None)
    if pr_24:
        for c in SCORE_COLS:
            if c in pr_24.get("numeric_stats", {}):
                s = pr_24["numeric_stats"][c]
                logger.info("  %s: min=%s max=%s mean=%s pct_null=%s", c, s.get("min"), s.get("max"), s.get("mean"), pr_24.get("null_stats", {}).get(c, {}).get("pct_null"))
            if c in pr_24.get("outlier_stats", {}):
                o = pr_24["outlier_stats"][c]
                logger.info("    IQR outlier count=%s pct=%s", o.get("outlier_count"), o.get("pct_outlier"))


# =============================================================================
# Main
# =============================================================================

def main():
    spark = get_spark()
    (ROOT / DIR_BRONZE).mkdir(parents=True, exist_ok=True)
    (ROOT / DIR_REPORTS).mkdir(parents=True, exist_ok=True)

    profile_results = []
    schemas_by_year = {}

    for ano in ANOS:
        try:
            df = ingest_bronze(spark, ano)
        except FileNotFoundError as e:
            logger.warning("Skip %s: %s", ano, e)
            continue

        # Schema for drift (exclude metadata columns)
        meta = {"ano", "ingest_ts", "source_file", "row_count"}
        cols = [c for c in df.columns if c not in meta]
        schemas_by_year[ano] = {
            "columns": set(cols),
            "column_count": len(cols),
            "column_order": cols,
            "types": {f.name: _spark_type_name(f.dataType) for f in df.schema if f.name in cols},
        }

        prof = profile_dataframe(spark, df, ano)
        profile_results.append(prof)

    if not profile_results:
        logger.error("No Bronze data produced. Check data/raw for CSVs.")
        spark.stop()
        return

    # Schema drift report
    drift_report = detect_schema_drift(schemas_by_year)
    report_path = ROOT / DIR_REPORTS / "schema_drift_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(drift_report, f, indent=2, ensure_ascii=False)
    logger.info("Schema drift report: %s", report_path)

    # Profiling summary Parquet + CSV
    summary_df = build_profiling_summary_df(spark, profile_results)
    summary_df.write.mode("overwrite").parquet(str(ROOT / DIR_REPORTS / "data_profiling_summary.parquet"))
    summary_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(ROOT / DIR_REPORTS / "data_profiling_summary.csv"))
    logger.info("Profiling summary: data/reports/data_profiling_summary.parquet and .csv")

    # Console validation
    print_validation(profile_results, drift_report)

    spark.stop()
    logger.info("Data Understanding profiling finished.")


if __name__ == "__main__":
    main()
