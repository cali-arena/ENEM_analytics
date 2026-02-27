"""
Schema mapping validation — ENEM Opportunity & Equity Radar (2020–2024).

CRISP-DM: Data Preparation. Validates that raw (Bronze) schema per year
aligns with the canonical schema and mapping_por_ano.yml.
Outputs: missing_columns, extra_columns, type_conflicts.
Fails if any required canonical column is missing for a year.

Usage: python pipelines/validate_mapping.py
       python pipelines/validate_mapping.py --bronze-dir data/bronze

No Pandas. Spark used only for schema inspection.
"""
import json
import logging
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    raise ImportError("PyYAML required: pip install pyyaml")

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

CONTRACTS_DIR = ROOT / "contracts"
SCHEMA_CANONICO = CONTRACTS_DIR / "schema_canonico.yml"
MAPPING_POR_ANO = CONTRACTS_DIR / "mapping_por_ano.yml"
DEFAULT_BRONZE_DIR = ROOT / "data" / "bronze"
ANOS = [2020, 2021, 2022, 2023, 2024]
BRONZE_META_COLS = {"ano", "ingest_ts", "source_file", "row_count"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# YAML and canonical schema
# -----------------------------------------------------------------------------

def load_canonical_schema(path: Path) -> dict:
    """Load schema_canonico.yml; return dict with columns (name, type, required)."""
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    canonical = {}
    for col in data.get("columns", []):
        name = col.get("name")
        if not name:
            continue
        canonical[name] = {
            "type": col.get("type", "string"),
            "required": bool(col.get("required", False)),
        }
    return canonical


def load_mapping(path: Path) -> dict:
    """Load mapping_por_ano.yml; return { year_2020: { mappings, null_source_columns }, ... }."""
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    result = {}
    for key, val in data.items():
        if not key.startswith("year_"):
            continue
        year = int(key.replace("year_", ""))
        mappings = {}
        null_sources = []
        if isinstance(val, dict):
            null_sources = val.get("null_source_columns") or []
            for k, v in val.items():
                if k == "null_source_columns":
                    continue
                if isinstance(v, str):
                    mappings[k] = v  # raw_col -> canonical_col
        result[year] = {"mappings": mappings, "null_source_columns": null_sources}
    return result


# -----------------------------------------------------------------------------
# Spark schema from Bronze
# -----------------------------------------------------------------------------

def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("ENEM-Validate-Mapping")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def spark_type_name(dt) -> str:
    """Normalize Spark type to string for comparison."""
    if type(dt) == StringType:
        return "string"
    if type(dt) in (IntegerType, LongType):
        return "int"
    if type(dt) in (DoubleType, FloatType):
        return "double"
    return dt.simpleString() if hasattr(dt, "simpleString") else str(dt)


def type_compatible(canonical_type: str, spark_type_str: str) -> bool:
    """Canonical type (string/int/double) vs Spark-reported type string."""
    c = (canonical_type or "string").lower()
    s = (spark_type_str or "").lower()
    if c == "string":
        return s in ("string", "str")
    if c == "int":
        return s in ("int", "integer", "long", "bigint")
    if c == "double":
        return s in ("double", "float")
    return c == s


def get_raw_schema_for_year(spark: SparkSession, bronze_dir: Path, ano: int) -> dict:
    """
    Read Bronze Parquet for the year; exclude metadata columns.
    Return dict: raw_column_name -> spark_type_string
    """
    path = bronze_dir / f"enem_{ano}.parquet"
    if not path.exists():
        return {}
    df = spark.read.parquet(str(path))
    out = {}
    for f in df.schema.fields:
        if f.name in BRONZE_META_COLS:
            continue
        out[f.name] = spark_type_name(f.dataType)
    return out


# -----------------------------------------------------------------------------
# Comparison logic
# -----------------------------------------------------------------------------

def validate_year(
    ano: int,
    raw_schema: dict,
    mapping: dict,
    canonical: dict,
) -> tuple[list, list, list]:
    """
    For one year: compute missing_columns, extra_columns, type_conflicts.
    missing_columns: list of canonical column names that have no valid source.
    extra_columns: raw column names not used in mapping.
    type_conflicts: list of { canonical, raw_column, expected_type, actual_type }.
    """
    mappings = mapping.get("mappings", {})
    null_sources = mapping.get("null_source_columns") or []
    raw_cols = set(raw_schema.keys())

    # Canonical columns that are filled from null (no raw source)
    canonical_from_null = set(null_sources)

    # Canonical columns that have a raw source
    canonical_from_raw = set(mappings.values())

    # For each canonical column: either in null_sources or has a mapping from some raw column
    missing = []
    type_conflicts = []

    for canon_name, canon_spec in canonical.items():
        expected_type = canon_spec.get("type", "string")
        if canon_name in null_sources:
            # Expected to be null-filled; no source column to check
            continue
        # Find raw column that maps to this canonical
        raw_col = None
        for r, c in mappings.items():
            if c == canon_name:
                raw_col = r
                break
        if raw_col is None:
            # Mapping doesn't provide a source for this canonical (and not in null_sources)
            missing.append(canon_name)
            continue
        if raw_col not in raw_cols:
            missing.append(canon_name)
            continue
        actual_type = raw_schema.get(raw_col)
        if not type_compatible(expected_type, actual_type):
            type_conflicts.append({
                "canonical": canon_name,
                "raw_column": raw_col,
                "expected_type": expected_type,
                "actual_type": actual_type,
            })

    # Extra columns: raw columns not referenced in mapping keys
    used_raw = set(mappings.keys())
    extra = sorted(raw_cols - used_raw)

    return missing, extra, type_conflicts


def run_validation(
    spark: SparkSession,
    bronze_dir: Path,
    canonical_path: Path,
    mapping_path: Path,
) -> dict:
    """
    Load contract and mapping, get raw schema per year from Bronze,
    run validate_year for each year. Aggregate and return structured result.
    """
    canonical = load_canonical_schema(canonical_path)
    mapping_by_year = load_mapping(mapping_path)

    missing_columns = {}
    extra_columns = {}
    type_conflicts = {}

    for ano in ANOS:
        raw_schema = get_raw_schema_for_year(spark, bronze_dir, ano)
        mapping = mapping_by_year.get(ano, {"mappings": {}, "null_source_columns": []})

        miss, extra, conflicts = validate_year(ano, raw_schema, mapping, canonical)
        missing_columns[ano] = miss
        extra_columns[ano] = extra
        type_conflicts[ano] = conflicts

    return {
        "missing_columns": missing_columns,
        "extra_columns": extra_columns,
        "type_conflicts": type_conflicts,
        "canonical_columns": list(canonical.keys()),
        "required_columns": [c for c, s in canonical.items() if s.get("required")],
    }


def check_required_and_fail(result: dict) -> None:
    """Raise SystemExit if any required canonical column is missing for any year."""
    required = set(result.get("required_columns") or [])
    missing = result.get("missing_columns") or {}
    failed = []
    for ano, cols in missing.items():
        for c in cols:
            if c in required:
                failed.append((ano, c))
    if failed:
        logger.error("Validation failed: required columns missing: %s", failed)
        sys.exit(1)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Validate Bronze schema vs canonical and mapping")
    parser.add_argument("--bronze-dir", type=str, default=str(DEFAULT_BRONZE_DIR), help="Bronze Parquet directory")
    parser.add_argument("--schema", type=str, default=str(SCHEMA_CANONICO), help="Path to schema_canonico.yml")
    parser.add_argument("--mapping", type=str, default=str(MAPPING_POR_ANO), help="Path to mapping_por_ano.yml")
    args = parser.parse_args()

    bronze_dir = Path(args.bronze_dir)
    schema_path = Path(args.schema)
    mapping_path = Path(args.mapping)

    if not schema_path.exists():
        logger.error("Schema not found: %s", schema_path)
        sys.exit(1)
    if not mapping_path.exists():
        logger.error("Mapping not found: %s", mapping_path)
        sys.exit(1)

    spark = get_spark()
    result = run_validation(spark, bronze_dir, schema_path, mapping_path)
    spark.stop()

    # Structured output (JSON-serializable)
    output = {
        "missing_columns": result["missing_columns"],
        "extra_columns": result["extra_columns"],
        "type_conflicts": result["type_conflicts"],
    }
    print(json.dumps(output, indent=2, ensure_ascii=False))

    check_required_and_fail(result)

    # Log summary
    for ano in ANOS:
        m, e, t = result["missing_columns"].get(ano, []), result["extra_columns"].get(ano, []), result["type_conflicts"].get(ano, [])
        logger.info("Year %s: missing=%s, extra_count=%s, type_conflicts=%s", ano, m, len(e), len(t))
    logger.info("Validation passed: no required columns missing.")


if __name__ == "__main__":
    main()
