"""
Bronze-only step: raw CSV → data/bronze Parquet per year.

Uses pipelines.data_quality_pipeline.load_bronze. Run after coleta (data/raw).
Usage: python scripts/run_bronze.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipelines.data_quality_pipeline import (
    ANOS,
    DIR_BRONZE,
    ROOT,
    get_spark,
    load_bronze,
)


def main():
    (ROOT / DIR_BRONZE).mkdir(parents=True, exist_ok=True)
    spark = get_spark()
    for ano in ANOS:
        try:
            load_bronze(spark, ano)
        except FileNotFoundError as e:
            print(f"[WARN] Skip {ano}: {e}")
    spark.stop()
    print("Bronze step done.")


if __name__ == "__main__":
    main()
