"""
ENEM Opportunity & Equity Radar — ML Explainability (CRISP-DM: Modeling).

Loads trained baseline (Ridge) and strong (GBT) models, extracts coefficients and
feature importances mapped to feature names, and produces presentation-ready reports.

Outputs:
- reports/top_5_features.json
- reports/top_20_features.csv
- reports/explainability_summary.md

SHAP: Not used. TreeExplainer would require exporting trees to a SHAP-compatible format
or running SHAP on a sample in a non-Spark context, adding heavy dependencies (shap,
possibly pandas/sklearn). Spark ML's built-in featureImportances (gain-based) is
standard for tree models and is PySpark-native with no extra frameworks.
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

from pyspark.ml.regression import GBTRegressionModel, LinearRegressionModel
from pyspark.ml.pipeline import PipelineModel

# -----------------------------------------------------------------------------
# Paths and feature metadata (must match train_ml_temporal.py pipeline order)
# -----------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DIR_MODELS = ROOT / "models"
DIR_REPORTS = ROOT / "reports"

PATH_PREP = DIR_MODELS / "prep_model"
PATH_BASELINE = DIR_MODELS / "baseline_model"
PATH_STRONG = DIR_MODELS / "strong_model"

# Order must match VectorAssembler inputCols in train_ml_temporal.py
FEATURE_NAMES = [
    "uf_idx",
    "sexo_idx",
    "uf_renda_idx",
    "renda_ordinal",
    "ano_numeric",
    "media_objetiva",
    "renda_x_ano",
]

# Human-readable labels and one-line business interpretation for top features
FEATURE_INTERPRETATION = {
    "uf_idx": "State of residence (UF). Regional differences in schooling and exam conditions.",
    "sexo_idx": "Gender. Associated with different score distributions in the data.",
    "uf_renda_idx": "State × income bracket. Captures regional inequality by socioeconomic level.",
    "renda_ordinal": "Household income bracket (Q006). Higher income is associated with higher scores.",
    "ano_numeric": "Exam year. Captures trend and policy/context changes over time.",
    "media_objetiva": "Average of objective exam scores (CN, CH, LC, MT). Strong predictor of essay performance.",
    "renda_x_ano": "Income × year. How the income–score relationship changes over time.",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def load_models(spark):
    """Load prep pipeline and baseline/strong regression models."""
    prep = PipelineModel.load(str(PATH_PREP))
    baseline = LinearRegressionModel.load(str(PATH_BASELINE))
    strong = GBTRegressionModel.load(str(PATH_STRONG))
    logger.info("Loaded prep, baseline (Ridge), strong (GBT) models")
    return prep, baseline, strong


def _vector_to_list(v):
    """Convert Spark DenseVector or SparseVector to list by index 0..n-1."""
    n = len(v)
    if hasattr(v, "toArray"):
        return list(v.toArray())
    if hasattr(v, "values") and not hasattr(v, "indices"):
        return list(v.values)
    if hasattr(v, "indices") and hasattr(v, "values"):
        out = [0.0] * n
        for i, x in zip(v.indices, v.values):
            out[i] = float(x)
        return out
    return [float(v[i]) for i in range(n)]


def extract_baseline_coefficients(baseline_model):
    """Map Ridge coefficients to feature names. Returns list of (name, coefficient)."""
    coefs = _vector_to_list(baseline_model.coefficients)
    if len(coefs) != len(FEATURE_NAMES):
        raise ValueError(
            f"Coefficient length {len(coefs)} does not match feature count {len(FEATURE_NAMES)}"
        )
    return list(zip(FEATURE_NAMES, coefs))


def extract_strong_importances(strong_model):
    """Map GBT feature importances to feature names. Returns list of (name, importance)."""
    imp = strong_model.featureImportances
    values = _vector_to_list(imp)
    if len(values) != len(FEATURE_NAMES):
        raise ValueError(
            f"Importance length {len(values)} does not match feature count {len(FEATURE_NAMES)}"
        )
    return list(zip(FEATURE_NAMES, values))


def build_ranked_importance(strong_importances, baseline_coefs):
    """Merge strong model importances with baseline coefficients; sort by strong importance desc."""
    by_name = {name: coef for name, coef in baseline_coefs}
    rows = []
    for name, imp in strong_importances:
        rows.append({
            "feature": name,
            "importance": round(imp, 6),
            "coefficient_baseline": round(by_name.get(name, 0.0), 6),
        })
    rows.sort(key=lambda x: -x["importance"])
    for i, r in enumerate(rows, start=1):
        r["rank"] = i
    return rows


def write_top5_json(ranked, out_path):
    """Write top 5 features in required JSON format with interpretation."""
    top5 = ranked[:5]
    payload = [
        {
            "feature": r["feature"],
            "importance": r["importance"],
            "interpretation": FEATURE_INTERPRETATION.get(
                r["feature"], "Feature used by the model for prediction."
            ),
        }
        for r in top5
    ]
    DIR_REPORTS.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    logger.info("Wrote %s", out_path)


def write_top20_csv(ranked, out_path):
    """Write top 20 (or all) features to CSV: feature, rank, importance, coefficient_baseline."""
    DIR_REPORTS.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("feature,rank,importance_strong,coefficient_baseline\n")
        for r in ranked:
            f.write(
                f"{r['feature']},{r['rank']},{r['importance']:.6f},{r['coefficient_baseline']:.6f}\n"
            )
    logger.info("Wrote %s", out_path)


def write_explainability_summary(ranked, out_path):
    """Write concise, slide-ready explainability summary in Markdown."""
    top5 = ranked[:5]
    lines = [
        "# Explainability Summary — ENEM Redação Model",
        "",
        "## Top 5 drivers (by feature importance, GBT model)",
        "",
    ]
    for i, r in enumerate(top5, start=1):
        interp = FEATURE_INTERPRETATION.get(r["feature"], "Feature used for prediction.")
        lines.append(f"{i}. **{r['feature']}** (importance: {r['importance']:.4f})  ")
        lines.append(f"   {interp}")
        lines.append("")

    lines.extend([
        "---",
        "",
        "## What “importance” means",
        "",
        "For the tree model (GBT), **importance** is the total gain in accuracy from splits on that feature. "
        "Higher values mean the feature is used more often and/or in more impactful splits to predict essay score. "
        "It does not indicate direction (positive vs negative effect); it only indicates how much the feature helps the model.",
        "",
        "For the linear model (Ridge), **coefficients** show direction and scale: a positive coefficient means higher feature value is associated with higher predicted score, and vice versa.",
        "",
        "---",
        "",
        "## Caveats",
        "",
        "- **Correlation ≠ causation.** Important features are associated with the target in the data; they do not prove that changing the feature would change the score.",
        "- **Proxies.** Some features (e.g. income bracket, region) are proxies for opportunity and context; the model reflects existing inequities rather than “fair” rules.",
        "- **Fairness.** Using demographic or socioeconomic features in policy or high-stakes decisions may reinforce disparities; monitor and assess fairness in deployment.",
        "",
    ])
    DIR_REPORTS.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    logger.info("Wrote %s", out_path)


def main():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("ENEM-Explainability").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    prep, baseline, strong = load_models(spark)
    baseline_coefs = extract_baseline_coefficients(baseline)
    strong_importances = extract_strong_importances(strong)
    ranked = build_ranked_importance(strong_importances, baseline_coefs)

    write_top5_json(ranked, DIR_REPORTS / "top_5_features.json")
    write_top20_csv(ranked, DIR_REPORTS / "top_20_features.csv")
    write_explainability_summary(ranked, DIR_REPORTS / "explainability_summary.md")

    logger.info("Explainability report finished.")


if __name__ == "__main__":
    main()
