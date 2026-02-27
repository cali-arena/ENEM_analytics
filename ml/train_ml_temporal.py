"""
ENEM Opportunity & Equity Radar (2020–2024) — ML Training (CRISP-DM: Modeling).

Production-grade temporal ML pipeline:
- Target: NU_NOTA_REDACAO (redacao) — regression.
- Data: Gold layer only (modeled, stable, decision-ready).
- Train: 2020–2023; Test: 2024 (temporal validation).
- Models: Ridge (baseline), GBTRegressor (strong).
- Artifacts: models/baseline_model/, models/strong_model/, reports/ml_metrics.json, reports/predictions_2024.parquet.

Why features come from Gold (not Silver):
- Gold is the modeled, decision-ready layer: star schema with surrogate keys, consistent
  semantics (e.g. faixa_renda, ciclo), and valid-cohort rules already applied. Using Gold
  ensures features align with how the organization will consume the model (reports, APIs).
- Silver is clean and conformed but not decision-optimized: it keeps raw-ish column names,
  may include rows we exclude in Gold (e.g. absent on exam day), and is not the single
  source of truth for analytics. Training on Silver would require duplicating Gold business
  rules and risk drift between training and deployment.

Why temporal split (train 2020–2023, test 2024):
- Prevents leakage: we never use future information when training. Real deployment will
  score new years (e.g. 2025) so evaluating on 2024 simulates that.
- Matches real-world deployment: the model is trained on historical data and applied to
  the next year; temporal validation is the correct way to estimate production performance.
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# -----------------------------------------------------------------------------
# Paths and config
# -----------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DIR_GOLD = ROOT / "data" / "gold"
DIR_MODELS = ROOT / "models"
DIR_REPORTS = ROOT / "reports"

# Gold paths (modeled, stable)
PATH_FATO = DIR_GOLD / "fato_desempenho"
PATH_DIM_GEO = DIR_GOLD / "dim_geografia"
PATH_DIM_PERFIL = DIR_GOLD / "dim_perfil"
PATH_DIM_TEMPO = DIR_GOLD / "dim_tempo"

TARGET_COL = "redacao"  # Option A: NU_NOTA_REDACAO
TRAIN_YEARS = [2020, 2021, 2022, 2023]
TEST_YEAR = 2024
RANDOM_SEED = 42

# Presence flags: In Gold, the fact table is built for "valid cohort" (all presenca_* = 1).
# We do NOT use presence as a feature to avoid any conceptual leakage (e.g. predicting
# redacao using "absent on redacao day"). Including constant columns would add no signal.
# If Silver were used, we would still avoid target-derived flags (e.g. absent on redacao).
FEATURE_COLS_NUMERIC = ["renda_ordinal", "ano", "media_objetiva"]  # after indexing: uf_idx, sexo_idx
FEATURE_COLS_INDEXED = ["uf_idx", "sexo_idx"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("ENEM-ML-Temporal")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


# -----------------------------------------------------------------------------
# Load Gold fact + dimensions
# -----------------------------------------------------------------------------
def load_data(spark: SparkSession):
    """
    Load Gold fact and dimensions. All features must come from Gold (modeled, stable).
    """
    logger.info("Loading Gold tables: fato_desempenho, dim_geografia, dim_perfil, dim_tempo")
    fato = spark.read.parquet(str(PATH_FATO))
    dim_geo = spark.read.parquet(str(PATH_DIM_GEO))
    dim_perfil = spark.read.parquet(str(PATH_DIM_PERFIL))
    dim_tempo = spark.read.parquet(str(PATH_DIM_TEMPO))

    # Join fact to dims to get UF, sexo, faixa_renda, ano (and optional ciclo)
    df = (
        fato
        .join(dim_geo, fato["id_geo"] == dim_geo["id_geo"], "left")
        .join(dim_perfil, fato["id_perfil"] == dim_perfil["id_perfil"], "left")
        .join(dim_tempo, fato["id_tempo"] == dim_tempo["id_tempo"], "left")
    )
    # Ensure we have stable column names from dims (avoid duplicates)
    df = df.select(
        fato["nu_inscricao"],
        fato["ano"],
        fato["id_tempo"],
        fato["id_geo"],
        fato["id_perfil"],
        fato["cn"],
        fato["ch"],
        fato["lc"],
        fato["mt"],
        fato["redacao"],
        fato["media_objetiva"],
        F.coalesce(F.trim(dim_geo["sg_uf_residencia"]), F.lit("NA")).alias("sg_uf_residencia"),
        F.coalesce(F.trim(dim_perfil["tp_sexo"]), F.lit("NA")).alias("tp_sexo"),
        F.coalesce(F.trim(dim_perfil["faixa_renda"]), F.lit("NA")).alias("faixa_renda"),
        dim_tempo["ciclo_pre_pos_pandemia"],
    )
    logger.info("Gold joined row count (all years): %s", df.count())
    return df


# -----------------------------------------------------------------------------
# Ordinal mapping Q006 (faixa_renda): A=1 .. Q=17 (ENEM standard)
# -----------------------------------------------------------------------------
Q006_ORDER = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q"]


def _renda_ordinal_expr():
    """Spark expression: map faixa_renda letter to 1..17, else 0."""
    cases = F.lit(0)
    for i, letter in enumerate(Q006_ORDER, start=1):
        cases = F.when(F.col("faixa_renda") == letter, i).otherwise(cases)
    return cases


# -----------------------------------------------------------------------------
# Build feature dataset (no target-derived fields; no leakage)
# -----------------------------------------------------------------------------
def build_features(df):
    """
    Build feature dataset from Gold columns only.
    - UF: indexed (StringIndexer in pipeline).
    - sexo: indexed.
    - renda: ordinal from faixa_renda (Q006).
    - ano: numeric (year trend).
    - media_objetiva: strong predictor for redacao (not derived from redacao).
    - Optional interaction: renda_ordinal * ano (year trend for income).
    """
    df = df.withColumn("renda_ordinal", _renda_ordinal_expr())
    df = df.withColumn("ano_numeric", F.col("ano").cast("double"))
    # Optional interaction: renda x year trend
    df = df.withColumn("renda_x_ano", F.col("renda_ordinal") * (F.col("ano_numeric") - 2019.0))
    # Optional interaction: renda x UF (concat then index in pipeline)
    df = df.withColumn("uf_renda", F.concat_ws("_", F.col("sg_uf_residencia"), F.col("faixa_renda")))
    # Drop rows with null target (required for training and test)
    df = df.filter(F.col(TARGET_COL).isNotNull())
    return df


# -----------------------------------------------------------------------------
# Train/Test split (temporal)
# -----------------------------------------------------------------------------
def temporal_split(df):
    train = df.filter(F.col("ano").isin(TRAIN_YEARS))
    test = df.filter(F.col("ano") == TEST_YEAR)
    logger.info("Train rows (2020–2023): %s | Test rows (2024): %s", train.count(), test.count())
    return train, test


# -----------------------------------------------------------------------------
# Pipeline: StringIndexer(s) + VectorAssembler
# -----------------------------------------------------------------------------
def _build_prep_pipeline():
    indexer_uf = StringIndexer(
        inputCol="sg_uf_residencia",
        outputCol="uf_idx",
        handleInvalid="keep",
    )
    indexer_sexo = StringIndexer(
        inputCol="tp_sexo",
        outputCol="sexo_idx",
        handleInvalid="keep",
    )
    indexer_uf_renda = StringIndexer(
        inputCol="uf_renda",
        outputCol="uf_renda_idx",
        handleInvalid="keep",
    )
    # Feature vector: uf_idx, sexo_idx, uf_renda_idx, renda_ordinal, ano_numeric, media_objetiva, renda_x_ano
    assembler = VectorAssembler(
        inputCols=[
            "uf_idx",
            "sexo_idx",
            "uf_renda_idx",
            "renda_ordinal",
            "ano_numeric",
            "media_objetiva",
            "renda_x_ano",
        ],
        outputCol="features",
        handleInvalid="keep",
    )
    return Pipeline(stages=[indexer_uf, indexer_sexo, indexer_uf_renda, assembler])


def _feature_list():
    return [
        "uf_idx",
        "sexo_idx",
        "uf_renda_idx",
        "renda_ordinal",
        "ano_numeric",
        "media_objetiva",
        "renda_x_ano",
    ]


# -----------------------------------------------------------------------------
# Train baseline (Ridge) and strong (GBT) models
# -----------------------------------------------------------------------------
def train_models(train_df, prep_pipeline):
    """
    Fit prep pipeline on train, then fit Ridge and GBT on (features, target).
    Returns: (prep_model, baseline_model, strong_model).
    """
    # Fit preprocessing on train only (no test data used)
    prep_model = prep_pipeline.fit(train_df)
    train_featured = prep_model.transform(train_df)

    # Baseline: Ridge (LinearRegression with regParam = Ridge)
    baseline = LinearRegression(
        featuresCol="features",
        labelCol=TARGET_COL,
        regParam=0.1,
        elasticNetParam=0.0,  # pure Ridge
        maxIter=100,
        seed=RANDOM_SEED,
    )
    baseline_model = baseline.fit(train_featured)

    # Strong: GBT
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol=TARGET_COL,
        maxDepth=5,
        maxIter=100,
        seed=RANDOM_SEED,
    )
    strong_model = gbt.fit(train_featured)

    return prep_model, baseline_model, strong_model


# -----------------------------------------------------------------------------
# Evaluate: RMSE, MAE, R2 on test; optional by-year on train
# -----------------------------------------------------------------------------
def evaluate(prep_model, baseline_model, strong_model, train_df, test_df):
    """Compute RMSE, MAE, R2 on test (2024). Optionally by-year on train."""
    test_featured = prep_model.transform(test_df)
    test_baseline = baseline_model.transform(test_featured)
    test_strong = strong_model.transform(test_featured)

    evaluator_rmse = RegressionEvaluator(labelCol=TARGET_COL, predictionCol="prediction", metricName="rmse")
    evaluator_mae = RegressionEvaluator(labelCol=TARGET_COL, predictionCol="prediction", metricName="mae")
    evaluator_r2 = RegressionEvaluator(labelCol=TARGET_COL, predictionCol="prediction", metricName="r2")

    def metrics(df, name):
        return {
            "rmse": evaluator_rmse.evaluate(df),
            "mae": evaluator_mae.evaluate(df),
            "r2": evaluator_r2.evaluate(df),
        }

    test_metrics_baseline = metrics(test_baseline, "baseline_test")
    test_metrics_strong = metrics(test_strong, "strong_test")

    # By-year on train (using strong model; same prep)
    train_featured = prep_model.transform(train_df)
    train_pred = strong_model.transform(train_featured)
    by_year = (
        train_pred.groupBy("ano")
        .agg(
            F.sqrt(F.mean(F.pow(F.col("prediction") - F.col(TARGET_COL), 2))).alias("rmse"),
            F.mean(F.abs(F.col("prediction") - F.col(TARGET_COL))).alias("mae"),
            F.count("*").alias("n"),
        )
        .orderBy("ano")
    )
    by_year_list = [row.asDict() for row in by_year.collect()]

    return {
        "test_2024": {
            "baseline": test_metrics_baseline,
            "strong": test_metrics_strong,
        },
        "train_by_year_strong": by_year_list,
    }, test_strong


# -----------------------------------------------------------------------------
# Save artifacts
# -----------------------------------------------------------------------------
def save_artifacts(prep_model, baseline_model, strong_model, metrics_dict, predictions_df):
    DIR_MODELS.mkdir(parents=True, exist_ok=True)
    DIR_REPORTS.mkdir(parents=True, exist_ok=True)

    baseline_path = DIR_MODELS / "baseline_model"
    strong_path = DIR_MODELS / "strong_model"
    # Save full pipelines (prep + model) so inference only needs to load one pipeline per model
    # Baseline: prep_model + baseline_model (we save baseline as a Pipeline that includes prep)
    # PipelineModel can only be built from a Pipeline. We have prep_model (PipelineModel) and
    # baseline_model (LinearRegressionModel). To save a single "baseline" artifact we could
    # either save prep_model and baseline_model separately and document loading both, or
    # build a Pipeline(stages=[...]) that includes the fitted stages. Spark does not allow
    # adding already-fit stages to a Pipeline easily. So save two pieces for baseline:
    # models/baseline_prep/ and models/baseline_lr/ (or we save prep once and baseline + strong
    # each with their own copy of prep). Simpler: save prep_model, baseline_model, strong_model
    # under models/prep_model/, models/baseline_model/, models/strong_model/. For inference,
    # load prep_model, transform data, then load baseline or strong and transform.
    prep_path = DIR_MODELS / "prep_model"
    prep_model.write().overwrite().save(str(prep_path))
    baseline_model.write().overwrite().save(str(baseline_path))
    strong_model.write().overwrite().save(str(strong_path))

    logger.info("Saved models: %s, %s, %s", prep_path, baseline_path, strong_path)

    metrics_path = DIR_REPORTS / "ml_metrics.json"
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics_dict, f, indent=2)
    logger.info("Saved metrics: %s", metrics_path)

    pred_path = DIR_REPORTS / "predictions_2024.parquet"
    predictions_df.write.mode("overwrite").parquet(str(pred_path))
    logger.info("Saved predictions: %s", pred_path)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    # 1) Load Gold
    df = load_data(spark)
    # 2) Features (no target-derived; no leakage)
    df = build_features(df)
    train_df, test_df = temporal_split(df)

    # 3) Preprocessing pipeline (fit on train only)
    prep_pipeline = _build_prep_pipeline()
    prep_model, baseline_model, strong_model = train_models(train_df, prep_pipeline)

    # 4) Feature list (for reproducibility and documentation)
    logger.info("Feature list (in order): %s", _feature_list())

    # 5) Evaluate
    metrics_dict, predictions_2024 = evaluate(prep_model, baseline_model, strong_model, train_df, test_df)
    logger.info("Test 2024 baseline: %s", metrics_dict["test_2024"]["baseline"])
    logger.info("Test 2024 strong:   %s", metrics_dict["test_2024"]["strong"])

    # 6) Save
    save_artifacts(prep_model, baseline_model, strong_model, metrics_dict, predictions_2024)
    logger.info("ML temporal pipeline finished.")


if __name__ == "__main__":
    main()
