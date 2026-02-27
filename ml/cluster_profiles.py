"""
ENEM Opportunity & Equity Radar — Clustering on participant embeddings (CRISP-DM: Modeling).

Consumes Gold participant embeddings; fits clustering on 2020–2023, applies to 2024.
Outputs: cluster_profiles.parquet, cluster_evolution_uf_ano.parquet.
Clear separation: embedding (input) vs clustering (this script).
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

# -----------------------------------------------------------------------------
# Paths and config
# -----------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DIR_GOLD = ROOT / "data" / "gold"
PATH_EMBEDDINGS = DIR_GOLD / "participant_embeddings.parquet"
PATH_FATO = DIR_GOLD / "fato_desempenho"
PATH_DIM_GEO = DIR_GOLD / "dim_geografia"
PATH_DIM_PERFIL = DIR_GOLD / "dim_perfil"
PATH_CLUSTER_PROFILES = DIR_GOLD / "cluster_profiles.parquet"
PATH_CLUSTER_EVOLUTION = DIR_GOLD / "cluster_evolution_uf_ano.parquet"
PATH_PARTICIPANT_CLUSTERS = DIR_GOLD / "participant_clusters.parquet"

TRAIN_YEARS = [2020, 2021, 2022, 2023]
APPLY_YEAR = 2024
K_RANGE = [6, 7, 8]
SEED = 42

Q006_ORDER = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q"]
IMBALANCE_MIN_PCT = 1.0
IMBALANCE_MAX_PCT = 40.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# 1) Load embeddings — split train (2020–2023) vs apply (2024)
# -----------------------------------------------------------------------------
def load_embeddings(spark):
    """Load embedding parquet; return full DF and train/apply splits by ano."""
    df = spark.read.parquet(str(PATH_EMBEDDINGS))
    train = df.filter(F.col("ano").isin(TRAIN_YEARS))
    apply = df.filter(F.col("ano") == APPLY_YEAR)
    logger.info("Embeddings loaded: train years %s, apply year %s", TRAIN_YEARS, APPLY_YEAR)
    return df, train, apply


def embeddings_to_matrix(spark_df):
    """Collect (nu_inscricao, ano, embedding_vector) and return ids + numpy matrix."""
    rows = spark_df.collect()
    if not rows:
        return [], np.zeros((0, 1))
    ids = [(r.nu_inscricao, r.ano) for r in rows]
    vecs = np.array([r.embedding_vector for r in rows], dtype=np.float64)
    return ids, vecs


# -----------------------------------------------------------------------------
# 2) Clustering — KMeans, k chosen by silhouette
# -----------------------------------------------------------------------------
def fit_best_kmeans(X, k_range, seed):
    """Fit KMeans for each k in k_range; return best k and fitted model by silhouette."""
    best_k, best_score, best_model = None, -1.0, None
    for k in k_range:
        if k >= X.shape[0]:
            continue
        km = KMeans(n_clusters=k, random_state=seed, n_init=10)
        labels = km.fit_predict(X)
        score = silhouette_score(X, labels)
        logger.info("k=%d  silhouette=%.4f", k, score)
        if score > best_score:
            best_score = score
            best_k = k
            best_model = km
    return best_k, best_model, best_score


def assign_clusters(model, X):
    """Assign cluster_id from fitted KMeans (nearest center)."""
    return model.predict(X)


# -----------------------------------------------------------------------------
# Build cluster assignments DF (nu_inscricao, ano, cluster_id) for all data
# -----------------------------------------------------------------------------
def build_full_assignments(spark, train_ids, train_labels, apply_ids, apply_labels):
    """Combine train and apply assignments into one Spark DataFrame."""
    from pyspark.sql import Row
    from pyspark.sql import types as T
    rows = []
    for (n, a), c in zip(train_ids, train_labels):
        rows.append(Row(nu_inscricao=int(n), ano=int(a), cluster_id=int(c)))
    for (n, a), c in zip(apply_ids, apply_labels):
        rows.append(Row(nu_inscricao=int(n), ano=int(a), cluster_id=int(c)))
    schema = T.StructType([
        T.StructField("nu_inscricao", T.LongType(), False),
        T.StructField("ano", T.IntegerType(), False),
        T.StructField("cluster_id", T.IntegerType(), False),
    ])
    return spark.createDataFrame(rows, schema)


# -----------------------------------------------------------------------------
# 3) Cluster profiles — join Gold fact + dims, aggregate by cluster
# -----------------------------------------------------------------------------
def load_gold_for_profiles(spark):
    """Gold fact + dim_geo + dim_perfil for redacao, media_objetiva, presence, UF, renda."""
    fato = spark.read.parquet(str(PATH_FATO))
    dim_geo = spark.read.parquet(str(PATH_DIM_GEO))
    dim_perfil = spark.read.parquet(str(PATH_DIM_PERFIL))
    df = (
        fato
        .join(dim_geo, fato["id_geo"] == dim_geo["id_geo"], "left")
        .join(dim_perfil, fato["id_perfil"] == dim_perfil["id_perfil"], "left")
    )
    df = df.select(
        fato["nu_inscricao"],
        fato["ano"],
        fato["redacao"],
        fato["media_objetiva"],
        fato["presenca_cn"],
        fato["presenca_ch"],
        fato["presenca_lc"],
        fato["presenca_mt"],
        F.coalesce(F.trim(dim_geo["sg_uf_residencia"]), F.lit("NA")).alias("sg_uf_residencia"),
        F.coalesce(F.trim(dim_perfil["faixa_renda"]), F.lit("NA")).alias("faixa_renda"),
    )
    return df


def renda_ordinal_expr():
    cases = F.lit(0)
    for i, letter in enumerate(Q006_ORDER, start=1):
        cases = F.when(F.col("faixa_renda") == letter, i).otherwise(cases)
    return cases


def build_cluster_profiles(spark, assignments, gold_df):
    """Per-cluster: size, media_redacao, media_obj, renda_media, presence_rate, top_3_ufs."""
    gold = gold_df.withColumn("renda_ordinal", renda_ordinal_expr())
    gold = gold.withColumn(
        "full_presence",
        F.when(
            (F.col("presenca_cn") == 1) & (F.col("presenca_ch") == 1)
            & (F.col("presenca_lc") == 1) & (F.col("presenca_mt") == 1),
            1,
        ).otherwise(0),
    )
    joined = assignments.join(
        gold,
        (assignments["nu_inscricao"] == gold["nu_inscricao"]) & (assignments["ano"] == gold["ano"]),
        "inner",
    )
    agg_df = joined.groupBy("cluster_id").agg(
        F.count("*").alias("size"),
        F.mean("redacao").alias("media_redacao"),
        F.mean("media_objetiva").alias("media_obj"),
        F.mean("renda_ordinal").alias("renda_media"),
        F.mean("full_presence").alias("presence_rate"),
    )
    uf_counts = joined.groupBy("cluster_id", "sg_uf_residencia").agg(F.count("*").alias("cnt"))
    w = Window.partitionBy("cluster_id").orderBy(F.desc("cnt"))
    uf_ranked = uf_counts.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") <= 3)
    top3_df = uf_ranked.groupBy("cluster_id").agg(
        F.collect_list(F.struct("rn", "sg_uf_residencia")).alias("_s"),
    )
    top3_df = top3_df.withColumn(
        "top_3_ufs",
        F.expr("transform(array_sort(_s), x -> x.sg_uf_residencia)"),
    ).select("cluster_id", "top_3_ufs")
    profiles = agg_df.join(top3_df, "cluster_id", "left")
    return profiles


# -----------------------------------------------------------------------------
# 4) Cluster evolution — % participants per cluster by ano and UF
# -----------------------------------------------------------------------------
def build_cluster_evolution(spark, assignments, gold_df):
    """For each (ano, UF): % of participants in each cluster_id."""
    gold_uf = gold_df.select("nu_inscricao", "ano", F.col("sg_uf_residencia").alias("uf"))
    j = assignments.join(
        gold_uf,
        (assignments["nu_inscricao"] == gold_uf["nu_inscricao"]) & (assignments["ano"] == gold_uf["ano"]),
        "inner",
    ).select(assignments["ano"], gold_uf["uf"], assignments["cluster_id"])
    total = j.groupBy("ano", "uf").agg(F.count("*").alias("total"))
    by_cluster = j.groupBy("ano", "uf", "cluster_id").agg(F.count("*").alias("cnt"))
    joined = by_cluster.join(total, ["ano", "uf"], "left")
    evolution = joined.withColumn("pct_participants", F.col("cnt") / F.col("total"))
    return evolution.select("ano", "uf", "cluster_id", "pct_participants")


# -----------------------------------------------------------------------------
# 5) Validation — silhouette, size distribution, imbalance
# -----------------------------------------------------------------------------
def print_validation(silhouette, labels, n_total):
    logger.info("Silhouette score (train): %.4f", silhouette)
    unique, counts = np.unique(labels, return_counts=True)
    logger.info("Cluster size distribution:")
    for c, cnt in zip(unique, counts):
        pct = 100.0 * cnt / n_total
        logger.info("  cluster_id=%d  count=%d  (%.2f%%)", c, int(cnt), pct)
        if pct < IMBALANCE_MIN_PCT:
            logger.warning("Imbalance: cluster %d has %.2f%% < %.1f%%", c, pct, IMBALANCE_MIN_PCT)
        if pct > IMBALANCE_MAX_PCT:
            logger.warning("Imbalance: cluster %d has %.2f%% > %.1f%%", c, pct, IMBALANCE_MAX_PCT)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    spark = (
        SparkSession.builder.appName("ENEM-Cluster-Profiles")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 1) Load embeddings, split
    full_emb, train_emb, apply_emb = load_embeddings(spark)
    train_ids, X_train = embeddings_to_matrix(train_emb)
    apply_ids, X_apply = embeddings_to_matrix(apply_emb)
    if not train_ids or X_train.size == 0:
        logger.error("No train embeddings; abort.")
        return
    # 2) KMeans k=6..8 by silhouette; apply to 2024
    best_k, model, sil = fit_best_kmeans(X_train, K_RANGE, SEED)
    logger.info("Best k=%d  silhouette=%.4f", best_k, sil)
    train_labels = assign_clusters(model, X_train)
    apply_labels = assign_clusters(model, X_apply) if apply_ids and X_apply.size > 0 else np.array([], dtype=np.int32)
    assignments = build_full_assignments(spark, train_ids, train_labels, apply_ids or [], apply_labels)
    assignments.write.mode("overwrite").parquet(str(PATH_PARTICIPANT_CLUSTERS))

    # 5) Validation
    n_total = len(train_labels) + len(apply_labels)
    all_labels = np.concatenate([train_labels, apply_labels]) if len(apply_labels) > 0 else train_labels
    print_validation(sil, all_labels, n_total)

    # 3) Cluster profiles (join Gold)
    gold = load_gold_for_profiles(spark)
    profiles = build_cluster_profiles(spark, assignments, gold)
    profiles.write.mode("overwrite").parquet(str(PATH_CLUSTER_PROFILES))
    logger.info("Saved %s", PATH_CLUSTER_PROFILES)

    # 4) Cluster evolution
    gold_uf = gold.select("nu_inscricao", "ano", F.col("sg_uf_residencia").alias("uf"))
    evolution = build_cluster_evolution(spark, assignments, gold_uf)
    evolution.write.mode("overwrite").parquet(str(PATH_CLUSTER_EVOLUTION))
    logger.info("Saved %s", PATH_CLUSTER_EVOLUTION)

    logger.info("Clustering pipeline finished.")


if __name__ == "__main__":
    main()
