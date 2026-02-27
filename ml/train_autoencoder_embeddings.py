"""
ENEM Opportunity & Equity Radar (2020–2024) — Tabular Autoencoder Embeddings (CRISP-DM: Modeling).

Creates participant embeddings from Gold data using a tabular autoencoder.
Train: 2020–2023. Validate: 2024. Saves model and embeddings to Gold.

Why embeddings come from Gold:
- Gold is the modeled, decision-ready layer: consistent semantics (faixa_renda, surrogate keys),
  valid-cohort rules, and alignment with other analytics (KPIs, ML models). Embeddings built from
  Gold are directly joinable to fact/dim tables and usable in the same pipelines without
  redefining "participant" or re-applying Silver cleaning. Silver is raw-cleaned but not
  decision-optimized; building embeddings from Gold keeps a single source of truth for
  downstream use (e.g. clustering, retrieval, dashboards).
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import numpy as np
import torch
import torch.nn as nn
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sklearn.preprocessing import StandardScaler

# -----------------------------------------------------------------------------
# Paths and config
# -----------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DIR_GOLD = ROOT / "data" / "gold"
DIR_MODELS = ROOT / "models"
PATH_FATO = DIR_GOLD / "fato_desempenho"
PATH_DIM_GEO = DIR_GOLD / "dim_geografia"
PATH_DIM_PERFIL = DIR_GOLD / "dim_perfil"
PATH_MODEL = DIR_MODELS / "autoencoder.pt"
PATH_EMBEDDINGS = DIR_GOLD / "participant_embeddings.parquet"

TRAIN_YEARS = [2020, 2021, 2022, 2023]
VAL_YEAR = 2024
EMBEDDING_DIM = 16
HIDDEN_1 = 64
HIDDEN_2 = 32
BATCH_SIZE = 1024
EPOCHS = 50
LR = 1e-3
SEED = 42

# Q006 ordinal (ENEM)
Q006_ORDER = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def set_seed(seed: int):
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


# -----------------------------------------------------------------------------
# 1) Load Gold and build features (from Gold only)
# -----------------------------------------------------------------------------
def load_data(spark: SparkSession):
    """Load Gold fact + dim_perfil + dim_geografia. All features from Gold (modeled, stable)."""
    logger.info("Loading Gold: fato_desempenho, dim_geografia, dim_perfil")
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
        fato["cn"],
        fato["ch"],
        fato["lc"],
        fato["mt"],
        fato["redacao"],
        fato["media_objetiva"],
        fato["presenca_cn"],
        fato["presenca_ch"],
        fato["presenca_lc"],
        fato["presenca_mt"],
        F.coalesce(F.trim(dim_geo["sg_uf_residencia"]), F.lit("NA")).alias("sg_uf_residencia"),
        F.coalesce(F.trim(dim_perfil["tp_sexo"]), F.lit("NA")).alias("tp_sexo"),
        F.coalesce(F.trim(dim_perfil["faixa_renda"]), F.lit("NA")).alias("faixa_renda"),
    )
    return df


def _renda_ordinal_expr():
    cases = F.lit(0)
    for i, letter in enumerate(Q006_ORDER, start=1):
        cases = F.when(F.col("faixa_renda") == letter, i).otherwise(cases)
    return cases


def build_features(df):
    """Add renda_ordinal. Keep: notas, media_objetiva, redacao, presence, UF, sexo for encoding later."""
    df = df.withColumn("renda_ordinal", _renda_ordinal_expr())
    return df


# -----------------------------------------------------------------------------
# Train/val split and standardization (no leakage: scaler fit on train only)
# -----------------------------------------------------------------------------
FEATURE_COLS = [
    "cn", "ch", "lc", "mt", "media_objetiva", "redacao",
    "renda_ordinal", "sexo_encoded",
    "presenca_cn", "presenca_ch", "presenca_lc", "presenca_mt",
    "uf_idx",
]


def prepare_train_val(train_pdf, val_pdf, uf_list, sexo_list):
    """
    Encode UF/sexo from pre-fitted lists (train only = no leakage), fill nulls, standardize.
    Returns (X_train, X_val, train_ids, val_ids, scaler, feature_names).
    """
    uf_map = {u: i for i, u in enumerate(uf_list)}
    sexo_map = {s: i for i, s in enumerate(sexo_list)}
    train_pdf = train_pdf.copy()
    val_pdf = val_pdf.copy()
    train_pdf["uf_idx"] = train_pdf["sg_uf_residencia"].map(lambda u: uf_map.get(u, 0))
    train_pdf["sexo_encoded"] = train_pdf["tp_sexo"].map(lambda s: sexo_map.get(s, 0))
    val_pdf["uf_idx"] = val_pdf["sg_uf_residencia"].map(lambda u: uf_map.get(u, 0))
    val_pdf["sexo_encoded"] = val_pdf["tp_sexo"].map(lambda s: sexo_map.get(s, 0))

    for c in FEATURE_COLS:
        if c in train_pdf.columns:
            train_pdf[c] = train_pdf[c].fillna(0)
        if c in val_pdf.columns:
            val_pdf[c] = val_pdf[c].fillna(0)

    X_train = train_pdf[FEATURE_COLS].astype(np.float32).values
    X_val = val_pdf[FEATURE_COLS].astype(np.float32).values
    train_ids = train_pdf[["nu_inscricao", "ano"]].copy()
    val_ids = val_pdf[["nu_inscricao", "ano"]].copy()

    scaler = StandardScaler()
    scaler.fit(X_train)
    scaler.scale_[scaler.scale_ == 0] = 1.0  # avoid div-by-zero for constant features
    X_train = scaler.transform(X_train)
    X_val = scaler.transform(val_pdf[FEATURE_COLS].astype(np.float32).values)
    return X_train, X_val, train_ids, val_ids, scaler, FEATURE_COLS


# -----------------------------------------------------------------------------
# 2) PyTorch Autoencoder
# -----------------------------------------------------------------------------
class Autoencoder(nn.Module):
    def __init__(self, input_dim: int, hidden1: int = 64, hidden2: int = 32, embedding_dim: int = 16):
        super().__init__()
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, hidden1),
            nn.ReLU(),
            nn.Linear(hidden1, hidden2),
            nn.ReLU(),
            nn.Linear(hidden2, embedding_dim),
        )
        self.decoder = nn.Sequential(
            nn.Linear(embedding_dim, hidden2),
            nn.ReLU(),
            nn.Linear(hidden2, hidden1),
            nn.ReLU(),
            nn.Linear(hidden1, input_dim),
        )
        self._embedding_dim = embedding_dim
        self._input_dim = input_dim

    def forward(self, x):
        z = self.encoder(x)
        return self.decoder(z), z

    @property
    def embedding_dim(self):
        return self._embedding_dim


def train_epoch(model, loader, criterion, optimizer, device):
    model.train()
    total_loss = 0.0
    for batch in loader:
        x = batch[0].to(device)
        optimizer.zero_grad()
        recon, _ = model(x)
        loss = criterion(recon, x)
        loss.backward()
        optimizer.step()
        total_loss += loss.item() * x.size(0)
    return total_loss / len(loader.dataset)


@torch.no_grad()
def evaluate_loss(model, X, device, batch_size=4096):
    model.eval()
    criterion = nn.MSELoss()
    total_loss = 0.0
    n = 0
    for i in range(0, len(X), batch_size):
        chunk = torch.from_numpy(X[i : i + batch_size].astype(np.float32)).to(device)
        recon, _ = model(chunk)
        total_loss += criterion(recon, chunk).item() * len(chunk)
        n += len(chunk)
    return total_loss / n if n else 0.0


@torch.no_grad()
def get_embeddings(model, X, device, batch_size=4096):
    model.eval()
    out = []
    for i in range(0, len(X), batch_size):
        chunk = torch.from_numpy(X[i : i + batch_size].astype(np.float32)).to(device)
        _, z = model(chunk)
        out.append(z.cpu().numpy())
    return np.vstack(out)


def save_model(model, path, feature_names, scaler_mean, scaler_scale):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    state = {
        "state_dict": model.state_dict(),
        "input_dim": model._input_dim,
        "embedding_dim": model._embedding_dim,
        "feature_names": feature_names,
        "scaler_mean": scaler_mean,
        "scaler_scale": scaler_scale,
    }
    torch.save(state, path)


def load_model(path, device):
    state = torch.load(path, map_location=device, weights_only=False)
    model = Autoencoder(
        input_dim=state["input_dim"],
        hidden1=HIDDEN_1,
        hidden2=HIDDEN_2,
        embedding_dim=state["embedding_dim"],
    )
    model.load_state_dict(state["state_dict"])
    return model, state.get("feature_names", []), state.get("scaler_mean"), state.get("scaler_scale")


# -----------------------------------------------------------------------------
# Write embeddings to Gold parquet (nu_inscricao, ano, embedding_vector)
# -----------------------------------------------------------------------------
def write_embeddings_parquet(spark, nu_inscricao, ano, embeddings, path):
    from pyspark.sql import types as T
    from pyspark.sql import Row
    rows = [
        Row(nu_inscricao=int(n), ano=int(a), embedding_vector=[float(x) for x in emb])
        for n, a, emb in zip(nu_inscricao, ano, embeddings)
    ]
    schema = T.StructType([
        T.StructField("nu_inscricao", T.LongType(), False),
        T.StructField("ano", T.IntegerType(), False),
        T.StructField("embedding_vector", T.ArrayType(T.FloatType(), False), False),
    ])
    spark.createDataFrame(rows, schema).write.mode("overwrite").parquet(str(path))


# -----------------------------------------------------------------------------
# 3) Evaluation: reconstruction loss + embedding distribution stability
# -----------------------------------------------------------------------------
def print_evaluation(model, X_train, X_val, device, embeddings_train, embeddings_val):
    train_loss = evaluate_loss(model, X_train, device)
    val_loss = evaluate_loss(model, X_val, device)
    logger.info("Reconstruction loss — train: %.6f  val: %.6f", train_loss, val_loss)

    for name, emb in [("train", embeddings_train), ("val", embeddings_val)]:
        mean = np.mean(emb, axis=0)
        std = np.std(emb, axis=0)
        logger.info("Embeddings %s — shape %s  mean(axis=0) range [%.4f, %.4f]  std(axis=0) range [%.4f, %.4f]",
                    name, emb.shape, mean.min(), mean.max(), std.min(), std.max())
    # Stability: compare mean embedding per dimension between train and val
    mean_train = np.mean(embeddings_train, axis=0)
    mean_val = np.mean(embeddings_val, axis=0)
    diff = np.abs(mean_train - mean_val)
    logger.info("Embedding distribution stability — max |mean_train - mean_val| per dim: %.4f", diff.max())


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    set_seed(SEED)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info("Device: %s", device)

    spark = (
        SparkSession.builder.appName("ENEM-Autoencoder-Embeddings")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = load_data(spark)
    df = build_features(df)

    train_df = df.filter(F.col("ano").isin(TRAIN_YEARS))
    val_df = df.filter(F.col("ano") == VAL_YEAR)
    train_pdf = train_df.toPandas()
    val_pdf = val_df.toPandas()
    uf_list = sorted(train_pdf["sg_uf_residencia"].dropna().unique().tolist()) or ["NA"]
    sexo_list = sorted(train_pdf["tp_sexo"].dropna().unique().tolist()) or ["NA"]

    X_train, X_val, train_ids, val_ids, scaler, feature_names = prepare_train_val(
        train_pdf, val_pdf, uf_list, sexo_list
    )
    input_dim = X_train.shape[1]
    logger.info("Feature names: %s", feature_names)
    logger.info("Train size: %d  Val size: %d  input_dim: %d", len(X_train), len(X_val), input_dim)

    model = Autoencoder(
        input_dim=input_dim,
        hidden1=HIDDEN_1,
        hidden2=HIDDEN_2,
        embedding_dim=EMBEDDING_DIM,
    ).to(device)
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=LR)
    train_loader = torch.utils.data.DataLoader(
        torch.utils.data.TensorDataset(torch.from_numpy(X_train)),
        batch_size=BATCH_SIZE,
        shuffle=True,
        generator=torch.Generator().manual_seed(SEED),
    )

    for epoch in range(EPOCHS):
        loss = train_epoch(model, train_loader, criterion, optimizer, device)
        if (epoch + 1) % 10 == 0 or epoch == 0:
            val_loss = evaluate_loss(model, X_val, device)
            logger.info("Epoch %d  train_loss: %.6f  val_loss: %.6f", epoch + 1, loss, val_loss)

    save_model(
        model, PATH_MODEL,
        feature_names=feature_names,
        scaler_mean=scaler.mean_.tolist(),
        scaler_scale=scaler.scale_.tolist(),
    )
    logger.info("Saved %s", PATH_MODEL)

    embeddings_train = get_embeddings(model, X_train, device)
    embeddings_val = get_embeddings(model, X_val, device)
    print_evaluation(model, X_train, X_val, device, embeddings_train, embeddings_val)

    # Full dataset embeddings for parquet (train + val)
    full_df = df.toPandas()
    full_df = full_df.copy()
    uf_map = {u: i for i, u in enumerate(uf_list)}
    sexo_map = {s: i for i, s in enumerate(sexo_list)}
    full_df["uf_idx"] = full_df["sg_uf_residencia"].map(lambda u: uf_map.get(u, 0))
    full_df["sexo_encoded"] = full_df["tp_sexo"].map(lambda s: sexo_map.get(s, 0))
    for c in FEATURE_COLS:
        full_df[c] = full_df[c].fillna(0)
    X_full = scaler.transform(full_df[FEATURE_COLS].astype(np.float32).values)
    embeddings_full = get_embeddings(model, X_full, device)
    write_embeddings_parquet(
        spark,
        full_df["nu_inscricao"].astype(int).tolist(),
        full_df["ano"].astype(int).tolist(),
        embeddings_full,
        PATH_EMBEDDINGS,
    )
    logger.info("Saved %s", PATH_EMBEDDINGS)
    logger.info("Autoencoder embeddings pipeline finished.")


if __name__ == "__main__":
    main()
