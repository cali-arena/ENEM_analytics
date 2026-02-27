"""
Clusters (Cohorts & Clusters) usando DuckDB + sklearn — sem Spark/PyTorch.
Gera cluster_profiles.parquet e cluster_evolution_uf_ano.parquet em data/gold (prontos para cloud).

Uso: python scripts/run_clusters_duckdb.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DIR_GOLD = ROOT / "data" / "gold"
PATH_FATO = DIR_GOLD / "fato_desempenho"
PATH_DIM_GEO = DIR_GOLD / "dim_geografia"
PATH_DIM_PERFIL = DIR_GOLD / "dim_perfil"
PATH_CLUSTER_PROFILES = DIR_GOLD / "cluster_profiles.parquet"
PATH_CLUSTER_EVOLUTION = DIR_GOLD / "cluster_evolution_uf_ano.parquet"

K_CLUSTERS = 7
SEED = 42


def main():
    import duckdb
    import numpy as np
    from sklearn.cluster import MiniBatchKMeans
    from sklearn.preprocessing import StandardScaler

    if not PATH_FATO.exists():
        print("Execute primeiro: python scripts/run_lakehouse_duckdb.py")
        return

    def _parquet_path(p: Path) -> str:
        s = str(p.resolve()).replace("\\", "/")
        if p.is_dir():
            return s + "/**/*.parquet"
        return s

    con = duckdb.connect(":memory:")
    fato_str = _parquet_path(PATH_FATO)
    dim_geo_str = _parquet_path(PATH_DIM_GEO)
    dim_perfil_str = _parquet_path(PATH_DIM_PERFIL)

    con.execute(f"""
        CREATE OR REPLACE VIEW gold_joined AS
        SELECT
            f.nu_inscricao, f.ano, f.redacao, f.media_objetiva,
            f.presenca_cn, f.presenca_ch, f.presenca_lc, f.presenca_mt,
            COALESCE(TRIM(g.sg_uf_residencia), 'NA') AS sg_uf_residencia,
            COALESCE(TRIM(p.faixa_renda), 'NA') AS faixa_renda
        FROM read_parquet('{fato_str}') f
        LEFT JOIN read_parquet('{dim_geo_str}') g ON f.id_geo = g.id_geo
        LEFT JOIN read_parquet('{dim_perfil_str}') p ON f.id_perfil = p.id_perfil
    """)
    # Amostra para clustering (rápido; perfis/evolução depois em cima da amostra)
    SAMPLE_SIZE = 400_000
    n_total = con.execute("SELECT count(*) FROM gold_joined").fetchone()[0]
    if n_total < K_CLUSTERS:
        print("Poucos dados para clustering.")
        return
    use_sample = n_total > SAMPLE_SIZE
    if use_sample:
        df = con.execute(f"SELECT * FROM gold_joined ORDER BY random() LIMIT {SAMPLE_SIZE}").fetchdf()
        print(f"Clustering em amostra de {len(df):,} de {n_total:,} linhas.")
    else:
        df = con.execute("SELECT * FROM gold_joined").fetchdf()

    # Renda ordinal (A=1, B=2, ...)
    Q006_ORDER = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q"]
    ordem = {l: i for i, l in enumerate(Q006_ORDER, 1)}
    df["renda_ordinal"] = df["faixa_renda"].map(lambda x: ordem.get(str(x).strip().upper(), 0))
    df["full_presence"] = (
        (df["presenca_cn"] == 1) & (df["presenca_ch"] == 1) & (df["presenca_lc"] == 1) & (df["presenca_mt"] == 1)
    ).astype(int)

    # Features para KMeans (preencher nulos)
    feat_cols = ["redacao", "media_objetiva", "renda_ordinal", "full_presence"]
    X = df[feat_cols].fillna(0).astype(np.float64)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    km = MiniBatchKMeans(n_clusters=K_CLUSTERS, random_state=SEED, batch_size=10000, max_iter=50)
    df["cluster_id"] = km.fit_predict(X_scaled)

    # cluster_profiles: size, media_redacao, media_obj, presence_rate, top_3_ufs
    profiles = (
        df.groupby("cluster_id")
        .agg(
            size=("nu_inscricao", "count"),
            media_redacao=("redacao", "mean"),
            media_obj=("media_objetiva", "mean"),
            presence_rate=("full_presence", "mean"),
        )
        .reset_index()
    )
    # Top 3 UFs por cluster
    uf_cnt = df.groupby(["cluster_id", "sg_uf_residencia"]).size().reset_index(name="cnt")
    uf_cnt["rn"] = uf_cnt.groupby("cluster_id")["cnt"].rank(method="first", ascending=False)
    top3 = uf_cnt[uf_cnt["rn"] <= 3].groupby("cluster_id")["sg_uf_residencia"].apply(lambda x: [str(v) for v in x]).reset_index()
    top3.columns = ["cluster_id", "top_3_ufs"]
    profiles = profiles.merge(top3, on="cluster_id", how="left")
    profiles["cluster_id"] = profiles["cluster_id"].astype(int)

    # cluster_evolution_uf_ano: ano, uf, cluster_id, pct_participants
    tot = df.groupby(["ano", "sg_uf_residencia"]).size().reset_index(name="total")
    by_c = df.groupby(["ano", "sg_uf_residencia", "cluster_id"]).size().reset_index(name="cnt")
    ev = by_c.merge(tot, on=["ano", "sg_uf_residencia"], how="left")
    ev["pct_participants"] = ev["cnt"] / ev["total"]
    evolution = ev[["ano", "sg_uf_residencia", "cluster_id", "pct_participants"]].copy()
    evolution.columns = ["ano", "uf", "cluster_id", "pct_participants"]
    evolution["cluster_id"] = evolution["cluster_id"].astype(int)

    # Salvar Parquet (pronto para cloud)
    DIR_GOLD.mkdir(parents=True, exist_ok=True)
    profiles.to_parquet(PATH_CLUSTER_PROFILES, index=False)
    evolution.to_parquet(PATH_CLUSTER_EVOLUTION, index=False)
    print("Gold Parquet: data/gold/cluster_profiles.parquet")
    print("Gold Parquet: data/gold/cluster_evolution_uf_ano.parquet")
    print("Clusters prontos para Cohorts & Clusters e para cloud.")


if __name__ == "__main__":
    main()
