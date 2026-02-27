"""
Gera demo_gold.zip com dados Gold mínimos (sem precisar de CSVs em data/raw).

Uso (na raiz do repo):
  python scripts/build_demo_gold.py

Saída: demo_gold.zip na raiz (ou em dist/) pronto para subir em GitHub Release.
No Streamlit Cloud, configure DEMO_GOLD_URL apontando para o asset do release.

Estrutura do zip: conteúdo de data/gold (kpis_uf_ano/, dim_tempo/, etc.)
para que scripts/bootstrap_cloud.py extraia corretamente em GOLD_DIR.
"""
from __future__ import annotations

import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Saída: zip na raiz do repo (você pode mover para Releases depois)
OUT_ZIP = ROOT / "demo_gold.zip"
# Diretório temporário para montar o "gold" antes de zipar
GOLD_DEMO_DIR = ROOT / "tmp" / "demo_gold_build"


def _clean_dir(p: Path) -> None:
    if p.exists():
        for f in p.rglob("*"):
            if f.is_file():
                f.unlink()
        for d in sorted(p.rglob("*"), key=lambda x: -len(x.parts)):
            if d.is_dir():
                d.rmdir()
    p.mkdir(parents=True, exist_ok=True)


def main():
    import duckdb

    _clean_dir(GOLD_DEMO_DIR)

    con = duckdb.connect(":memory:")

    # Dados mínimos: 2 anos, 3 UFs, poucas linhas para cada tabela
    anos = [2022, 2023]
    ufs = ["SP", "RJ", "MG"]

    # dim_tempo
    con.execute("""
        CREATE TABLE dim_tempo AS
        SELECT * FROM (VALUES
            (2022, 2022, 'pos_pandemia'),
            (2023, 2023, 'pos_pandemia')
        ) AS t(id_tempo, ano, ciclo_pre_pos_pandemia)
    """)
    _write_parquet_dir(con, "dim_tempo", GOLD_DEMO_DIR / "dim_tempo")

    # dim_geografia
    con.execute("""
        CREATE TABLE dim_geografia AS
        SELECT * FROM (VALUES
            (1, 'SP'), (2, 'RJ'), (3, 'MG')
        ) AS t(id_geo, sg_uf_residencia)
    """)
    _write_parquet_dir(con, "dim_geografia", GOLD_DEMO_DIR / "dim_geografia")

    # dim_perfil
    con.execute("""
        CREATE TABLE dim_perfil AS
        SELECT * FROM (VALUES
            (1, 'M', 'A'), (2, 'F', 'B'), (3, 'M', 'C')
        ) AS t(id_perfil, tp_sexo, faixa_renda)
    """)
    _write_parquet_dir(con, "dim_perfil", GOLD_DEMO_DIR / "dim_perfil")

    # kpis_uf_ano (obrigatório para o dashboard)
    con.execute(f"""
        CREATE TABLE kpis_uf_ano AS
        SELECT
            ano,
            sg_uf_residencia,
            1000::BIGINT AS count_participantes,
            600.0::DOUBLE AS media_redacao,
            550.0::DOUBLE AS media_objetiva,
            500.0::DOUBLE AS p25_redacao,
            600.0::DOUBLE AS p50_redacao,
            700.0::DOUBLE AS p75_redacao,
            450.0::DOUBLE AS p25_objetiva,
            550.0::DOUBLE AS p50_objetiva,
            650.0::DOUBLE AS p75_objetiva,
            10.0::DOUBLE AS pct_top800_redacao,
            95.0::DOUBLE AS pct_presence_full
        FROM (VALUES {', '.join(f"({a}, '{u}')" for a in anos for u in ufs)}) AS t(ano, sg_uf_residencia)
    """)
    _write_parquet_dir(con, "kpis_uf_ano", GOLD_DEMO_DIR / "kpis_uf_ano", partition_by="ano")

    # fato_desempenho (amostra mínima)
    con.execute("""
        CREATE TABLE fato_desempenho AS
        SELECT
            1001::BIGINT AS nu_inscricao, 2022::INTEGER AS ano, 2022::INTEGER AS id_tempo,
            1::INTEGER AS id_geo, 1::INTEGER AS id_perfil,
            500.0::DOUBLE AS cn, 520.0::DOUBLE AS ch, 540.0::DOUBLE AS lc, 510.0::DOUBLE AS mt, 620.0::DOUBLE AS redacao,
            517.5::DOUBLE AS media_objetiva,
            1::INTEGER AS presenca_cn, 1::INTEGER AS presenca_ch, 1::INTEGER AS presenca_lc, 1::INTEGER AS presenca_mt
        UNION ALL SELECT 1002, 2023, 2023, 2, 2, 510, 530, 550, 520, 630, 527.5, 1, 1, 1, 1
        UNION ALL SELECT 1003, 2023, 2023, 3, 3, 480, 500, 520, 490, 600, 497.5, 1, 1, 1, 1
    """)
    _write_parquet_dir(con, "fato_desempenho", GOLD_DEMO_DIR / "fato_desempenho", partition_by="ano")

    # distribuicoes_notas
    con.execute(f"""
        CREATE TABLE distribuicoes_notas AS
        SELECT
            ano,
            sg_uf_residencia,
            400.0::DOUBLE AS redacao_p10, 500.0::DOUBLE AS redacao_p25, 600.0::DOUBLE AS redacao_p50,
            700.0::DOUBLE AS redacao_p75, 800.0::DOUBLE AS redacao_p90,
            350.0::DOUBLE AS objetiva_p10, 450.0::DOUBLE AS objetiva_p25, 550.0::DOUBLE AS objetiva_p50,
            650.0::DOUBLE AS objetiva_p75, 750.0::DOUBLE AS objetiva_p90,
            1000::BIGINT AS n
        FROM (VALUES {', '.join(f"({a}, '{u}')" for a in anos for u in ufs)}) AS t(ano, sg_uf_residencia)
    """)
    _write_parquet_dir(con, "distribuicoes_notas", GOLD_DEMO_DIR / "distribuicoes_notas", partition_by="ano")

    con.close()

    # cluster_profiles e cluster_evolution_uf_ano (arquivos únicos)
    import pandas as pd

    profiles = pd.DataFrame({
        "cluster_id": [0, 1, 2],
        "size": [500, 300, 200],
        "media_redacao": [580.0, 620.0, 540.0],
        "media_obj": [520.0, 560.0, 480.0],
        "presence_rate": [0.98, 0.99, 0.97],
        "top_3_ufs": [["SP", "RJ", "MG"], ["SP", "MG", "RJ"], ["RJ", "SP", "MG"]],
    })
    profiles.to_parquet(GOLD_DEMO_DIR / "cluster_profiles.parquet", index=False)

    evolution = pd.DataFrame({
        "ano": [2022, 2022, 2023, 2023],
        "uf": ["SP", "RJ", "SP", "RJ"],
        "cluster_id": [0, 0, 1, 1],
        "pct_participants": [0.4, 0.35, 0.45, 0.38],
    })
    evolution.to_parquet(GOLD_DEMO_DIR / "cluster_evolution_uf_ano.parquet", index=False)

    # Zipar: conteúdo de GOLD_DEMO_DIR na raiz do zip (para extractall(GOLD_DIR) dar certo)
    if OUT_ZIP.exists():
        OUT_ZIP.unlink()
    with zipfile.ZipFile(OUT_ZIP, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in GOLD_DEMO_DIR.rglob("*"):
            if f.is_file():
                arcname = f.relative_to(GOLD_DEMO_DIR)
                zf.write(f, arcname)

    # Limpar tmp
    for f in GOLD_DEMO_DIR.rglob("*"):
        if f.is_file():
            f.unlink()
    for d in sorted(GOLD_DEMO_DIR.rglob("*"), key=lambda x: -len(x.parts)):
        if d.is_dir():
            d.rmdir()
    GOLD_DEMO_DIR.rmdir()

    print(f"demo_gold.zip criado: {OUT_ZIP} ({OUT_ZIP.stat().st_size / 1024:.1f} KB)")
    print("Próximo passo: criar um Release no GitHub, anexar este arquivo e definir DEMO_GOLD_URL no Streamlit Cloud.")


def _write_parquet_dir(con, table: str, out_path: Path, partition_by: str | None = None) -> None:
    out_path.mkdir(parents=True, exist_ok=True)
    # No Windows, DuckDB aceita melhor caminho de arquivo; diretório com part.parquet
    path_str = str(out_path.resolve()).replace("\\", "/")
    if partition_by:
        con.execute(f"COPY {table} TO '{path_str}' (FORMAT PARQUET, PARTITION_BY ({partition_by}))")
    else:
        # Escrever arquivo explícito dentro do dir para evitar "Acesso negado" no Windows
        single = (out_path / "part-0.parquet").resolve()
        single.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY {table} TO '{str(single).replace(chr(92), '/')}' (FORMAT PARQUET)")


if __name__ == "__main__":
    main()
