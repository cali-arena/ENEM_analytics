"""
Pipeline Bronze → Silver → Gold usando DuckDB (sem Spark).
Gera Parquet em data/bronze, data/silver e data/gold — pronto para enviar à nuvem (ex.: Semilit/cloud) depois.

Uso: python scripts/run_lakehouse_duckdb.py

- Funciona no Windows sem HADOOP_HOME/winutils.
- Saída 100% Parquet (particionado por ano onde aplicável), ideal para upload para cloud/Data Lake.
"""
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

ANOS = [2020, 2021, 2022, 2023, 2024]
DIR_RAW = ROOT / "data" / "raw"
DIR_BRONZE = ROOT / "data" / "bronze"
DIR_SILVER = ROOT / "data" / "silver"
DIR_GOLD = ROOT / "data" / "gold"


def _raw_csv(ano: int) -> Path:
    for name in (f"ENEM_{ano}.csv", f"enem_{ano}.csv"):
        p = DIR_RAW / name
        if p.exists():
            return p
    return DIR_RAW / f"ENEM_{ano}.csv"


def main():
    import duckdb

    con = duckdb.connect(":memory:")

    # Criar diretórios
    for d in (DIR_BRONZE, DIR_SILVER, DIR_GOLD):
        d.mkdir(parents=True, exist_ok=True)

    # 1) Bronze: ler CSVs (delim ;, header). Cada raw_ano = todos os anos com coluna ano.
    all_parts = []
    for ano in ANOS:
        path = _raw_csv(ano)
        if not path.exists():
            print(f"[AVISO] Raw não encontrado: {path}")
            continue
        path_str = str(path).replace("\\", "/")
        con.execute(f"""
            CREATE OR REPLACE VIEW raw_{ano} AS
            SELECT *, {ano}::INTEGER AS ano
            FROM read_csv('{path_str}', delim=';', header=true, auto_detect=true)
        """)
        all_parts.append(f"raw_{ano}")
        print(f"Bronze: carregado {path.name}")

    if not all_parts:
        print("Nenhum CSV em data/raw. Abortando.")
        return

    # Silver: uma view por ano (evita conflito de nome SG_UF_* na mesma SELECT), depois união + filtro presença
    for v in all_parts:
        ano = int(v.split("_")[1])
        cols = [r[0] for r in con.execute(f"DESCRIBE {v}").fetchall()]
        has_prova = "SG_UF_PROVA" in cols
        has_res = "SG_UF_RESIDENCIA" in cols
        if has_prova and has_res:
            uf_expr = "COALESCE(TRIM(SG_UF_PROVA), TRIM(SG_UF_RESIDENCIA), '') AS SG_UF_RESIDENCIA"
        elif has_prova:
            uf_expr = "COALESCE(TRIM(SG_UF_PROVA), '') AS SG_UF_RESIDENCIA"
        else:
            uf_expr = "COALESCE(TRIM(SG_UF_RESIDENCIA), '') AS SG_UF_RESIDENCIA"
        con.execute(f"""
            CREATE OR REPLACE VIEW silver_{ano} AS
            SELECT
                NU_INSCRICAO, {uf_expr},
                TP_SEXO, Q006,
                COALESCE(TP_PRESENCA_CN, 0)::INTEGER AS TP_PRESENCA_CN,
                COALESCE(TP_PRESENCA_CH, 0)::INTEGER AS TP_PRESENCA_CH,
                COALESCE(TP_PRESENCA_LC, 0)::INTEGER AS TP_PRESENCA_LC,
                COALESCE(TP_PRESENCA_MT, 0)::INTEGER AS TP_PRESENCA_MT,
                NU_NOTA_CN, NU_NOTA_CH, NU_NOTA_LC, NU_NOTA_MT, NU_NOTA_REDACAO,
                ano
            FROM {v}
        """)
    union_sql = " UNION ALL ".join(f"SELECT * FROM silver_{int(p.split('_')[1])}" for p in all_parts)
    con.execute(f"""
        CREATE OR REPLACE VIEW silver AS
        SELECT * FROM ({union_sql}) sub
        WHERE TP_PRESENCA_CN = 1 AND TP_PRESENCA_CH = 1 AND TP_PRESENCA_LC = 1 AND TP_PRESENCA_MT = 1
    """)
    n_silver = con.execute("SELECT count(*) FROM silver").fetchone()[0]
    print(f"Silver: {n_silver} linhas (presença plena)")

    # Escrever Silver Parquet (enem_participante, particionado por ano para cloud)
    silver_path = DIR_SILVER / "enem_participante"
    if silver_path.exists():
        shutil.rmtree(silver_path)
    silver_path.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY (SELECT * FROM silver)
        TO '{silver_path}'
        (FORMAT PARQUET, PARTITION_BY (ano))
    """)
    print("Silver Parquet: data/silver/enem_participante/")

    # 2) Gold: dims + fato + kpis + distribuições
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")

    def _clean_path(p: Path):
        if p.exists():
            if p.is_file():
                p.unlink()
            else:
                shutil.rmtree(p)

    # dim_tempo
    con.execute("""
        CREATE OR REPLACE VIEW gold.dim_tempo AS
        SELECT DISTINCT ano::INTEGER AS id_tempo, ano,
               CASE WHEN ano IN (2020, 2021) THEN 'pandemia'
                    WHEN ano >= 2022 THEN 'pos_pandemia'
                    ELSE 'pre_pandemia' END AS ciclo_pre_pos_pandemia
        FROM silver
    """)
    _clean_path(DIR_GOLD / "dim_tempo")
    con.execute(f"""
        COPY gold.dim_tempo TO '{DIR_GOLD / "dim_tempo"}' (FORMAT PARQUET)
    """)
    print("Gold Parquet: data/gold/dim_tempo/")

    # dim_geografia (UF)
    con.execute("""
        CREATE OR REPLACE VIEW gold.dim_geografia AS
        WITH ufs AS (
            SELECT DISTINCT COALESCE(TRIM(SG_UF_RESIDENCIA), 'NA') AS sg_uf_residencia
            FROM silver WHERE TRIM(COALESCE(SG_UF_RESIDENCIA,'')) != ''
        )
        SELECT ROW_NUMBER() OVER (ORDER BY sg_uf_residencia)::INTEGER AS id_geo, sg_uf_residencia FROM ufs
    """)
    _clean_path(DIR_GOLD / "dim_geografia")
    con.execute(f"""
        COPY gold.dim_geografia TO '{DIR_GOLD / "dim_geografia"}' (FORMAT PARQUET)
    """)
    print("Gold Parquet: data/gold/dim_geografia/")

    # dim_perfil
    con.execute("""
        CREATE OR REPLACE VIEW gold.dim_perfil AS
        WITH prof AS (
            SELECT DISTINCT COALESCE(TP_SEXO,'') AS tp_sexo, COALESCE(Q006,'') AS faixa_renda
            FROM silver
        )
        SELECT ROW_NUMBER() OVER (ORDER BY tp_sexo, faixa_renda)::INTEGER AS id_perfil, tp_sexo, faixa_renda FROM prof
    """)
    _clean_path(DIR_GOLD / "dim_perfil")
    con.execute(f"""
        COPY gold.dim_perfil TO '{DIR_GOLD / "dim_perfil"}' (FORMAT PARQUET)
    """)
    print("Gold Parquet: data/gold/dim_perfil/")

    # kpis_uf_ano (o que o dashboard precisa)
    con.execute("""
        CREATE OR REPLACE VIEW gold.kpis_uf_ano AS
        SELECT
            ano,
            COALESCE(TRIM(SG_UF_RESIDENCIA), 'NA') AS sg_uf_residencia,
            COUNT(*) AS count_participantes,
            AVG(NU_NOTA_REDACAO) AS media_redacao,
            AVG((NU_NOTA_CN + NU_NOTA_CH + NU_NOTA_LC + NU_NOTA_MT) / 4.0) AS media_objetiva,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY NU_NOTA_REDACAO) AS p25_redacao,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY NU_NOTA_REDACAO) AS p50_redacao,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY NU_NOTA_REDACAO) AS p75_redacao,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY (NU_NOTA_CN+NU_NOTA_CH+NU_NOTA_LC+NU_NOTA_MT)/4.0) AS p25_objetiva,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (NU_NOTA_CN+NU_NOTA_CH+NU_NOTA_LC+NU_NOTA_MT)/4.0) AS p50_objetiva,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY (NU_NOTA_CN+NU_NOTA_CH+NU_NOTA_LC+NU_NOTA_MT)/4.0) AS p75_objetiva,
            100.0 * SUM(CASE WHEN NU_NOTA_REDACAO >= 800 THEN 1 ELSE 0 END) / COUNT(*) AS pct_top800_redacao,
            100.0 * SUM(CASE WHEN COALESCE(TP_PRESENCA_CN,0)=1 AND COALESCE(TP_PRESENCA_CH,0)=1 AND COALESCE(TP_PRESENCA_LC,0)=1 AND COALESCE(TP_PRESENCA_MT,0)=1 THEN 1 ELSE 0 END) / COUNT(*) AS pct_presence_full
        FROM silver
        GROUP BY ano, COALESCE(TRIM(SG_UF_RESIDENCIA), 'NA')
    """)
    kpis_path = DIR_GOLD / "kpis_uf_ano"
    _clean_path(kpis_path)
    kpis_path.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY gold.kpis_uf_ano TO '{kpis_path}' (FORMAT PARQUET, PARTITION_BY (ano))
    """)
    print("Gold Parquet: data/gold/kpis_uf_ano/")

    # fato_desempenho (resumido: chave + notas + ids dims)
    con.execute("""
        CREATE OR REPLACE VIEW gold.fato_desempenho AS
        SELECT
            NU_INSCRICAO AS nu_inscricao,
            ano,
            ano AS id_tempo,
            (SELECT id_geo FROM gold.dim_geografia g WHERE g.sg_uf_residencia = COALESCE(TRIM(s.SG_UF_RESIDENCIA), 'NA') LIMIT 1) AS id_geo,
            (SELECT id_perfil FROM gold.dim_perfil p WHERE p.tp_sexo = COALESCE(s.TP_SEXO,'') AND p.faixa_renda = COALESCE(s.Q006,'') LIMIT 1) AS id_perfil,
            NU_NOTA_CN AS cn, NU_NOTA_CH AS ch, NU_NOTA_LC AS lc, NU_NOTA_MT AS mt, NU_NOTA_REDACAO AS redacao,
            (NU_NOTA_CN + NU_NOTA_CH + NU_NOTA_LC + NU_NOTA_MT) / 4.0 AS media_objetiva,
            COALESCE(TP_PRESENCA_CN,0) AS presenca_cn, COALESCE(TP_PRESENCA_CH,0) AS presenca_ch,
            COALESCE(TP_PRESENCA_LC,0) AS presenca_lc, COALESCE(TP_PRESENCA_MT,0) AS presenca_mt
        FROM silver s
    """)
    fato_path = DIR_GOLD / "fato_desempenho"
    _clean_path(fato_path)
    fato_path.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY gold.fato_desempenho TO '{fato_path}' (FORMAT PARQUET, PARTITION_BY (ano))
    """)
    print("Gold Parquet: data/gold/fato_desempenho/")

    # distribuicoes_notas (percentis por ano+UF)
    con.execute("""
        CREATE OR REPLACE VIEW gold.distribuicoes_notas AS
        SELECT
            ano,
            COALESCE(TRIM(SG_UF_RESIDENCIA), 'NA') AS sg_uf_residencia,
            PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY NU_NOTA_REDACAO) AS redacao_p10,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY NU_NOTA_REDACAO) AS redacao_p25,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY NU_NOTA_REDACAO) AS redacao_p50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY NU_NOTA_REDACAO) AS redacao_p75,
            PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY NU_NOTA_REDACAO) AS redacao_p90,
            PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY (NU_NOTA_CN+NU_NOTA_CH+NU_NOTA_LC+NU_NOTA_MT)/4.0) AS objetiva_p10,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY (NU_NOTA_CN+NU_NOTA_CH+NU_NOTA_LC+NU_NOTA_MT)/4.0) AS objetiva_p25,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (NU_NOTA_CN+NU_NOTA_CH+NU_NOTA_LC+NU_NOTA_MT)/4.0) AS objetiva_p50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY (NU_NOTA_CN+NU_NOTA_CH+NU_NOTA_LC+NU_NOTA_MT)/4.0) AS objetiva_p75,
            PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY (NU_NOTA_CN+NU_NOTA_CH+NU_NOTA_LC+NU_NOTA_MT)/4.0) AS objetiva_p90,
            COUNT(*) AS n
        FROM silver
        GROUP BY ano, COALESCE(TRIM(SG_UF_RESIDENCIA), 'NA')
    """)
    dist_path = DIR_GOLD / "distribuicoes_notas"
    _clean_path(dist_path)
    dist_path.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY gold.distribuicoes_notas TO '{dist_path}' (FORMAT PARQUET, PARTITION_BY (ano))
    """)
    print("Gold Parquet: data/gold/distribuicoes_notas/")

    # Bronze: opcional — escrever Parquet por ano para cloud
    for ano in ANOS:
        if f"raw_{ano}" not in all_parts:
            continue
        out_bronze = DIR_BRONZE / f"enem_{ano}.parquet"
        try:
            con.execute(f"COPY raw_{ano} TO '{out_bronze}' (FORMAT PARQUET)")
        except Exception as e:
            print(f"[AVISO] Bronze {ano} não escrito: {e}")
    print("Bronze Parquet: data/bronze/enem_<ano>.parquet (se disponível)")

    n_kpis = con.execute("SELECT count(*) FROM gold.kpis_uf_ano").fetchone()[0]
    print(f"\nConcluído. gold.kpis_uf_ano: {n_kpis} linhas. Reinicie o dashboard para carregar.")
    print("Todos os artefatos estão em Parquet — prontos para enviar à nuvem (Semilit/cloud) depois.")


if __name__ == "__main__":
    main()
