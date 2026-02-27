"""
Pipeline Spark – ENEM (últimos 5 anos).
Estágios: 1) Leitura  2) Limpeza  3) Modelagem  4) Escrita (Parquet particionado por ano).
Boas práticas: schema unificado, tratamento de nulos, tipos corretos, particionamento.
Uso: python scripts/02_spark_pipeline.py
"""
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import ANOS, DIR_RAW, DIR_PROCESSED


# Colunas de interesse unificadas (existem na maioria dos anos)
# Notas: NU_NOTA_CN, NU_NOTA_CH, NU_NOTA_LC, NU_NOTA_MT, NU_NOTA_REDACAO
# Socioeconômico: TP_FAIXA_ETARIA, TP_SEXO, TP_COR_RACA, Q006 (renda), Q001 (escolaridade pai), etc.
COLS_NOTAS = ["NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO"]
COLS_DEMO = ["TP_FAIXA_ETARIA", "TP_SEXO", "TP_COR_RACA", "TP_ESTADO_CIVIL", "TP_DEPENDENCIA_ADM_ESC"]
COLS_ESCOLA = ["TP_DEPENDENCIA_ADM_ESC", "TP_LOCALIZACAO_ESC"]
COLS_QUESTIONARIO = ["Q001", "Q002", "Q006"]  # Q001/Q002 escolaridade pais, Q006 renda
COLUNAS_UNIFICADAS = (
    ["NU_INSCRICAO", "NU_ANO"] + COLS_NOTAS + COLS_DEMO + COLS_ESCOLA + COLS_QUESTIONARIO
)


def get_spark():
    return (
        SparkSession.builder.appName("ENEM-Pipeline")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


def ler_csv_ano(spark, ano: int) -> "pyspark.sql.DataFrame":
    """Estágio 1: Leitura do CSV de um ano (delimitador ;, header)."""
    path = ROOT / DIR_RAW / f"ENEM_{ano}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Execute 01_coleta_enem.py primeiro. Não encontrado: {path}")
    df = (
        spark.read.option("header", "true")
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .option("nullValue", "")
        .csv(str(path))
    )
    return df.withColumn("NU_ANO", F.lit(ano))


def limpar_e_tipar(df):
    """
    Estágio 2: Limpeza e tipagem.
    - Substituir string vazia e '.' por null em colunas numéricas.
    - Converter notas para double, códigos para int onde fizer sentido.
    """
    for col_name in df.columns:
        if "NOTA" in col_name.upper():
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name).isin("", ".", " "), None)
                .otherwise(F.col(col_name).cast(DoubleType()))
            )
        elif col_name.upper() in ("TP_FAIXA_ETARIA", "TP_SEXO", "TP_COR_RACA", "TP_ESTADO_CIVIL",
                                  "TP_DEPENDENCIA_ADM_ESC", "TP_LOCALIZACAO_ESC") or col_name.upper().startswith("Q0"):
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name).isin("", ".", " "), None)
                .otherwise(F.col(col_name).cast(StringType()))
            )
    return df


def modelar(df, ano: int):
    """
    Estágio 3: Modelagem.
    - Garantir coluna NU_ANO; remover duplicatas por inscrição.
    - Manter apenas dados “reais” do ENEM: quem tem pelo menos uma nota (fez alguma prova).
    """
    if "NU_ANO" not in df.columns:
        df = df.withColumn("NU_ANO", F.lit(ano))
    # Apenas participantes com pelo menos uma nota (dados reais do exame)
    nota_cols = [c for c in COLS_NOTAS if c in df.columns]
    if nota_cols:
        cond = F.col(nota_cols[0]).isNotNull()
        for c in nota_cols[1:]:
            cond = cond | F.col(c).isNotNull()
        df = df.filter(cond)
    df = df.dropDuplicates(["NU_INSCRICAO"]) if "NU_INSCRICAO" in df.columns else df
    return df


def main():
    spark = get_spark()
    (ROOT / DIR_PROCESSED).mkdir(parents=True, exist_ok=True)
    path_out = ROOT / DIR_PROCESSED / "enem_unificado"

    dfs = []
    all_cols = set()
    for ano in ANOS:
        print(f"Processando ano {ano} ...")
        df = ler_csv_ano(spark, ano)
        cols_desejadas = ["NU_INSCRICAO", "NU_ANO"] + [c for c in (COLS_NOTAS + COLS_DEMO + COLS_ESCOLA + COLS_QUESTIONARIO) if c in df.columns]
        df = df.select([c for c in cols_desejadas if c in df.columns])
        all_cols.update(df.columns)
        df = limpar_e_tipar(df)
        df = modelar(df, ano)
        dfs.append(df)

    # Garantir mesmo conjunto de colunas em todos os DataFrames (preencher faltantes com null)
    col_list = sorted(all_cols)
    for i, df in enumerate(dfs):
        for c in col_list:
            if c not in df.columns:
                df = df.withColumn(c, F.lit(None).cast(StringType()))
        dfs[i] = df.select(col_list)

    unificado = dfs[0]
    for df in dfs[1:]:
        unificado = unificado.unionByName(df, allowMissingColumns=True)

    print("Escrevendo Parquet particionado por NU_ANO ...")
    unificado.write.mode("overwrite").partitionBy("NU_ANO").parquet(str(path_out))
    print("Pipeline concluído:", path_out)
    spark.stop()


if __name__ == "__main__":
    main()
