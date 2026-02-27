"""
Análises unificadas e visualizações – ENEM 2020–2024.
Lê o Parquet processado, gera métricas por ano e gráficos em output/.
Uso: python scripts/03_analise_visualizacao.py
"""
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import ANOS, DIR_PROCESSED, DIR_OUTPUT


def get_spark():
    return SparkSession.builder.appName("ENEM-Analise").getOrCreate()


def carregar_unificado(spark):
    path = ROOT / DIR_PROCESSED / "enem_unificado"
    if not path.exists():
        raise FileNotFoundError(f"Execute 02_spark_pipeline.py primeiro. Não encontrado: {path}")
    return spark.read.parquet(str(path))


def analise_por_ano(df):
    """Médias das notas por ano e por área."""
    notas = ["NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO"]
    cols = [c for c in notas if c in df.columns]
    if not cols:
        return None
    return (
        df.filter(F.col("NU_ANO").isin(ANOS))
        .groupBy("NU_ANO")
        .agg(*[F.mean(F.col(c)).alias(c) for c in cols], F.count("*").alias("total"))
        .orderBy("NU_ANO")
    )


def analise_media_geral_por_ano(df):
    """Média das 5 áreas (onde disponível) por ano."""
    notas = ["NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO"]
    cols = [c for c in notas if c in df.columns]
    if not cols:
        return None
    return (
        df.filter(F.col("NU_ANO").isin(ANOS))
        .withColumn("media_geral", sum(F.col(c) for c in cols) / len(cols))
        .groupBy("NU_ANO")
        .agg(F.mean("media_geral").alias("media_geral"), F.count("*").alias("participantes"))
        .orderBy("NU_ANO")
    )


def analise_sexo_por_ano(df):
    if "TP_SEXO" not in df.columns:
        return None
    return (
        df.filter(F.col("NU_ANO").isin(ANOS))
        .groupBy("NU_ANO", "TP_SEXO")
        .agg(F.count("*").alias("total"))
        .orderBy("NU_ANO", "TP_SEXO")
    )


def salvar_grafico_medias_por_ano(pandas_df, path_out):
    """Gráfico de linhas: média por área ao longo dos 5 anos."""
    path_out = Path(path_out)
    path_out.parent.mkdir(parents=True, exist_ok=True)
    areas = {"NU_NOTA_CN": "Ciências Natureza", "NU_NOTA_CH": "Ciências Humanas",
             "NU_NOTA_LC": "Linguagens", "NU_NOTA_MT": "Matemática", "NU_NOTA_REDACAO": "Redação"}
    fig, ax = plt.subplots(figsize=(10, 6))
    anos = pandas_df["NU_ANO"].astype(int)
    for col in pandas_df.columns:
        if col.startswith("NU_NOTA") and col in areas:
            ax.plot(anos, pandas_df[col], marker="o", label=areas[col])
    ax.set_xlabel("Ano")
    ax.set_ylabel("Média")
    ax.set_title("ENEM – Média por área (2020–2024)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(path_out, dpi=150)
    plt.close()
    print("Salvo:", path_out)


def salvar_grafico_participantes_por_ano(pandas_df, path_out):
    """Barras: total de participantes por ano."""
    path_out = Path(path_out)
    path_out.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(8, 5))
    anos_str = pandas_df["NU_ANO"].astype(int).astype(str)
    ax.bar(anos_str, pandas_df["total"], color="steelblue", edgecolor="navy", alpha=0.8)
    ax.set_xlabel("Ano")
    ax.set_ylabel("Total de participantes")
    ax.set_title("ENEM – Participantes por ano (2020–2024)")
    plt.tight_layout()
    plt.savefig(path_out, dpi=150)
    plt.close()
    print("Salvo:", path_out)


def salvar_grafico_sexo_por_ano(pandas_df, path_out):
    if pandas_df is None or pandas_df.empty:
        return
    path_out = Path(path_out)
    path_out.parent.mkdir(parents=True, exist_ok=True)
    pivot = pandas_df.pivot(index="NU_ANO", columns="TP_SEXO", values="total").fillna(0)
    pivot.plot(kind="bar", stacked=False, figsize=(9, 5), ax=plt.gca())
    plt.xlabel("Ano")
    plt.ylabel("Total")
    plt.title("ENEM – Participantes por sexo e ano (2020–2024)")
    plt.legend(title="Sexo")
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig(path_out, dpi=150)
    plt.close()
    print("Salvo:", path_out)


def main():
    spark = get_spark()
    (ROOT / DIR_OUTPUT).mkdir(parents=True, exist_ok=True)
    df = carregar_unificado(spark)

    # Coletar amostra para pandas (se dados muito grandes) ou agregados
    df_por_ano = analise_por_ano(df)
    pdf_medias = None
    if df_por_ano:
        pdf_medias = df_por_ano.toPandas()
        salvar_grafico_medias_por_ano(pdf_medias, ROOT / DIR_OUTPUT / "medias_por_ano.png")
        salvar_grafico_participantes_por_ano(pdf_medias[["NU_ANO", "total"]], ROOT / DIR_OUTPUT / "participantes_por_ano.png")

    df_sexo = analise_sexo_por_ano(df)
    if df_sexo:
        pdf_sexo = df_sexo.toPandas()
        salvar_grafico_sexo_por_ano(pdf_sexo, ROOT / DIR_OUTPUT / "sexo_por_ano.png")

    # Média geral por ano
    df_media_geral = analise_media_geral_por_ano(df)
    if df_media_geral:
        pdf_media = df_media_geral.toPandas()
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.plot(pdf_media["NU_ANO"].astype(int), pdf_media["media_geral"], marker="o", color="darkgreen", linewidth=2)
        ax.set_xlabel("Ano")
        ax.set_ylabel("Média geral (5 áreas)")
        ax.set_title("ENEM – Média geral por ano (2020–2024)")
        ax.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(ROOT / DIR_OUTPUT / "media_geral_por_ano.png", dpi=150)
        plt.close()
        print("Salvo: media_geral_por_ano.png")

    # Resumo em texto (insights)
    resumo_path = ROOT / DIR_OUTPUT / "resumo_insights.txt"
    with open(resumo_path, "w", encoding="utf-8") as f:
        f.write("ENEM – Resumo e insights (2020–2024)\n")
        f.write("=" * 50 + "\n\n")
        if pdf_medias is not None and not pdf_medias.empty:
            f.write("Participantes por ano:\n")
            for _, row in pdf_medias.iterrows():
                f.write(f"  {int(row['NU_ANO'])}: {int(row['total']):,}\n")
            f.write("\nMédias por área (último ano disponível):\n")
            for col in pdf_medias.columns:
                if col.startswith("NU_NOTA") and pdf_medias[col].notna().any():
                    f.write(f"  {col}: {pdf_medias[col].iloc[-1]:.1f}\n")
        f.write("\nGráficos gerados: medias_por_ano.png, participantes_por_ano.png, ")
        f.write("sexo_por_ano.png, media_geral_por_ano.png\n")
    print("Resumo salvo:", resumo_path)

    spark.stop()
    print("Análises e gráficos em:", ROOT / DIR_OUTPUT)


if __name__ == "__main__":
    main()
