"""
Gera dados mockados (Parquet) e envia para o R2 para o app_s3_duckdb.py exibir algo.

Uso (na raiz do repo, com .streamlit/secrets.toml ou env com R2_*):
  pip install boto3 pandas pyarrow
  python scripts/build_and_upload_demo_r2.py

O que sobe:
  - gold/kpis_uf_ano/*.parquet (2020–2024, 27 UFs, KPIs sintéticos)
  - gold/cluster_profiles.parquet
  - gold/cluster_evolution_uf_ano.parquet
  - silver/quality_report/part-0.parquet
  - silver/null_report/part-0.parquet

Não precisa de CSV; tudo sintético. Depois rode o app e faça Refresh cache.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

OUT_DIR = ROOT / "tmp" / "demo_r2_upload"

# UFs para o demo (27 + NA opcional)
UFS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG",
    "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
]
ANOS = [2020, 2021, 2022, 2023, 2024]


def build_demo_parquets():
    import pandas as pd

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "gold").mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "silver").mkdir(parents=True, exist_ok=True)

    # ---- gold/kpis_uf_ano (2020-2024, 27 UFs — igual ao host para tendências e Radar) ----
    rows = []
    for ano in ANOS:
        for i, uf in enumerate(UFS):
            # Tendência por ano (crescimento da média) + variação por UF
            base_obj = 500.0 + (ano - 2020) * 5 + (i % 5) * 6
            base_red = 565.0 + (ano - 2020) * 8 + (i % 4) * 5
            count = 50000 + (ano * 1000) + (i * 200)
            presenca = 92.0 + (i % 6) + (ano - 2020) * 0.5
            rows.append({
                "ano": ano,
                "sg_uf_residencia": uf,
                "count_participantes": count,
                "media_objetiva": round(base_obj, 2),
                "media_redacao": round(base_red, 2),
                "p25_redacao": base_red - 80,
                "p50_redacao": base_red,
                "p75_redacao": base_red + 80,
                "p25_objetiva": base_obj - 70,
                "p50_objetiva": base_obj,
                "p75_objetiva": base_obj + 70,
                "pct_top800_redacao": 8.0 + (i % 4),
                "pct_presence_full": min(99.5, round(presenca, 2)),
            })
    df_kpis = pd.DataFrame(rows)
    kpis_dir = OUT_DIR / "gold" / "kpis_uf_ano"
    kpis_dir.mkdir(parents=True, exist_ok=True)
    df_kpis.to_parquet(kpis_dir / "part-0.parquet", index=False)
    print(f"  gold/kpis_uf_ano: {len(df_kpis)} linhas")

    # ---- gold/cluster_profiles.parquet (app usa size, cluster_id; media_objetiva para tabela) ----
    profiles = pd.DataFrame({
        "cluster_id": [0, 1, 2, 3, 4, 5, 6],
        "size": [92433, 26969, 55545, 42000, 38000, 31000, 25000],
        "media_redacao": [477.43, 759.99, 802.74, 650.0, 720.0, 680.0, 580.0],
        "media_obj": [431.31, 600.83, 612.52, 560.0, 590.0, 550.0, 500.0],
        "media_objetiva": [431.31, 600.83, 612.52, 560.0, 590.0, 550.0, 500.0],  # alias para exibição
        "presence_rate": [0.85, 0.98, 0.99, 0.95, 0.97, 0.94, 0.90],
        "top_3_ufs": ["BA PA SP", "MG RJ SP", "BA MG SP", "PR SC RS", "SP MG RJ", "CE PE BA", "MA PI CE"],
    })
    profiles.to_parquet(OUT_DIR / "gold" / "cluster_profiles.parquet", index=False)
    print("  gold/cluster_profiles.parquet")

    # ---- gold/cluster_evolution_uf_ano.parquet (todas as UFs para "Evolução por UF" igual ao host) ----
    ev_rows = []
    for ano in ANOS:
        for uf in UFS:
            for cid in range(7):
                pct = 0.12 + (cid * 0.08) + ((ano - 2020) * 0.01) + (hash(uf) % 5) / 100.0
                ev_rows.append({"ano": ano, "uf": uf, "cluster_id": cid, "pct_participants": round(pct, 4)})
    evolution = pd.DataFrame(ev_rows)
    evolution.to_parquet(OUT_DIR / "gold" / "cluster_evolution_uf_ano.parquet", index=False)
    print(f"  gold/cluster_evolution_uf_ano.parquet: {len(evolution)} linhas")

    # ---- silver/quality_report ----
    qr = pd.DataFrame([
        {"rule_name": "idade_fora_faixa", "year": 2020, "rows_removed": 1200},
        {"rule_name": "idade_fora_faixa", "year": 2021, "rows_removed": 1100},
        {"rule_name": "nota_negativa", "year": 2020, "rows_removed": 800},
        {"rule_name": "nota_negativa", "year": 2021, "rows_removed": 750},
        {"rule_name": "duplicata_inscricao", "year": 2020, "rows_removed": 320},
        {"rule_name": "duplicata_inscricao", "year": 2021, "rows_removed": 280},
        {"rule_name": "presenca_invalida", "year": 2020, "rows_removed": 500},
        {"rule_name": "presenca_invalida", "year": 2021, "rows_removed": 450},
    ])
    (OUT_DIR / "silver" / "quality_report").mkdir(parents=True, exist_ok=True)
    qr.to_parquet(OUT_DIR / "silver" / "quality_report" / "part-0.parquet", index=False)
    print(f"  silver/quality_report: {len(qr)} linhas")

    # ---- silver/null_report ----
    nr = pd.DataFrame([
        {"column_name": "tp_sexo", "year": 2020, "pct_null": 0.1},
        {"column_name": "tp_sexo", "year": 2021, "pct_null": 0.08},
        {"column_name": "Q006", "year": 2020, "pct_null": 2.5},
        {"column_name": "Q006", "year": 2021, "pct_null": 2.2},
        {"column_name": "NU_NOTA_REDACAO", "year": 2020, "pct_null": 1.2},
        {"column_name": "NU_NOTA_REDACAO", "year": 2021, "pct_null": 1.0},
        {"column_name": "SG_UF_RESIDENCIA", "year": 2020, "pct_null": 0.05},
        {"column_name": "SG_UF_RESIDENCIA", "year": 2021, "pct_null": 0.04},
    ])
    (OUT_DIR / "silver" / "null_report").mkdir(parents=True, exist_ok=True)
    nr.to_parquet(OUT_DIR / "silver" / "null_report" / "part-0.parquet", index=False)
    print(f"  silver/null_report: {len(nr)} linhas")

    return OUT_DIR


def upload_to_r2(local_root: Path):
    from scripts.upload_to_r2 import get_s3_client, upload_dir, key_from_path

    s3, bucket = get_s3_client()
    total_up, total_skip = 0, 0

    for prefix in ("gold", "silver"):
        local_dir = local_root / prefix
        if not local_dir.is_dir():
            continue
        print(f"[{prefix}] {local_dir} → s3://{bucket}/{prefix}/")
        up, skip = upload_dir(s3, local_dir, prefix, bucket)
        total_up += up
        total_skip += skip
        print(f"  uploaded={up} skipped={skip}")

    print(f"Done. Total uploaded: {total_up}, skipped: {total_skip}")
    print("No app: Refresh cache e recarregue a página.")


def main():
    print("Gerando Parquets mockados em", OUT_DIR)
    build_demo_parquets()
    print("Enviando para R2...")
    upload_to_r2(OUT_DIR)
    print("Demo no R2 pronto. Rode: streamlit run app/app_s3_duckdb.py")


if __name__ == "__main__":
    main()
