"""
Coleta ENEM 2024 – formato específico INEP 2024.
O ZIP 2024 traz tabelas separadas: PARTICIPANTES_2024.csv e RESULTADOS_2024.csv.
Este script junta as duas, gera um único CSV no mesmo formato dos anos anteriores
(com notas e presença) e substitui data/raw/ENEM_2024.csv.
Uso: python scripts/02_coleta_enem_2024_completo.py
"""
import csv
import sys
from pathlib import Path

import requests
from tqdm import tqdm

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import BASE_URL, DIR_DATA, DIR_RAW


def zip_valido(path: Path) -> bool:
    import zipfile
    try:
        with zipfile.ZipFile(path, "r") as z:
            z.testzip()
        return True
    except Exception:
        return False


def download_zip_2024() -> Path:
    """Baixa microdados_enem_2024.zip se necessário."""
    path_zip = ROOT / DIR_DATA / "microdados_enem_2024.zip"
    if path_zip.exists() and zip_valido(path_zip):
        print("[2024] ZIP já existe e é válido.")
        return path_zip
    url = f"{BASE_URL}/microdados_enem_2024.zip"
    path_tmp = ROOT / DIR_DATA / "microdados_enem_2024.zip.tmp"
    print("[2024] Baixando", url, "...")
    r = requests.get(url, stream=True, timeout=120)
    r.raise_for_status()
    total = int(r.headers.get("content-length", 0))
    with open(path_tmp, "wb") as f:
        for chunk in tqdm(r.iter_content(chunk_size=8192), total=max(0, total // 8192), unit="KB"):
            f.write(chunk)
    if not zip_valido(path_tmp):
        path_tmp.unlink(missing_ok=True)
        raise RuntimeError("Download concluído mas ZIP inválido.")
    if path_zip.exists():
        path_zip.unlink(missing_ok=True)
    path_tmp.rename(path_zip)
    return path_zip


def ler_csv_do_zip(zip_path: Path, nome_arquivo: str, encoding: str = "utf-8"):
    """Lê um CSV dentro do ZIP como linhas (lista de listas)."""
    import zipfile
    with zipfile.ZipFile(zip_path, "r") as z:
        with z.open(nome_arquivo) as f:
            raw = f.read().decode(encoding, errors="replace")
    reader = csv.reader(raw.splitlines(), delimiter=";")
    return list(reader)


def coleta_2024_completo():
    (ROOT / DIR_DATA).mkdir(parents=True, exist_ok=True)
    (ROOT / DIR_RAW).mkdir(parents=True, exist_ok=True)

    path_zip = download_zip_2024()

    print("[2024] Lendo PARTICIPANTES_2024.csv ...")
    part = ler_csv_do_zip(path_zip, "DADOS/PARTICIPANTES_2024.csv")
    header_part = part[0]
    rows_part = part[1:]

    print("[2024] Lendo RESULTADOS_2024.csv ...")
    res = ler_csv_do_zip(path_zip, "DADOS/RESULTADOS_2024.csv")
    header_res = res[0]
    rows_res = res[1:]

    n_part = len(rows_part)
    n_res = len(rows_res)
    if n_part != n_res:
        print(f"  AVISO: PARTICIPANTES tem {n_part} linhas, RESULTADOS tem {n_res}. Usando mínimo.")
    n = min(n_part, n_res)

    # Colunas de RESULTADOS que não estão em PARTICIPANTES (evitar duplicata)
    cols_res = header_res
    cols_part = header_part
    duplicados = {"NU_ANO", "CO_MUNICIPIO_PROVA", "NO_MUNICIPIO_PROVA", "CO_UF_PROVA", "SG_UF_PROVA"}
    cols_res_extra = [c for c in cols_res if c not in cols_part and c not in duplicados]
    # Ordem final: igual aos anos anteriores (participantes + escola/presença/notas + questionário já está em participantes)
    header_final = header_part + cols_res_extra

    print("[2024] Juntando tabelas (por ordem de linha) ...")
    out_path = ROOT / DIR_RAW / "ENEM_2024.csv"
    idx_res_extra = [cols_res.index(c) for c in cols_res_extra]
    n_col_part = len(header_part)
    n_col_extra = len(cols_res_extra)
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(header_final)
        for i in range(n):
            row_p = rows_part[i] if i < len(rows_part) else []
            row_r = rows_res[i] if i < len(rows_res) else []
            row_p = (row_p + [""] * n_col_part)[:n_col_part]
            row_extra = [row_r[j] if j < len(row_r) else "" for j in idx_res_extra]
            w.writerow(row_p + row_extra)

    print("[2024] CSV completo salvo:", out_path)
    print("  Colunas totais:", len(header_final))
    print("  Linhas (registros):", n)
    if "NU_NOTA_MT" in header_final:
        print("  Estrutura: completa (com notas e presença).")
    return out_path


if __name__ == "__main__":
    coleta_2024_completo()
