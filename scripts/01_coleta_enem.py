"""
Coleta dos microdados do ENEM – ano a ano (2020 a 2024).
Baixa um ZIP por vez, extrai o CSV de participantes e salva em data/raw/ENEM_AAAA.csv.
Uso: python scripts/01_coleta_enem.py
"""
import os
import sys
import zipfile
import time
from pathlib import Path

import requests
from tqdm import tqdm

# raiz do projeto
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import BASE_URL, ANOS, DIR_DATA, DIR_RAW, PARTICIPANTES_FOLDER, PARTICIPANTES_FILE_PATTERN


def ensure_dirs():
    (ROOT / DIR_DATA).mkdir(parents=True, exist_ok=True)
    (ROOT / DIR_RAW).mkdir(parents=True, exist_ok=True)


def zip_valido(path_zip: Path) -> bool:
    """Verifica se o arquivo é um ZIP válido (download completo)."""
    try:
        with zipfile.ZipFile(path_zip, "r") as z:
            z.testzip()
        return True
    except (zipfile.BadZipFile, OSError):
        return False


def download_zip(ano: int) -> Path:
    """Baixa o ZIP do ENEM do ano e retorna o path local. Usa arquivo temporário e valida o ZIP."""
    url = f"{BASE_URL}/microdados_enem_{ano}.zip"
    path_zip = ROOT / DIR_DATA / f"microdados_enem_{ano}.zip"
    if path_zip.exists() and zip_valido(path_zip):
        print(f"[{ano}] ZIP já existe e é válido: {path_zip.name}")
        return path_zip
    path_tmp = ROOT / DIR_DATA / f"microdados_enem_{ano}.zip.tmp"
    if path_zip.exists() and not zip_valido(path_zip):
        print(f"[{ano}] ZIP existente corrompido/incompleto, baixando novamente...")
        try:
            path_zip.unlink()
        except OSError:
            pass  # arquivo em uso; baixar para .tmp e usar esse
    if not path_zip.exists() or not zip_valido(path_zip):
        print(f"[{ano}] Baixando {url} ...")
        r = requests.get(url, stream=True, timeout=120)
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with open(path_tmp, "wb") as f:
            for chunk in tqdm(r.iter_content(chunk_size=8192), total=max(0, total // 8192), unit="KB"):
                f.write(chunk)
        if not zip_valido(path_tmp):
            path_tmp.unlink(missing_ok=True)
            raise RuntimeError(f"[{ano}] Download concluído mas ZIP inválido. Tente novamente.")
        try:
            if path_zip.exists():
                path_zip.unlink()
            path_tmp.rename(path_zip)
            return path_zip
        except OSError:
            print(f"[{ano}] ZIP em uso; usando cópia em {path_tmp.name}")
            return path_tmp
    return path_zip


def find_participantes_csv(zip_path: Path, ano: int) -> str:
    """Encontra o CSV de PARTICIPANTES (não itens da prova): header deve conter NU_INSCRICAO."""
    with zipfile.ZipFile(zip_path, "r") as z:
        names = z.namelist()
    candidatos = [
        n for n in names
        if n.endswith(".csv") and ("MICRODADOS" in n.upper() or "DADOS" in n) and str(ano) in n
    ]
    if not candidatos:
        candidatos = [n for n in names if n.endswith(".csv") and ("DADOS" in n or "MICRODADOS" in n.upper())]
    # Preferir o arquivo cujo header contém NU_INSCRICAO (tabela de participantes)
    with zipfile.ZipFile(zip_path, "r") as z:
        for n in candidatos:
            try:
                with z.open(n) as f:
                    header = f.read(4096).decode("utf-8", errors="replace").split("\n")[0]
                if "NU_INSCRICAO" in header:
                    return n
            except Exception:
                continue
    if candidatos:
        return candidatos[0]
    raise FileNotFoundError(f"Nenhum CSV de participantes no ZIP para ano {ano}. Exemplos: {names[:25]}")


def extract_participantes(zip_path: Path, ano: int) -> Path:
    """Extrai apenas o CSV de participantes para data/raw/ENEM_AAAA.csv."""
    out_csv = ROOT / DIR_RAW / f"ENEM_{ano}.csv"
    if out_csv.exists():
        print(f"[{ano}] CSV já extraído: {out_csv.name}")
        return out_csv
    csv_name = find_participantes_csv(zip_path, ano)
    with zipfile.ZipFile(zip_path, "r") as z:
        with z.open(csv_name) as src:
            content = src.read()
    # ENEM usa ; como separador e encoding pode ser latin-1
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            text = content.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        text = content.decode("utf-8", errors="replace")
    out_csv.write_text(text, encoding="utf-8")
    # Validação rápida: garantir que é o CSV oficial (colunas ENEM)
    first_line = text.split("\n")[0]
    if "NU_NOTA_MT" in first_line and "TP_SEXO" in first_line and "NU_INSCRICAO" in first_line:
        print(f"[{ano}] Extraído e validado (dados oficiais INEP) -> {out_csv}")
    else:
        print(f"[{ano}] Extraído (verifique o header) -> {out_csv}")
    return out_csv


def coleta_ano(ano: int) -> Path:
    """Faz download e extração para um único ano."""
    ensure_dirs()
    path_zip = download_zip(ano)
    return extract_participantes(path_zip, ano)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Coleta microdados ENEM (ano a ano)")
    parser.add_argument("anos", nargs="*", type=int, default=None, help="Anos (ex: 2020 2021). Se vazio, usa config.ANOS")
    args = parser.parse_args()
    anos = args.anos if args.anos else ANOS
    ensure_dirs()
    for ano in anos:
        try:
            coleta_ano(ano)
        except Exception as e:
            print(f"[{ano}] Erro: {e}")
            raise
        if ano != anos[-1]:
            time.sleep(2)
    print("Coleta concluída. Arquivos em:", ROOT / DIR_RAW)


if __name__ == "__main__":
    main()
