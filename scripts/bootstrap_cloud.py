"""
Cloud bootstrap helpers for Streamlit Community Cloud.

Responsibilities:
- Ensure a small demo Gold dataset exists so the app can start quickly.
- Never block forever: use timeouts and friendly warnings.

This script assumes an external ZIP with demo Parquet files is available.
You must set DEMO_GOLD_URL to a public HTTPS URL (e.g. GitHub Release asset).
"""
from __future__ import annotations

import os
import time
import zipfile
from pathlib import Path
from typing import Tuple

import requests

from app.paths import GOLD_DIR, ROOT

# Placeholder – set this to a real URL in your deployment repo.
DEMO_GOLD_URL = os.environ.get(
    "DEMO_GOLD_URL",
    "https://example.com/demo_gold.zip",  # TODO: replace with real URL
)


def _gold_present() -> bool:
    """Returns True if at least one kpis_uf_ano parquet exists."""
    path = GOLD_DIR / "kpis_uf_ano"
    if path.is_dir():
        return any(path.rglob("*.parquet"))
    if path.with_suffix(".parquet").exists():
        return True
    return False


def _download_demo_zip(dest_zip: Path, timeout_sec: int = 30) -> Tuple[bool, str]:
    if not DEMO_GOLD_URL or DEMO_GOLD_URL.startswith("https://example.com"):
        return False, "DEMO_GOLD_URL não configurada para demo_gold.zip."
    try:
        resp = requests.get(DEMO_GOLD_URL, stream=True, timeout=timeout_sec)
        resp.raise_for_status()
        with dest_zip.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        return True, "Download concluído."
    except Exception as e:
        return False, f"Falha ao baixar demo_gold.zip: {e}"


def ensure_demo_gold(max_wait_sec: int = 60) -> Tuple[bool, str]:
    """
    Ensure that a small demo Gold dataset exists.

    Returns (ok, message).
    - ok=True: Gold pronto para uso.
    - ok=False: app pode continuar, mas deve mostrar aviso de que não há dados.
    """
    if _gold_present():
        return True, "data/gold já contém demo ou dataset completo."

    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    temp_dir = ROOT / "tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    zip_path = temp_dir / "demo_gold.zip"

    start = time.perf_counter()
    ok, msg = _download_demo_zip(zip_path)
    if not ok:
        return False, f"Bootstrap Cloud: {msg}"

    # Extract with timeout guard
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(GOLD_DIR)
    except Exception as e:
        return False, f"Erro ao extrair demo_gold.zip: {e}"
    finally:
        try:
            zip_path.unlink(missing_ok=True)  # type: ignore[arg-type]
        except Exception:
            pass

    elapsed = time.perf_counter() - start
    if elapsed > max_wait_sec:
        return False, "Bootstrap Cloud demorou demais para completar."

    if not _gold_present():
        return False, "Bootstrap Cloud não encontrou Parquets após extração."

    return True, f"Demo Gold carregado em {elapsed:.1f}s."

