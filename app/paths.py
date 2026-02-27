"""
Common path constants for the ENEM Radar project.

Centralizes ROOT and data directories so local and Streamlit Cloud
use the same layout.
"""
from __future__ import annotations

from pathlib import Path

ROOT: Path = Path(__file__).resolve().parents[1]
DATA_DIR: Path = ROOT / "data"
RAW_DIR: Path = DATA_DIR / "raw"
BRONZE_DIR: Path = DATA_DIR / "bronze"
SILVER_DIR: Path = DATA_DIR / "silver"
GOLD_DIR: Path = DATA_DIR / "gold"
REPORTS_DIR: Path = ROOT / "reports"
LOGS_DIR: Path = ROOT / "logs"

