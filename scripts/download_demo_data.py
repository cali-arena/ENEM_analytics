"""
Helper script to download a small demo Gold dataset (Parquet) for local testing.

Usage (from repo root):
    python scripts/download_demo_data.py

By default reads DEMO_GOLD_URL from environment or uses the same constant
as bootstrap_cloud.py. This is mainly for local dev; Streamlit Cloud will
call bootstrap_cloud.ensure_demo_gold() at runtime.
"""
from __future__ import annotations

import os
from pathlib import Path

from scripts.bootstrap_cloud import ensure_demo_gold


def main():
    ok, msg = ensure_demo_gold()
    print(msg)
    if not ok:
        print(
            "\nDemo Gold não foi baixado/extrado. "
            "Configure DEMO_GOLD_URL com o link público para demo_gold.zip "
            "ou baixe manualmente e extraia em data/gold."
        )


if __name__ == "__main__":
    main()

