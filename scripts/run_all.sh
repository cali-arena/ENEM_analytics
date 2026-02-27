#!/usr/bin/env bash
# ENEM Opportunity & Equity Radar — run pipelines in order (Windows-friendly bash: Git Bash / WSL)
# Run from repo root: bash scripts/run_all.sh
# Optional: pass "ml" to also run ML and clusters after gold.

set -e
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "=== Bronze ==="
python scripts/run_bronze.py

echo "=== Silver ==="
python pipelines/silver_cleaning_pipeline.py

echo "=== Gold ==="
python pipelines/gold_star_schema.py

if [ "$1" = "ml" ]; then
  echo "=== ML ==="
  python ml/train_ml_temporal.py
  echo "=== Clusters ==="
  python ml/cluster_profiles.py
fi

echo "=== Smoke test ==="
python scripts/smoke_test.py

echo "Done."
