# ENEM Opportunity & Equity Radar — local deployment
# Run from repo root. Windows: use Git Bash or WSL for make; or use scripts/run_all.sh

PYTHON ?= python

# Download raw CSVs (optional; run once before bronze)
coleta:
	$(PYTHON) scripts/01_coleta_enem.py
	$(PYTHON) scripts/02_coleta_enem_2024_completo.py

bronze:
	$(PYTHON) scripts/run_bronze.py

silver:
	$(PYTHON) pipelines/silver_cleaning_pipeline.py

gold:
	$(PYTHON) pipelines/gold_star_schema.py

ml:
	$(PYTHON) ml/train_ml_temporal.py

clusters:
	$(PYTHON) ml/cluster_profiles.py

app:
	streamlit run app/app.py

demo:
	$(PYTHON) demo/run_live_demo.py

smoke:
	$(PYTHON) scripts/smoke_test.py

# Run pipelines in order (bronze → silver → gold); then optional ml + clusters
all: bronze silver gold

all-ml: all ml clusters

.PHONY: coleta bronze silver gold ml clusters app demo smoke all all-ml
