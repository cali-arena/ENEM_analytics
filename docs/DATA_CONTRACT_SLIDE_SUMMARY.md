# Data Contract — Slide Summary

**Project:** ENEM Opportunity & Equity Radar (2020–2024)  
**Stage:** CRISP-DM → Data Preparation (Data Contract)

---

## 1. Top 10 canonical columns (modeling-critical)

| Canonical Column | Type | Validation Rule | Why It Matters |
|------------------|------|-----------------|----------------|
| **NU_INSCRICAO** | string | non_empty_string; no leading/trailing whitespace | Primary key for deduplication and join; no feature use, but required for traceability. |
| **ano** | int | allowed_values: [2020, 2021, 2022, 2023, 2024] | Partition and trend dimension; enables year-on-year KPIs and growth-rate calculations. |
| **SG_UF_RESIDENCIA** | string | valid Brazilian state code (2 chars); uppercase | Geographic dimension for targeting, equity by region, and policy simulation. |
| **TP_SEXO** | string | allowed_values: ["M", "F"]; uppercase | Demographics and fairness; required for equity and disaggregation. |
| **NU_NOTA_CN** | double | between [0, 1000]; null when absent | Objective outcome; input to MEDIA_OBJETIVA and subject-level analysis. |
| **NU_NOTA_CH** | double | between [0, 1000]; null when absent | Objective outcome; input to MEDIA_OBJETIVA and subject-level analysis. |
| **NU_NOTA_LC** | double | between [0, 1000]; null when absent | Objective outcome; input to MEDIA_OBJETIVA and subject-level analysis. |
| **NU_NOTA_MT** | double | between [0, 1000]; null when absent | Objective outcome; input to MEDIA_OBJETIVA and subject-level analysis. |
| **NU_NOTA_REDACAO** | double | between [0, 1000]; null when absent/disqualified | Essay outcome; drives REDACAO_800_PLUS and equity metrics. |
| **Q006** | string | allowed_values: A–Q; uppercase; null_allowed | Income bracket; core for gap analysis and equity KPIs; non-response must be documented. |

*Presence columns (TP_PRESENCA_CN/CH/LC/MT) are required for filtering the valid cohort but are not used as model features; they define who is in scope for performance analytics.*

---

## 2. Why the data contract matters

| Dimension | Explanation |
|-----------|-------------|
| **Schema drift risk** | Raw ENEM layout changes by year (2024: 75 columns; 2020–2023: 76). Without a canonical schema and mapping, pipelines break or misalign columns. The contract fixes one target schema and per-year mapping (including null_source for missing columns), so drift is explicit and handled. |
| **Multi-year comparability** | KPIs (growth rate, gap index, risk by profile) require the same columns and types across 2020–2024. The contract defines a single set of column names and types; Silver applies the contract so Gold and models see a comparable dataset. |
| **Reproducibility** | schema_canonico.yml and mapping_por_ano.yml are versioned; validate_mapping.py checks Bronze against the contract. Anyone can reproduce Silver/Gold from the same Bronze and the same YAML, with no hidden assumptions. |
| **Modeling integrity** | Required columns and validation rules are explicit. Pipelines fail fast if a required column is missing or types conflict. Models and dashboards consume only contract-compliant data, reducing silent errors. |
| **Governance mindset** | The contract is the single source of truth for “what Silver and Gold look like.” Ownership (data_engineering), validity window (2020–2024), and rules are documented; changes go through contract updates and validation. |

---

## 3. Bronze vs Silver vs Gold (clarification)

| Layer | Role | Contract |
|-------|------|----------|
| **Bronze** | Raw ingestion only. One Parquet per year; schema as delivered by the source (CSV). Metadata (ano, ingest_ts, source_file, row_count) may be added. **No cleaning, no renaming, no filtering.** Preserves audit trail and allows reprocessing. |
| **Silver** | Contract applied. Canonical schema and mapping_por_ano are used to select, rename, and cast columns. Validation rules (presence, score range, categorical codes) are enforced; invalid rows are filtered or flagged. Output: one cleaned, harmonised dataset (e.g. enem_cleaned) ready for aggregation and modeling. |
| **Gold** | Business-ready. Built from Silver: aggregations (e.g. by UF, year), derived metrics (MEDIA_OBJETIVA, REDACAO_800_PLUS, Q006_ORDINAL), and partitioning (e.g. by ano). No new raw columns; only computed fields and structures for KPIs and models. |

*Summary: Bronze = raw, untouched. Silver = contract applied. Gold = aggregated and business-ready.*

---

## 4. Slide version (1 slide, condensed)

**Title:** Data Contract — ENEM Opportunity & Equity Radar (2020–2024)

**Key columns (top 10):** NU_INSCRICAO, ano, SG_UF_RESIDENCIA, TP_SEXO, NU_NOTA_CN, NU_NOTA_CH, NU_NOTA_LC, NU_NOTA_MT, NU_NOTA_REDACAO, Q006 — with explicit types and validation (scores 0–1000, presence 0/1/2, UF code, M/F, income A–Q).

**Contract benefit:** Single canonical schema and per-year mapping eliminate schema drift, ensure multi-year comparability, and make Silver/Gold reproducible and auditable; validation script fails if required columns are missing or types conflict.

**Impact on analytics:** All performance, equity, and trend KPIs (growth rate, gap index, risk by profile) are built on contract-compliant Silver/Gold; modeling and policy simulation use the same definitions and no ad-hoc column handling.

---

*End of Data Contract Slide Summary*
