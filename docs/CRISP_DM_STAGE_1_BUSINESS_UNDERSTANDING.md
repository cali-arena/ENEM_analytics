# CRISP-DM Stage 1: Business Understanding

## ENEM Opportunity & Equity Radar (2020–2024)

**Document version:** 1.0  
**Stage:** Business Understanding (CRISP-DM Phase 1)  
**Data product:** Decision-intelligence platform on ENEM public microdata, Lakehouse (Bronze/Silver/Gold).

---

## 1. Market Problem Definition

### Problem → Consequence → Opportunity

| Layer | Description |
|-------|-------------|
| **Problem** | Education inequality and performance trends are not observable at scale in a single, comparable frame. ENEM microdata are public but fragmented by year, schema, and format; there is no standardised, multi-year view that supports geographic, demographic, and subject-level comparison. |
| **Consequence** | Public institutions lack dynamic, comparable analytics. EdTechs and private universities operate without systematic geographic or profile intelligence. Municipal and state education departments design policies from lagging indicators and local samples. Cursinhos and policy analysts cannot reliably answer “where did performance grow or decline?” or “which profiles are at higher risk?”. Decisions become reactive and poorly targeted. |
| **Opportunity** | A single, harmonised 2020–2024 ENEM asset (Bronze → Silver → Gold) enables reproducible KPIs by UF/municipality, profile, and subject. Stakeholders can identify growth/decline hotspots, absence and low-performance risk by segment, and subject-level equity gaps—supporting targeting, product design, and policy simulation. |

### Why ENEM data matters strategically

- **National coverage:** ENEM is the main gateway to higher education in Brazil; microdata represent the full test-taking population by year.
- **Rich covariates:** Demographics (e.g. TP_SEXO), location (SG_UF_PROVA/SG_UF_RESIDENCIA), socioeconomic (Q006 income bracket), presence (TP_PRESENCA_*), and scores (CN, CH, LC, MT, Redação) allow profile-based and geographic analytics.
- **Public and reproducible:** INEP microdata are open; a Lakehouse built on them is auditable and reusable across organisations.

### Why 2020–2024 is relevant (post-pandemic shift)

- **2020–2021:** Pandemic disruption (remote teaching, exam postponements, changed participation).
- **2022–2024:** Return to in-person exams and “new normal” in K–12 and prep.
- **Comparable window:** A fixed 5-year window allows trend detection (growth/decline), risk profiling, and gap analysis with consistent metrics and definitions.

---

## 2. Target Users

| Segment | Who they are | Decision they need | How this product helps |
|---------|----------------|---------------------|-------------------------|
| **1. EdTech companies** | Providers of prep platforms, adaptive learning, and content. | Where to prioritise marketing, which regions/profiles to target, which subjects to emphasise. | Geographic and profile-level performance and growth; identification of high-opportunity UFs/municipalities and underperforming subject areas. |
| **2. Private universities** | HEIs using ENEM for selection and scholarships. | Where to recruit, how to set cut-offs, which profiles to support with aid. | Performance and participation by UF/municipality and by profile (e.g. income Q006); trend in scores to calibrate expectations. |
| **3. State/Municipal education departments** | SEDUCs and municipal secretariats. | Which schools/regions to prioritise, which subjects need intervention, how to allocate resources. | Performance growth/decline by geography; subject-level gaps; absence and low-performance risk by profile to target support. |
| **4. Cursinhos** | Prep courses (private and social). | Where to open units, which subjects to reinforce, which audiences to focus on. | Same as EdTech: geographic and subject intelligence; profile clustering for absence and performance risk. |
| **5. Public policy analysts** | Think tanks, ministries, researchers. | How to measure equity, where inequality is widening, what to simulate (e.g. income-based quotas). | Gap indices by income/region; trend KPIs; a clean Gold layer for policy simulation (e.g. impact of targeting by Q006 or UF). |

---

## 3. Business Questions (KPIs)

Three main questions drive the Gold layer design. For each: required Gold tables, aggregations, metric definitions, and formulas.

---

### Q1. Where did performance grow or decline most by UF/municipality?

**Purpose:** Identify geographic hotspots of improvement or decline for targeting and policy.

| Element | Definition |
|--------|------------|
| **Required Gold tables** | `gold_kpi_performance_by_uf_ano`, `gold_kpi_performance_by_municipio_ano` (or equivalent aggregates from `enem_model_dataset` / `enem_2020_2024_unified`). |
| **Required aggregations** | By `SG_UF_RESIDENCIA` (and optionally `CO_MUNICIPIO`/name), by `ano`: count of valid participants, mean of `MEDIA_OBJETIVA`, mean of `NU_NOTA_REDACAO`. |
| **Metric definitions** | **Mean_UF_ano** = mean of `MEDIA_OBJETIVA` for valid participants in that UF (and year). **Growth_Rate** = relative change from 2020 to 2024. |
| **Formula** | **Growth_Rate** = (Mean_2024 − Mean_2020) / Mean_2020  
  (Interpret: positive = improvement; negative = decline; scale as decimal, e.g. 0.05 = 5% growth.) |

---

### Q2. Which candidate profiles show higher absence or low-performance risk?

**Purpose:** Segment candidates by risk (absent or low scores) for targeting support and communication.

| Element | Definition |
|--------|------------|
| **Required Gold tables** | Silver/Gold with `TP_PRESENCA_*`, `MEDIA_OBJETIVA`, `NU_NOTA_REDACAO`, `TP_SEXO`, `Q006` (and optionally `Q006_ORDINAL`), `SG_UF_RESIDENCIA`, `ano`. Aggregates by profile group. |
| **Required aggregations** | By profile group (e.g. UF × Q006, or TP_SEXO × Q006, or Q006 alone): total participants (Bronze/Silver count), count with full presence (all TP_PRESENCA_* = 1), count with MEDIA_OBJETIVA below threshold (e.g. &lt; 450). |
| **Metric definitions** | **Absence_Risk** = % of participants in the group with at least one of TP_PRESENCA_CN, TP_PRESENCA_CH, TP_PRESENCA_LC, TP_PRESENCA_MT ≠ 1.  
  **Low_Performance_Risk** = % of participants with valid presence and MEDIA_OBJETIVA &lt; 450 (or another defined cutoff). |
| **Formula** | **Absence_Risk** = (1 − (count with all presence = 1) / (total in group)) × 100  
  **Low_Performance_Risk** = (count with MEDIA_OBJETIVA &lt; cutoff) / (count with valid presence) × 100 |

---

### Q3. Which subject areas show increasing performance gaps?

**Purpose:** Detect whether inequality in a given subject (e.g. Mathematics vs Languages) is widening over 2020–2024.

| Element | Definition |
|--------|------------|
| **Required Gold tables** | Gold with `NU_NOTA_CN`, `NU_NOTA_CH`, `NU_NOTA_LC`, `NU_NOTA_MT`, `Q006` or `Q006_ORDINAL`, `ano`. Aggregations by subject (area), by income group (e.g. quartiles), by year. |
| **Required aggregations** | By `ano`, by subject (CN, CH, LC, MT): mean score for top income quartile (e.g. Q006_ORDINAL in top 25%) and bottom quartile (bottom 25%). Optionally by UF. |
| **Metric definitions** | **Gap_Index_ano** = difference between mean score of top quartile and mean score of bottom quartile in that subject and year. **Gap_Change** = change in this gap from 2020 to 2024. |
| **Formula** | **Gap_Index_ano** = Mean_top_Q006(area, ano) − Mean_bottom_Q006(area, ano)  
  **Gap_Change** = Gap_Index_2024 − Gap_Index_2020  
  (Positive Gap_Change = widening inequality in that subject.) |

---

## 4. Product Value Proposition

**Positioning statement:**

*ENEM Opportunity & Equity Radar is a decision-intelligence platform that turns five years of ENEM microdata (2020–2024) into comparable, geography- and profile-aware KPIs so that EdTechs, universities, education departments, cursinhos, and policy analysts can target interventions, design products, and evaluate equity with evidence.*

**Capabilities:**

- **Evidence-based insights:** All metrics derive from INEP microdata via a documented Bronze → Silver → Gold pipeline; KPIs are reproducible and formula-defined.
- **Geographic intelligence:** Performance and growth by UF (and optionally municipality) support regional targeting and resource allocation.
- **Profile clustering:** Segmentation by income (Q006), sex (TP_SEXO), and location supports absence-risk and low-performance-risk analysis.
- **Trend detection:** 2020–2024 window enables growth rates, gap changes, and risk trends over time.
- **Policy simulation potential:** Gold layer and clear metric definitions allow “what-if” analyses (e.g. impact of focusing on bottom income quartile or specific UFs).

---

## 5. Success Metrics (Project Evaluation)

### Technical

| Criterion | Measurable target |
|-----------|--------------------|
| Gold tables built | At least one unified Gold dataset (e.g. `enem_2020_2024_unified.parquet`) and model-ready dataset (e.g. `enem_model_dataset.parquet`) with MEDIA_OBJETIVA, REDACAO_800_PLUS, Q006_ORDINAL, and canonical schema. |
| KPIs reproducible | Q1 Growth_Rate, Q2 Absence_Risk / Low_Performance_Risk, Q3 Gap_Index and Gap_Change computable from Gold using documented formulas. |
| 2020–2024 comparable | Same canonical schema and metric definitions across all five years; no schema drift in Gold. |

### Business

| Criterion | Measurable target |
|-----------|--------------------|
| Actionable insights | At least 3 concrete findings (e.g. “UF X grew 8%”, “Profile Y has 2× absence risk”, “Mathematics gap widened by Z points”). |
| Visualisation | At least 1 high-impact visual per main KPI (growth by UF, risk by profile, gap by subject/year). |
| Policy/product use | At least 1 simulated recommendation (e.g. “If we target bottom Q006 quartile in UF W, we impact N candidates”). |

### Academic / Methodology

| Criterion | Measurable target |
|-----------|--------------------|
| CRISP-DM alignment | Explicit Business Understanding (this document), with clear link to Data Understanding and subsequent stages. |
| Business framing | Problem → Consequence → Opportunity and user segments documented. |
| Value articulation | Value proposition and success criteria stated in measurable terms. |

---

## 6. Connection to Lakehouse Architecture

| Layer | Role | Outputs | Enables |
|-------|------|---------|--------|
| **Bronze** | Raw ingestion and standardisation. Read CSV per year; add `ano`, `ingestion_ts`, `source_file`; cast to canonical schema. | `data/bronze/enem_{2020..2024}.parquet` | Single schema across years; audit trail; repeatable ingestion. |
| **Silver** | Cleaning and validation. Apply presence rule (all four objective exams present); validate score ranges; deduplicate; normalize TP_SEXO and SG_UF_RESIDENCIA. | `data/silver/enem_cleaned.parquet`, `data/silver/data_quality_report.json` | Only valid participants for KPIs; quality metrics for monitoring. |
| **Gold** | Business-ready aggregates and model-ready features. Add MEDIA_OBJETIVA, REDACAO_800_PLUS, Q006_ORDINAL; optional aggregations by UF/ano, profile/ano, subject/income/ano. | `data/gold/enem_model_dataset.parquet` (partitioned by ano), `data/gold/enem_2020_2024_unified.parquet`; optional KPI tables. | **Decision-making:** Q1 (growth by geography), Q2 (risk by profile), Q3 (gaps by subject) computed from Gold without re-reading raw data. Same column order and types prevent schema drift and support dashboards and models. |

**How Gold enables decisions:** Gold holds participant-level and (if built) pre-aggregated KPI tables. Stakeholders query Gold to answer “where did performance grow?”, “who is at risk?”, and “where are gaps widening?” without reprocessing Silver or Bronze. Reproducibility is guaranteed by fixed formulas and a single pipeline.

---

## 7. Presentation Slide Summary (1-slide structure)

**Copy-ready structure for one executive slide:**

---

**Title:** ENEM Opportunity & Equity Radar (2020–2024)

**Problem:** Education inequality and performance trends are not visible at scale; ENEM data are fragmented by year and schema. Public and private actors lack comparable, multi-year analytics.

**Target users:** EdTechs, private universities, state/municipal education departments, cursinhos, public policy analysts.

**3 business questions:**
1. **Where** did performance grow or decline most by UF/municipality? → Growth_Rate = (Mean_2024 − Mean_2020) / Mean_2020.
2. **Which** candidate profiles show higher absence or low-performance risk? → Absence_Risk and Low_Performance_Risk by segment (e.g. Q006, UF).
3. **Which** subject areas show increasing performance gaps? → Gap_Index (top vs bottom income quartile) and Gap_Change 2020→2024.

**Product value:** Decision-intelligence platform that delivers evidence-based, geographic, and profile-level KPIs from ENEM 2020–2024 for targeting, trend detection, and policy simulation.

**Why it matters now:** 2020–2024 captures pandemic disruption and recovery; a comparable 5-year window is the right basis for targeting and equity analysis.

---

*End of CRISP-DM Stage 1: Business Understanding*
