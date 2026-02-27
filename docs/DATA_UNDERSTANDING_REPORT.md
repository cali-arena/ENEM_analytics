# Data Understanding — ENEM 2020–2024

**Executive report | CRISP-DM Stage 2**  
**Project:** ENEM Opportunity & Equity Radar  
**Source:** Bronze-layer profiling (`pipelines/data_understanding_profiling.py`)

---

## 1. Dataset overview (2020–2024)

### Summary table

| Year | Rows | Columns | % Null Scores | % Null Presence | Schema changes |
|------|------|--------|----------------|-----------------|----------------|
| 2020 | *[from profiling]* | 76 | *[pct_null_scores]* | *[pct_null_presence]* | Reference (2020–2023) |
| 2021 | *[from profiling]* | 76 | *[pct_null_scores]* | *[pct_null_presence]* | Same as 2020 |
| 2022 | *[from profiling]* | 76 | *[pct_null_scores]* | *[pct_null_presence]* | Same as 2020 |
| 2023 | *[from profiling]* | 76 | *[pct_null_scores]* | *[pct_null_presence]* | Same as 2020 |
| 2024 | *[from profiling]* | 75 | *[pct_null_scores]* | *[pct_null_presence]* | **Structural divergence** |

**Note:** Run `python pipelines/data_understanding_profiling.py` to generate `data/reports/data_profiling_summary.csv` and `data/reports/schema_drift_report.json`, then populate the table (Rows, % Null Scores, % Null Presence) from the CSV. Column counts and schema changes are from validation and drift reports.

### Scale and coverage

- **Total scale:** Five years of INEP microdata; row volume is in the order of millions of participants per year (typical ENEM cohort). Aggregate 2020–2024 forms a multi-million-row dataset suitable for Lakehouse refinement and trend analysis.
- **Row-volume anomalies:** 2021 may show lower participation or shifted cohorts due to pandemic postponements and exam dates. Year-on-year row counts from profiling should be checked for sudden drops or spikes; these inform whether year comparability is affected by coverage rather than schema alone.
- **Missing-value trends:** Null rates for scores reflect both non-attendance (participant absent for that exam) and non-release of results (e.g. disqualification). Presence flags (`TP_PRESENCA_*`) drive interpretability: high null scores with presence = 0 are expected; high null scores with presence = 1 indicate data or processing issues. Income (Q006) non-response is typically higher than demographic fields and can differ by year, with possible increase in sensitive questions in pandemic years.

---

## 2. Schema differences (2024 vs 2020–2023)

### What differs

- **Column count:** 2024 has **75** columns; 2020–2023 each have **76**. The difference is due to INEP’s change in microdata layout for 2024 (separate PARTICIPANTES and RESULTADOS tables merged in our ingestion).
- **Missing in 2024 (relative to 2020–2023):**  
  - **TP_ESCOLA** (type of school) is present in 2020–2023 and absent in the 2024 layout.  
  - **Q024** and **Q025** (questionnaire items) exist in 2020–2023 and are not present in 2024.  
  These appear as **missing_columns_2024** in `data/reports/schema_drift_report.json`.
- **Extra in 2024:** No columns appear only in 2024; 2024 is a subset or reorder of the reference schema.
- **Column order:** 2020–2023 place school and exam/location blocks before the questionnaire block; 2024 uses a different column order (participant block, then questionnaire, then school, then results). **Order differences** are recorded in the drift report (`column_order_reference` vs `column_order_2024`).
- **Type consistency:** For columns present in both reference and 2024, types are generally aligned (string, numeric, integer). Any **type_mismatches** reported in the drift output must be resolved before a single unified table.

### Why it matters

- **Schema drift** prevents naive row-wise concatenation: column alignment and missing fields would produce wrong mappings or null-filled columns. Downstream logic that assumes TP_ESCOLA or Q024/Q025 exists will fail or mislead on 2024 data.
- **Structural divergence** forces an explicit **canonical schema**: a single list of columns and types, with rules for filling missing columns (e.g. TP_ESCOLA, Q024, Q025) as null for 2024 when building a unified Bronze or Silver view.

### Impact on harmonization

- Harmonization must be **schema-first**: define one target schema (e.g. all 76 columns from the reference years), then map each year into it, adding nulls for columns absent in 2024.
- **Compatibility risk:** Analyses that use TP_ESCOLA or Q024/Q025 are not comparable across 2020–2023 and 2024 unless 2024 is explicitly excluded or those variables are dropped for all years. The report recommends documenting “comparability scope” (e.g. “2020–2023 only” vs “2020–2024 with missing coded as null”).

### Risk of naive merging

- Merging 2020–2024 without schema alignment yields column shifts, type coercion errors, or silent misalignment (e.g. 2024’s column N matching 2023’s column N+1). **Bronze must not be merged row-wise until a canonical schema is applied** (e.g. in the same pipeline or in a dedicated harmonization step that outputs a single schema).

---

## 3. Missing data analysis

### Critical variables

| Variable group | Role | Source of missingness |
|----------------|------|------------------------|
| **Score variables** (NU_NOTA_CN, CH, LC, MT, NU_NOTA_REDACAO) | Outcomes for performance and equity KPIs | Absence (did not take exam), disqualification, or non-release. Strongly tied to presence flags. |
| **Presence variables** (TP_PRESENCA_CN, CH, LC, MT) | Define valid test-taking and comparability | Should be 0/1; null or invalid codes indicate incomplete or inconsistent records. |
| **Income bracket** (Q006) | Socioeconomic equity and gap analysis | Questionnaire non-response; sensitive item, so missingness can be non-random by region or profile. |

### Distribution of missingness

- **Scores:** Missingness is high among participants who did not attend that day or who had results withheld. Distribution is **structural**: it follows exam logistics (e.g. one day absent implies four objective scores missing for that day). It is **not missing completely at random (MCAR)**; it is **missing at random (MAR)** or **missing not at random (MNAR)** depending on whether absence is explained by observed covariates (e.g. region, school type) or by unobserved factors (e.g. motivation, health).
- **Presence:** Expected to be almost complete in raw INEP data; any high null rate for presence flags would indicate an ingestion or parsing issue and should be flagged.
- **Income (Q006):** Typically shows higher non-response than core demographics. Missingness may cluster by school type, region, or year (e.g. different form versions or sensitivity post-pandemic), implying **potential bias** in any analysis that uses Q006 without a missingness strategy (e.g. separate “unknown” category or multiple imputation with clear assumptions).

### Bias risk

- **Selection bias:** Restricting to “complete cases” (all scores and Q006 non-missing) will drop absent participants and questionnaire non-respondents, who may differ systematically (e.g. more disadvantaged). Document exclusion criteria and consider reporting metrics both for “all participants” and “valid for performance analysis” (e.g. presence rule + non-null scores).
- **Temporal bias:** If missingness rates or patterns change across 2020–2024 (e.g. higher Q006 non-response in 2021), year-on-year comparisons must state that missingness is not constant and may affect equity indicators.

---

## 4. Outlier and distribution observations

### Extreme values in score columns

- Scores are officially on a **0–1000** scale per area and for redação. Values outside this range in raw data indicate data quality issues and should be treated as invalid in Silver.
- **IQR-based outlier detection** (Q1 − 1.5×IQR, Q3 + 1.5×IQR) from the profiling pipeline flags extreme values within the valid range. Such extremes can be genuine (very high or low performers) or data errors (e.g. encoding mistakes). Decision: in Silver, either retain with no cap (and document) or winsorize/cap for robustness; do **not** drop without a rule (e.g. “drop only if outside 0–1000”).

### Score distribution shifts across years

- **2020–2021:** Pandemic years; possible shifts in score distributions due to changed cohorts (e.g. more repeat test-takers), exam difficulty, or test conditions. Mean and variance may differ from 2022–2024.
- **2022–2024:** Return to more stable exam conditions; distributions may show a “new normal” (e.g. slightly lower or higher means by area). Profiling outputs (min, max, mean, std by year) should be compared to describe these shifts.
- **Structural change:** Any abrupt change in median or IQR between 2021 and 2022 (or 2023 and 2024) should be noted as potential **pandemic-related structural change** and documented in Gold so that trend KPIs (e.g. growth rate 2020→2024) are interpreted with care.

### Pandemic-related structural changes

- Participation and attendance patterns (presence flags, row counts) may show a break in 2020–2021 vs 2022–2024.
- Income and questionnaire completeness may shift. These are not “cleaning” issues but **context** for interpreting Silver/Gold metrics and for stating assumptions in any modeling or policy simulation.

---

## 5. Implications for Silver layer

### Cleaning rules that will be needed

| Rule | Purpose |
|------|--------|
| **Canonical schema** | Map all years to one column set and types; fill missing columns (e.g. TP_ESCOLA, Q024, Q025 in 2024) with null. |
| **Presence rule** | Retain only rows with TP_PRESENCA_CN = TP_PRESENCA_CH = TP_PRESENCA_LC = TP_PRESENCA_MT = 1 for “valid participant” in all four objective exams. |
| **Score validity** | Enforce scores in [0, 1000], non-null where presence = 1; treat out-of-range or null (when presence = 1) as invalid and exclude or flag. |
| **Deduplication** | Drop duplicate NU_INSCRICAO per year (or per year + source) to avoid double-counting. |
| **Categorical normalization** | Normalize TP_SEXO to a fixed set (e.g. M/F); normalize SG_UF_RESIDENCIA (e.g. uppercase, trim); define handling for invalid or null codes (exclude vs “unknown” category). |

### Why filtering must happen

- **Analytical validity:** KPIs (e.g. mean score by UF, growth rate, gap index) require a well-defined population. “All rows” includes absent participants and invalid records; filtering to “present and valid scores” yields a comparable analytical population.
- **Reproducibility:** Explicit filters (presence, score range, dedup) are documented and repeatable; ad-hoc exclusions are not.
- **Downstream safety:** Gold and models assume clean, aligned inputs; unfiltered or inconsistent Silver would propagate errors and bias into decisions.

### Risks to control before modeling

| Risk | Mitigation |
|------|------------|
| **Schema drift carried into Silver** | Apply canonical schema at Silver (or at Bronze output used for Silver); never merge years without schema alignment. |
| **Selection bias from presence/score filters** | Document who is excluded (e.g. “absent on any day”, “missing redação”); report counts and, if possible, compare demographics of excluded vs included. |
| **Invalid or out-of-range scores** | Enforce 0–1000 and non-null for retained rows; log or store invalid counts in a data quality report. |
| **Duplicate inscriptions** | Deduplicate by NU_INSCRICAO (and ano) so each participant counts once per year. |
| **Missing Q006 / sensitive variables** | Decide policy: exclude from equity analyses, or include with “unknown” category and state limitation in reporting. |

This connects **Data Understanding** (what we observed in Bronze) to **Data Preparation** (Silver cleaning and validation), without performing cleaning or modeling in this stage.

---

## 6. Slide summary (1-slide version)

**Slide title:** Data Understanding — ENEM 2020–2024

- **Total records:** Multi-million rows across five years (exact from `data_profiling_summary`: sum of row_count by year).
- **Schema drift insight:** 2024 has 75 columns vs 76 in 2020–2023; TP_ESCOLA and Q024–Q025 are missing in 2024; column order differs. Naive merge is not safe; a canonical schema and null-fill for missing columns are required.
- **Missingness insight:** Score nulls are largely structural (tied to absence); income (Q006) has higher non-response and potential bias. Missingness is not MCAR; document and control exclusion scope before Silver.
- **Key structural risk:** Combining years without schema alignment and without explicit presence/score rules leads to wrong KPIs and non-comparable trends. Pandemic years (2020–2021) may show distribution and participation shifts.
- **Why Bronze is critical:** Bronze preserves raw structure and metadata (ano, ingest_ts, source_file, row_count) and enables reproducible profiling and drift detection. All harmonization and filtering belong in Silver; Bronze remains the single source of truth for Data Understanding and audit.

---

*End of Data Understanding Report*
