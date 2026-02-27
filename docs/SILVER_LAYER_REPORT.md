# Silver Layer — Data Quality & Trust

**Project:** ENEM Opportunity & Equity Radar (2020–2024)  
**Stage:** CRISP-DM → Data Preparation (Silver)

---

## 1. What the Silver layer does

### Why raw data cannot be trusted

- **Source heterogeneity:** ENEM microdata vary by year (schema, column order, codes). Raw Bronze preserves exactly what INEP delivers: no single definition of “participant” or “valid score.”
- **Missing and invalid values:** Empty strings, out-of-range scores, invalid categorical codes (e.g. TP_SEXO or UF outside allowed sets) appear in raw data. Using them as-is distorts aggregates and models.
- **Duplicates and scope:** Duplicate NU_INSCRICAO within a year, and mix of participants who sat all exams with those who missed one or more days. Raw counts and means are not comparable to a well-defined analytical population.

Trust is established only after a **single canonical schema**, **explicit validation rules**, and **documented filters** are applied. That is the role of Silver.

### Why validation rules are necessary

- **Defined population:** KPIs (e.g. mean score by UF, growth rate, gap index) require a clear rule: “who is in.” Silver applies the rule “present in all four objective exams” (TP_PRESENCA_CN/CH/LC/MT = 1) so that every retained row has a full set of objective scores.
- **Type and range safety:** Scores are cast to double and bounded to [0, 1000]; presence to [0, 1, 2]; UF and TP_SEXO to allowed values. Invalid or missing codes are normalized or nulled so downstream logic does not see corrupt values.
- **Reproducibility:** Rules are implemented in code and audited (quality_report). The same Bronze + same pipeline yields the same Silver; no hidden manual steps.

Without these rules, “valid rows” and “performance” would be undefined and not reproducible.

### Risk of naive merging

- Merging raw years without a canonical schema causes **column misalignment** (e.g. 2024 has 75 columns, 2020–2023 have 76) and **type inconsistency**. Row-wise union produces wrong mappings or nulls where they are not intended.
- Merging **without** the presence filter mixes participants who took all exams with those who did not. A “mean score” over that mix is not interpretable (e.g. missing exam → missing score; including them as null or zero is wrong).
- Silver applies **mapping_por_ano** and **schema_canonico** before any merge, and keeps only the valid modeling cohort. Gold then aggregates over a single, comparable dataset.

### Impact on modeling integrity

- **Features and targets:** Models and dashboards consume Silver (or Gold built from Silver). If Silver allowed invalid scores, wrong types, or the wrong population, model outputs and KPIs would be biased or broken.
- **Fairness and equity:** Disaggregation by sex, UF, or income (Q006) requires consistent codes and a clear cohort. Silver enforces valid codes and documents nulls (e.g. Q006 non-response), so equity metrics are interpretable.
- **Audit trail:** quality_report and null_report record what was removed and where nulls remain. That supports review and governance instead of opaque “cleaned” data.

---

## 2. Cleaning results summary

| Year | Raw Rows | Valid Rows | % Kept |
|------|----------|------------|--------|
| 2020 | *[from Bronze]* | *[from Silver]* | *[computed]* |
| 2021 | *[from Bronze]* | *[from Silver]* | *[computed]* |
| 2022 | *[from Bronze]* | *[from Silver]* | *[computed]* |
| 2023 | *[from Bronze]* | *[from Silver]* | *[computed]* |
| 2024 | *[from Bronze]* | *[from Silver]* | *[computed]* |

*Populate from Bronze row counts (per-year Parquet) and Silver `enem_participante` count by ano. % Kept = (Valid Rows / Raw Rows) × 100.*

### Patterns to interpret

- **Was 2020 different?** 2020 was the first pandemic year (postponed exam, changed logistics). Raw volume or participation may differ from 2019 or 2021. Compare Raw Rows and % Kept to 2021–2024 to see if 2020 is an outlier; document any difference in reporting.
- **Did pandemic years affect presence?** 2020–2021 may show higher absence (TP_PRESENCA_* ≠ 1) due to health, access, or logistics. The rule *valid_modeling_cohort_all_present* then removes more rows in those years. That is **expected**: we keep only participants with a complete exam for comparability. If % Kept is lower in 2020–2021, it reflects real absence, not a pipeline error.

---

## 3. Top filtering rules

### Which rule removed the most data?

- In this pipeline, **valid_modeling_cohort_all_present** (keep only rows with TP_PRESENCA_CN = TP_PRESENCA_CH = TP_PRESENCA_LC = TP_PRESENCA_MT = 1) almost always removes the largest number of rows.
- **Reason:** Many participants miss one or more exam days. Those rows are dropped so that the analytical cohort is “took all four objective exams.” Deduplication (dedup_nu_inscricao_ano) and other rules (scores in range, normalize UF/sex) typically remove fewer rows.

### Why?

- ENEM is administered over two days; absence on one day implies missing scores for that day. There is no valid way to impute them for “mean objective score” or subject-level KPIs. Excluding incomplete records is the only way to keep metrics comparable across participants and years.

### Is it expected or concerning?

- **Expected.** High removal from this rule is consistent with real absence rates. It is **concerning** only if (a) the rule were mis-specified (e.g. wrong column) or (b) absence were systematically under-recorded in source data. The pipeline and contract align the rule with INEP presence codes; monitoring % Kept by year is sufficient to flag abnormal drops.

---

## 4. Null distribution insights

### Which columns still have nulls after Silver?

- **Scores (NU_NOTA_*):** In the **valid modeling cohort** (all four presences = 1), objective scores and redação are enforced in range [0, 1000]; invalid or out-of-range are set to null. For retained rows, score nulls should be rare unless the source had withheld results. Check **null_report** by year and column.
- **Q006 (income bracket):** Optional; non-response is common. Silver allows null. This column will still show non-zero null % in Silver.
- **SG_UF_RESIDENCIA:** Optional; missing or invalid UF is normalized to null. Null % depends on source completeness.
- **TP_SEXO:** Invalid codes are set to null. Null % should be low if source is complete.

### Are they modeling risks?

- **Q006 nulls:** Yes, for **equity and gap analyses** that use income. Models and gap indices must either exclude Q006-null rows for those analyses or define an explicit “unknown” category and state the limitation.
- **SG_UF_RESIDENCIA / TP_SEXO nulls:** Risk for **disaggregation by geography or sex**. Use only non-null rows for those breakdowns or document “unknown” in reporting.
- **Score nulls in valid cohort:** If non-zero, investigate (e.g. withheld results). For “mean score” and MEDIA_OBJETIVA, exclude or impute only with a documented rule.

### Structural vs random

- **Structural:** Q006 null = questionnaire non-response (often correlated with socioeconomic factors). UF null = missing or invalid location. These are **not** missing at random; document and control in analysis.
- **Random:** Isolated score nulls in an otherwise complete record could be data entry or withholding; treat as sporadic unless patterns appear in null_report by year or region.

---

## 5. Slide version (condensed)

**Title:** Silver Layer — Data Quality & Trust

- **% rows removed:** *[From pipeline: (Raw − Valid) / Raw × 100, per year or overall.]* Silver removes a large share of raw rows mainly by keeping only participants **present in all four objective exams** and by enforcing types and valid codes.
- **Main filtering rule:** **Valid modeling cohort** — retain only rows with TP_PRESENCA_CN = CH = LC = MT = 1. This guarantees a comparable, full-score population for KPIs and models.
- **Key integrity improvement:** A **single canonical schema** and **per-year mapping** (schema_canonico + mapping_por_ano) plus **explicit validation rules** (scores 0–1000, valid UF/sex, dedup) make Silver reproducible and auditable. Raw is no longer used for analytics.
- **Why Gold is now reliable:** Gold is built **only from Silver**. Silver defines who counts (valid cohort), which columns exist (canonical), and which values are allowed. So Gold aggregations (by year, UF, profile) and derived metrics (e.g. MEDIA_OBJETIVA, REDACAO_800_PLUS) rest on a clean, consistent base. No schema drift, no mixed populations, no opaque cleaning.

---

*End of Silver Layer Report*
