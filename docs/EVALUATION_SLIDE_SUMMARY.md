# Evaluation Slide Summary — ENEM Opportunity & Equity Radar (2020–2024)

**CRISP-DM: Evaluation.** Three slides; all statements tied to Gold-layer metrics.

---

## Slide 1 — Unified Trends 2020–2024

**Overall mean trend**
- Mean objective score (media_objetiva) by year: level and direction (up/down/stable) from Gold.
- Redação mean and % ≥800 by year: whether excellence (≥800) expanded or contracted.

**Presence trend**
- Gold valid cohort = 100% fully present by design. Raw presence trend (before Silver filter) must be taken from Silver/Bronze to assess pandemic impact on attendance.
- If raw presence dropped in 2020–2021 and recovered 2022–2024, treat 2020–2021 as a structural break for comparability.

**Income gap trend**
- Gap = mean(media_objetiva) for highest income (Q006 = Q) minus lowest (Q006 = A), by year.
- Widening gap → inequality in measured performance increased; narrowing → convergence. State direction and approximate magnitude (points) from Gold.

**Structural break**
- **Did the pandemic cause a break?** Yes if: raw presence or valid N drops in 2020–2021 and/or mean/gap shifts in 2020–2021 vs 2022–2024.
- **Did recovery happen?** If means and presence (from Silver) return toward 2019-like levels by 2022–2024, label 2022–2024 as recovery.
- **Is inequality widening or shrinking?** Answer from income-gap trend (and, if available, UF or gender gaps) over 2020–2024.

*All metrics above are computable from Gold (and Silver for presence); no speculation.*

---

## Slide 2 — Key Structural Breaks

**Year with biggest deviation**
- Compare year-level mean (or median) objective score and, if available, redação mean. Identify the year that deviates most from the 2020–2024 average or from the trend line.
- State the direction (e.g. “2021 lowest mean”) and that it is a statistical signal when the deviation exceeds typical year-to-year variation (e.g. from distribuicoes_notas or kpis_uf_ano).

**Subject with strongest shift**
- From Gold: compare 2020 vs 2024 mean by area (cn, ch, lc, mt) or use kpis/distribuicoes by year. The subject with largest |delta| (2024 − 2020) has the strongest shift.
- State the subject and direction (e.g. “Math showed largest gain”).

**Region with strongest improvement / decline**
- From Gold + dim_geografia: top 10 UFs by delta (2024 − 2020) in mean score = strongest improvement; bottom 10 = strongest decline (or least improvement).
- Name the UFs and the approximate deltas; avoid inferring causality without further evidence.

**Statistical signal vs noise**
- **Signal:** A trend or break that is consistent across metrics (e.g. gap widens in 3 of 5 years; one year’s mean is >2x the typical year-to-year change).
- **Noise:** Single-year blips or changes within the range of year-to-year variation. Clearly separate “observed change” from “interpreted as structural” and tie the latter to magnitude and consistency.

---

## Slide 3 — Decision Implications

**3 strategic insights**
1. **Level:** Mean objective score and redação (and % ≥800) over 2020–2024 — whether the system moved up, down or flat (state the metric and direction).
2. **Equity:** Income gap (Q−A) and, if used, gender or regional gaps — widening or narrowing (state direction and that it is from Gold).
3. **Geography:** Which UFs gained most and which gained least (or declined) in mean score 2020→2024 — from Gold top/bottom delta list.

**3 policy/market actions**
1. **Target support:** Prioritise reinforcement (e.g. math or redação) in UFs or profiles that lag or show the largest gap (e.g. low Q006, low-growth UF).
2. **Replicate and scale:** Document and spread practices from top-growth UFs or from periods when the income gap narrowed.
3. **Monitor and report:** Publish presence (from Silver), mean, and gap by year and segment so that “recovery” and “inequality” are measurable and auditable from the same Gold/Silver layer.

**1 “if nothing changes” scenario**
- If current trends (mean, gap, regional deltas) continue unchanged: state the implied outcome in one sentence (e.g. “Income gap will remain at X points; bottom UFs will stay Y points below top”), and note that this is a simple extrapolation from 2020–2024 Gold metrics, not a forecast.

**Gold layer reliability**
- All insights and actions above use Gold (fato_desempenho, kpis_uf_ano, distribuicoes_notas, dims). Gold is built only from Silver (valid cohort, canonical schema), so trends and gaps are comparable across years and suitable for strategic and policy use.

---

*End of Evaluation Slide Summary*
