# ENEM Profile Map — Cluster insights

**Source:** `gold.cluster_profiles`, `gold.cluster_evolution_uf_ano`.  
**Tone:** Executive, concise. No speculation without a metric; every insight linked to a measurable delta.

---

## Cluster profiles (interpretative)

Use `cluster_profiles` (columns: `cluster_id`, `size`, `media_redacao`, `media_obj`, `renda_media`, `presence_rate`, `top_3_ufs`) to label and describe each cluster. Below is the intended structure; fill names and numbers from your latest run.

| Cluster | Name (interpretative) | Behavior | Socioeconomic pattern | Performance pattern |
|--------|----------------------|----------|------------------------|----------------------|
| 0 | *From metrics* | *Describe who is in this cluster (e.g. exam-taking behavior, presence)* | *From `renda_media` and `top_3_ufs`: income level and regional concentration* | *From `media_redacao` and `media_obj`: essay vs objective performance* |
| 1 | *From metrics* | *Same* | *Same* | *Same* |
| 2 | *From metrics* | *Same* | *Same* | *Same* |
| 3–7 | *Same rule per cluster* | *Same* | *Same* | *Same* |

**Naming rule:** Derive the label from the numbers (e.g. “High income, high presence” if `renda_media` and `presence_rate` are above sample median; “Moderate math gap” if `media_obj` is below `media_redacao` by a defined threshold).

---

## One actionable insight (example)

**Metric:** Use `cluster_evolution_uf_ano` to compute, per UF, the change in share of participants in each cluster between 2020 and 2024 (`pct_participants` in 2024 minus 2020).

**Example (replace cluster id, UF, and % with your numbers):**

> **Cluster 3 (Low-income, high presence, moderate math gap)** increased its share of participants in **UF X** by **12 percentage points** between 2020 and 2024 (from 8% to 20%). That measurable growth suggests stronger demand for **targeted math reinforcement** in that state, aligned with the cluster’s performance profile.

- **Delta:** 12 p.p. (2024 − 2020) in one UF.  
- **Action:** Prioritize math support where this cluster grew most.  
- **No speculation:** The recommendation is tied to the observed evolution metric and the cluster’s `media_obj` / `media_redacao` profile.

---

## How to refresh this doc

1. Run `cluster_profiles.py` and load `gold.cluster_profiles`.
2. For each `cluster_id`, set **Name** from `renda_media`, `presence_rate`, `media_redacao`, `media_obj`, and **Behavior / Socioeconomic / Performance** from the same columns and `top_3_ufs`.
3. Load `gold.cluster_evolution_uf_ano`, compute per-UF and per-cluster deltas (2024 − 2020), pick the largest delta (or a policy-relevant UF), and write one **actionable insight** with that number and a concrete follow-up (e.g. targeted programs).
