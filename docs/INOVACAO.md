# Inovação — ENEM Opportunity & Equity Radar (2020–2024)

**Pitch do produto, fórmula do Radar de Prioridade, planos de fidelidade e casos de uso.**

---

## 1. Product pitch: “ENEM Opportunity & Equity Radar”

**Uma frase:** Plataforma de decisão que transforma microdados ENEM (2020–2024) em KPIs comparáveis por UF, perfil e área, com perfis (clusters), modelos preditivos e assistente analítico ancorado em evidência — para EdTechs, secretarias, universidades e formuladores de política.

**Proposta de valor:** Uma única base harmonizada (Bronze → Silver → Gold), reprodutível e auditável, que responde a “onde melhorou ou piorou?”, “quais perfis têm maior risco?” e “como evoluiu a excelência (ex.: redação ≥ 800)?” com números reais, sem especulação.

---

## 2. Radar de Prioridade: fórmula e explicação

**Objetivo:** Um score por UF (ou perfil) que oriente onde priorizar esforços — combina oportunidade (crescimento, tamanho) e equidade (redução de gap, presença).

**Fórmula (conceitual):**

```
Radar_de_Prioridade = w1 · Crescimento_normalizado
                    + w2 · Tamanho_coorte_normalizado
                    + w3 · (1 − Gap_normalizado)
                    + w4 · Presença_normalizada
```

- **Crescimento_normalizado:** Variação da média (ex.: objetiva ou redação) entre dois anos (ex.: 2021 → 2024), em escala 0–1 (ex.: min-max por UF).
- **Tamanho_coorte_normalizado:** Participantes no território/perfil, normalizado (ex.: 0–1) para não priorizar só UFs grandes.
- **Gap_normalizado:** Distância entre extremos (ex.: renda Q vs A, ou UF com menor vs maior média); (1 − Gap) para que menor gap aumente o score.
- **Presença_normalizada:** % de participantes com presença completa (ou indicador de risco de ausência invertido), em 0–1.

**Explicação:** O score não é “quem tem média maior”, e sim “onde há crescimento, massa relevante, menor desigualdade e melhor presença”. Pesos (w1…w4) são configuráveis por uso (ex.: mais peso em equidade para secretaria; mais em crescimento para EdTech).

---

## 3. Plano de fidelidade (tiers Bronze / Silver / Gold)

| Tier | Nome | O que o cliente acessa | Como o score mapeia |
|------|------|------------------------|----------------------|
| **Bronze** | Base | Acesso a relatórios agregados (ex.: KPIs por UF/ano); dashboards públicos ou resumos. Sem microdados. | Score Radar disponível apenas em nível agregado (ex.: top 10 UFs por Radar); sem drill-down por perfil. |
| **Silver** | Analítico | Acesso a tabelas Silver agregadas ou a amostras anonimizadas; quality_report e null_report para transparência. | Score Radar + segmentação por perfil (sexo, faixa de renda) e por ano; relatórios de evolução. |
| **Gold** | Decisão | Acesso a Gold completo (star schema, KPIs, cluster_profiles, cluster_evolution); uso do LLM Analyst e de previsões/embeddings quando contratado. | Score Radar em nível máximo de detalhe; uso de clusters (“Mapa de Perfis”), modelos e assistente para decisão tática e operacional. |

**Mapeamento score → tier:** Clientes que precisam apenas de “onde priorizar em alto nível” ficam no Bronze (score + resumos). Quem precisa de segmentação e auditoria sobe para Silver. Quem toma decisão com base em perfis, evolução e assistente usa o tier Gold.

---

## 4. Três casos de uso concretos

1. **EdTech (plataforma de preparação)**  
   Usa Radar de Prioridade e crescimento por UF/área para priorizar expansão comercial e conteúdo (ex.: reforço em matemática nas UFs com maior crescimento de participantes e maior gap na área). Tier Silver ou Gold; clusters para segmentar oferta (ex.: perfil “alto crescimento, média baixa em MT” = foco em MT).

2. **Secretaria estadual/municipal**  
   Usa KPIs por UF e por perfil para alocação de recursos e programas de reforço. Foco em equidade: gap por renda e por região; presença e risco de ausência. Tier Silver (relatórios e qualidade) ou Gold (clusters e evolução por UF/ano). Radar ajuda a priorizar regiões e perfis com maior necessidade e massa crítica.

3. **Universidade privada**  
   Usa tendência de notas e % redação ≥ 800 por UF para definir onde recrutar e como calibrar cortes e bolsas. Clusters e evolução por UF/ano (Gold) permitem segmentar candidatos e desenhar ações por perfil. Tier Gold para acesso ao Mapa de Perfis e ao assistente analítico em cima de Gold.

---

## 5. Slide version

- **Pitch:** ENEM Opportunity & Equity Radar — decisão baseada em dados ENEM 2020–2024; lakehouse + ML + clusters + LLM; evidência, não especulação.
- **Radar:** Fórmula = w1·Crescimento + w2·Tamanho + w3·(1−Gap) + w4·Presença; normalizado 0–1; pesos configuráveis.
- **Tiers:** Bronze (resumos + score agregado) | Silver (segmentação + qualidade) | Gold (decisão + clusters + LLM).
- **Casos de uso:** EdTech (priorização e conteúdo); Secretaria (equidade e recursos); Universidade (recrutamento e bolsas).
