# Gold Star Schema — ENEM Opportunity & Equity Radar

**Project:** ENEM Opportunity & Equity Radar (2020–2024)  
**Layer:** Gold (Star Schema)

---

## Diagram

```mermaid
erDiagram
  FATO_DESEMPENHO {
    string nu_inscricao
    int ano
    int id_tempo
    int id_geo
    int id_perfil
    double cn
    double ch
    double lc
    double mt
    double redacao
    double media_objetiva
  }
  DIM_TEMPO {
    int id_tempo
    int ano
    string ciclo_pre_pos_pandemia
  }
  DIM_GEOGRAFIA {
    int id_geo
    string sg_uf_residencia
  }
  DIM_PERFIL {
    int id_perfil
    string tp_sexo
    string faixa_renda
  }
  KPIS_UF_ANO {
    int ano
    string sg_uf_residencia
    long count_participantes
    double media_redacao
    double media_objetiva
    double pct_top800_redacao
  }
  DISTRIBUICOES_NOTAS {
    int ano
    string sg_uf_residencia
    double redacao_p50
    double objetiva_p50
  }
  FATO_DESEMPENHO }o--|| DIM_TEMPO : "id_tempo"
  FATO_DESEMPENHO }o--|| DIM_GEOGRAFIA : "id_geo"
  FATO_DESEMPENHO }o--|| DIM_PERFIL : "id_perfil"
  FATO_DESEMPENHO ..> KPIS_UF_ANO : "aggregated"
  FATO_DESEMPENHO ..> DISTRIBUICOES_NOTAS : "aggregated"
```

---

## Explanation

**Star layout:** The fact table `fato_desempenho` (grain: participante × ano) is at the center and references three dimensions via surrogate keys: `id_tempo`, `id_geo`, `id_perfil`. **Aggregations** (`kpis_uf_ano`, `distribuicoes_notas`) are built from the fact (group by ano + UF); the dotted relations indicate “derived from” for dashboards and analytics, not a direct FK.
