"""
Validação dos microdados ENEM em data/raw.
Confere: estrutura oficial INEP, contagem de linhas, amostra de valores (notas, códigos).
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import ANOS, DIR_RAW

# Mínimo para considerar "dados de participantes" (estrutura INEP)
COLS_MINIMO_PARTICIPANTES = ["NU_INSCRICAO", "TP_SEXO", "TP_FAIXA_ETARIA", "TP_COR_RACA", "Q001", "Q002", "Q006"]
# Estrutura completa (com notas e presença) - anos até 2023
COLS_NOTAS_PRESENCA = [
    "NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO",
    "TP_PRESENCA_CN", "TP_PRESENCA_CH", "TP_PRESENCA_LC", "TP_PRESENCA_MT",
]
SEPARADOR = ";"


def validar_arquivo(ano: int) -> dict:
    """Valida um CSV ENEM e retorna dict com resultado."""
    path = ROOT / DIR_RAW / f"ENEM_{ano}.csv"
    resultado = {"ano": ano, "path": str(path), "ok": False, "erros": [], "avisos": []}

    if not path.exists():
        resultado["erros"].append("Arquivo não encontrado")
        return resultado

    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            header = f.readline().strip()
            linhas_amostra = []
            for i, line in enumerate(f):
                if i >= 5:
                    break
                linhas_amostra.append(line.strip())
    except Exception as e:
        resultado["erros"].append(f"Erro ao ler: {e}")
        return resultado

    cols = header.split(SEPARADOR)
    resultado["num_colunas"] = len(cols)

    # 1) Arquivo de participantes: deve ter ao menos colunas mínimas INEP
    faltando_min = [c for c in COLS_MINIMO_PARTICIPANTES if c not in cols]
    if faltando_min:
        resultado["erros"].append(f"Não é tabela de participantes (faltam: {faltando_min})")
    else:
        resultado["header_inep_ok"] = True
    # Estrutura completa (notas/presença) ou reduzida (ex.: 2024 LGPD)
    faltando_notas = [c for c in COLS_NOTAS_PRESENCA if c not in cols]
    if faltando_notas:
        resultado["estrutura_reduzida"] = True
        resultado["avisos"].append("Estrutura reduzida (sem notas/presença neste arquivo; pode ser divulgação INEP/LGPD).")
    else:
        resultado["estrutura_completa"] = True

    # 2) Verificar se há linhas de dados
    resultado["tem_dados"] = len(linhas_amostra) >= 1
    resultado["amostra_linhas"] = len(linhas_amostra)
    if not resultado["tem_dados"]:
        resultado["avisos"].append("Nenhuma linha de dados (apenas header?)")

    # 3) Amostra de valores
    if linhas_amostra:
        idx_nu_insc = cols.index("NU_INSCRICAO") if "NU_INSCRICAO" in cols else -1
        idx_nota_cn = cols.index("NU_NOTA_CN") if "NU_NOTA_CN" in cols else -1
        idx_sexo = cols.index("TP_SEXO") if "TP_SEXO" in cols else -1
        indices_max = [i for i in (idx_nu_insc, idx_nota_cn, idx_sexo) if i >= 0]
        max_idx = max(indices_max) if indices_max else 0

        inscricoes_ok = 0
        notas_ok = 0
        sexo_ok = 0
        for linha in linhas_amostra:
            vals = linha.split(SEPARADOR)
            if len(vals) <= max_idx:
                continue
            if idx_nu_insc >= 0 and vals[idx_nu_insc].strip().isdigit():
                inscricoes_ok += 1
            if idx_sexo >= 0 and vals[idx_sexo].strip() in ("1", "2"):
                sexo_ok += 1
            if idx_nota_cn >= 0 and len(vals) > idx_nota_cn:
                try:
                    n = float((vals[idx_nota_cn].strip() or "0").replace(",", "."))
                    if 0 <= n <= 1000:
                        notas_ok += 1
                except ValueError:
                    pass
        resultado["amostra_inscricoes_numericas"] = inscricoes_ok
        resultado["amostra_sexo_valido"] = sexo_ok
        resultado["amostra_notas_intervalo"] = notas_ok
        if inscricoes_ok >= 1 and sexo_ok >= 1:
            resultado["valores_consistentes"] = True
        elif not resultado.get("erros"):
            resultado["avisos"].append("Amostra com valores inesperados (verifique)")

    resultado["ok"] = len(resultado["erros"]) == 0
    return resultado


def main():
    from config import DIR_OUTPUT
    (ROOT / DIR_OUTPUT).mkdir(parents=True, exist_ok=True)
    out_lines = []
    def log(s=""):
        out_lines.append(s)
        print(s)

    log("=" * 60)
    log("VALIDAÇÃO DOS MICRODADOS ENEM (data/raw)")
    log("Fonte: INEP - https://www.gov.br/inep/.../microdados/enem")
    log("=" * 60)

    todos_ok = True
    for ano in ANOS:
        r = validar_arquivo(ano)
        log(f"\n[{ano}] {r['path']}")
        if r["erros"]:
            todos_ok = False
            for e in r["erros"]:
                log(f"  ERRO: {e}")
        else:
            log("  Estrutura: OK (colunas oficiais INEP)")
        if r.get("num_colunas"):
            log(f"  Colunas: {r['num_colunas']}")
        if r.get("amostra_linhas"):
            log(f"  Amostra de linhas: {r['amostra_linhas']}")
        if r.get("valores_consistentes"):
            log("  Valores amostrados: consistentes (inscrição numérica, TP_SEXO 1/2)")
        if r.get("estrutura_completa"):
            log("  Estrutura: completa (notas e presença)")
        elif r.get("estrutura_reduzida"):
            log("  Estrutura: reduzida (ex.: divulgação INEP sem notas neste arquivo)")
        for a in r.get("avisos", []):
            log(f"  AVISO: {a}")

    log("\n" + "=" * 60)
    if todos_ok:
        log("RESULTADO: Dados verídicos (estrutura INEP; uso oficial).")
    else:
        log("RESULTADO: Há erros; revise os arquivos listados.")
    log("=" * 60)

    (ROOT / DIR_OUTPUT / "validacao_enem.txt").write_text("\n".join(out_lines), encoding="utf-8")
    log(f"\nRelatório salvo em: {ROOT / DIR_OUTPUT / 'validacao_enem.txt'}")


if __name__ == "__main__":
    main()
