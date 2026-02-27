# Configuração – Microdados ENEM (últimos 5 anos)
# Fonte: https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enem

BASE_URL = "https://download.inep.gov.br/microdados"
ANOS = [2020, 2021, 2022, 2023, 2024]

# Diretórios (relativos à raiz do projeto)
DIR_DATA = "data"
DIR_RAW = "data/raw"
DIR_PROCESSED = "data/processed"
DIR_OUTPUT = "output"

# Padrão de arquivo de participantes nos ZIPs (varia por ano)
# Ex: microdados_enem_2024/DADOS/MICRODADOS_ENEM_2024.csv
PARTICIPANTES_FILE_PATTERN = "MICRODADOS_ENEM_{ano}.csv"
PARTICIPANTES_FOLDER = "DADOS"
