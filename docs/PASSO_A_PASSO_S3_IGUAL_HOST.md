# Passo a passo: deixar o app no S3 igual ao host

Objetivo: o **app_s3_duckdb.py** no Cloud (Streamlit Cloud + R2) rodar **igual** ao que você vê no host local — mesmo que seja com dados mockados no S3.

---

## Resumo em 4 passos

1. **Gerar dados demo** (Parquets) e **subir para o R2**.
2. **Configurar o Streamlit Cloud** (app = `app_s3_duckdb.py`, secrets R2).
3. **Refresh cache** no app depois do deploy.
4. (Opcional) Conferir que o **Main file** do app no Cloud é `app/app_s3_duckdb.py`.

---

## Passo 1 — Gerar e subir dados para o R2

Na **raiz do repositório** (onde está o `README.md`):

### 1.1 Dependências

```bash
pip install boto3 pandas pyarrow
```

### 1.2 Credenciais R2

Crie ou edite **`.streamlit/secrets.toml`** (na raiz) com:

```toml
R2_ACCESS_KEY = "sua_access_key"
R2_SECRET_KEY = "sua_secret_key"
R2_ENDPOINT = "https://<SEU_ACCOUNT_ID>.r2.cloudflarestorage.com"
R2_BUCKET = "nome_do_seu_bucket"
```

- No Cloudflare: **R2** → seu bucket → **Manage R2 API Tokens** → criar token com **Object Read & Write** (e list).
- Substitua `<SEU_ACCOUNT_ID>` pelo ID da conta (ex.: no dashboard da Cloudflare).

### 1.3 Gerar Parquets e enviar para o R2

```bash
python scripts/build_and_upload_demo_r2.py
```

Isso:

- Gera em `tmp/demo_r2_upload/`:
  - **gold/kpis_uf_ano** (2020–2024, 27 UFs, média objetiva/redação, presença, etc.)
  - **gold/cluster_profiles.parquet**
  - **gold/cluster_evolution_uf_ano.parquet**
  - **silver/quality_report** e **silver/null_report**
- Envia tudo para `s3://<bucket>/gold/` e `s3://<bucket>/silver/`.

Se aparecer erro de credenciais, confira o **Passo 2** (secrets) e o endpoint (sem placeholder).

---

## Passo 2 — Configurar o Streamlit Cloud

1. Acesse [share.streamlit.io](https://share.streamlit.io) e abra o app que usa este repositório.
2. **Settings** (ou **Manage app**) do app.
3. **Main file path:**  
   `app/app_s3_duckdb.py`  
   (não use `app_one_page.py` nem `app.py` para ter o mesmo comportamento do host com S3.)
4. **Secrets** (ou **Environment variables**): adicione as mesmas variáveis do R2:

   - `R2_ACCESS_KEY`
   - `R2_SECRET_KEY`
   - `R2_ENDPOINT` (URL completa, ex.: `https://<account_id>.r2.cloudflarestorage.com`)
   - `R2_BUCKET`

   Use os **mesmos valores** do `.streamlit/secrets.toml` que você usou no Passo 1.
5. Salve e faça **Rerun** (ou **Reboot**) do app.

---

## Passo 3 — Refresh no app

Depois que o app subir no Cloud:

1. Abra o app no navegador.
2. Se houver botão **Refresh cache** (ou similar), clique.
3. Recarregue a página (F5).

O app vai ler de novo os Parquets do R2. Você deve ver:

- **D — Overview Nacional:** gráfico fixo (igual ao host, 2020–2024).
- **Restante:** tabelas, Radar, Clusters, LLM Analyst usando os dados que você subiu no Passo 1.

---

## Passo 4 — Conferir Main file

No Streamlit Cloud, em **Settings**:

- **Main file path** deve ser exatamente: **`app/app_s3_duckdb.py`**.

Se estiver como `app_one_page.py` ou `app.py`, o app não usa R2 da mesma forma e pode parecer diferente do host.

---

## O que fica “igual ao host”

- **Gráfico D (Overview Nacional):** no código está fixo com dados 2020–2024 (igual ao host), independente do R2.
- **KPIs, tabelas, Radar, Clusters, LLM:** vêm do R2. Com o upload do Passo 1, há dados para todos os anos e UFs; o comportamento fica alinhado ao host (com dados demo).
- Para **igualar tudo** sem depender de dados reais no R2, basta rodar o Passo 1 uma vez e configurar os Passos 2 e 3. Assim o S3 passa a ter dados suficientes e o app roda igual ao host (inclusive com “crescimento da linha da média” no gráfico D).

---

## Troubleshooting

| Problema | O que fazer |
|----------|-------------|
| “Nenhum dado” / KPIs vazios | Rodar de novo `python scripts/build_and_upload_demo_r2.py`, conferir bucket e prefixos (gold/silver), depois Refresh cache no app. |
| 403 Forbidden no R2 | Token do R2 precisa de **Object Read** (e list). Criar novo token no dashboard e atualizar secrets. |
| Gráfico D com só 1 ou 2 pontos | Garantir que o Main file é `app/app_s3_duckdb.py` (o gráfico D é fixo nesse app). Fazer Rerun no Cloud. |
| LLM / Radar vazios | Confirmar que o upload do Passo 1 terminou sem erro e que `gold/kpis_uf_ano`, `gold/cluster_*` existem no bucket. |

---

## Resumo dos comandos (local)

```bash
# Na raiz do repo
pip install boto3 pandas pyarrow
# Editar .streamlit/secrets.toml com R2_* (ver 1.2)
python scripts/build_and_upload_demo_r2.py
# Depois: configurar Cloud (Passo 2) e Refresh no app (Passo 3)
```

Depois disso, o app no S3 deve rodar **igual ao host**, com dados mock no R2.
