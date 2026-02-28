# Roda Ollama (llama3.2) + Streamlit. Local: Ollama em localhost, app aqui.
# Na Cloud: em Secrets use OLLAMA_BASE_URL (Ollama exposto) ou DEEPSEEK_API_KEY.

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

Write-Host "1. Ollama (servidor + modelo llama3.2)..." -ForegroundColor Cyan
if (-not (Get-Process -Name "ollama" -ErrorAction SilentlyContinue)) {
    Start-Process -FilePath "ollama" -ArgumentList "serve" -WindowStyle Hidden
    Start-Sleep -Seconds 3
}
ollama pull llama3.2 2>$null; if ($LASTEXITCODE -ne 0) { ollama pull llama3.2 }

Write-Host "2. Streamlit (app R2 + LLM em localhost:11434)..." -ForegroundColor Cyan
streamlit run app/app_s3_duckdb.py
