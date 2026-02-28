#!/usr/bin/env bash
# Roda Ollama (llama3.2) + Streamlit. Local: Ollama em localhost, app aqui.
# Na Cloud: em Secrets use OLLAMA_BASE_URL ou DEEPSEEK_API_KEY.

set -e
cd "$(dirname "$0")"
echo "1. Ollama (servidor + modelo llama3.2)..."
ollama serve &
sleep 3
ollama pull llama3.2 2>/dev/null || true
echo "2. Streamlit (app R2 + LLM em localhost:11434)..."
exec streamlit run app/app_s3_duckdb.py
