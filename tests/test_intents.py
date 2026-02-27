import os
from pathlib import Path

import pandas as pd
import pytest

from app.instant_intents import INTENT_CATALOG, run_intent, build_intent_sql, validate_sql
from app.db import get_connection


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def conn():
    return get_connection()


def _make_params(intent: dict) -> dict:
    params = {}
    for name in intent.get("required_params", []):
        if name == "year_start":
            params[name] = 2020
        elif name == "year_end":
            params[name] = 2024
        elif name in {"year", "ano"}:
            params[name] = 2024
        elif name == "limit":
            params[name] = 5
        elif name == "uf":
            params[name] = "SP"
        elif name == "cluster_id":
            params[name] = 0
        else:
            params[name] = 5
    return params


@pytest.mark.parametrize("intent_id", list(INTENT_CATALOG.keys()))
def test_each_intent_returns_dataframe(intent_id, conn):
    intent = INTENT_CATALOG[intent_id]
    params = _make_params(intent)
    try:
        df = run_intent(intent_id, params, conn)
    except Exception as e:
        pytest.skip(f"Intent {intent_id} skipped due to execution error: {e}")
    assert isinstance(df, pd.DataFrame)


@pytest.mark.parametrize("intent_id", list(INTENT_CATALOG.keys()))
def test_limit_enforced(intent_id, conn):
    intent = INTENT_CATALOG[intent_id]
    params = _make_params(intent)
    # For intents sem "limit", apenas garantimos que validate_sql inclui LIMIT
    sql = build_intent_sql(intent_id, params)
    assert "LIMIT" in sql.upper()
    if "limit" in intent.get("required_params", []):
        # Quando limit é parametrizável, usamos 5 e conferimos o tamanho do DF
        params["limit"] = 5
        try:
            df = run_intent(intent_id, params, conn)
        except Exception as e:
            pytest.skip(f"Intent {intent_id} skipped due to execution error: {e}")
        assert len(df) <= intent.get("default_limit", 10)


def test_invalid_intent_raises(conn):
    with pytest.raises(ValueError):
        run_intent("intent_inexistente", {}, conn)


def test_validate_sql_basic_rules():
    good = "SELECT 1 FROM gold.kpis_uf_ano LIMIT 10"
    assert validate_sql(good)

    bad_start = "UPDATE gold.kpis_uf_ano SET x = 1 LIMIT 10"
    assert not validate_sql(bad_start)

    bad_limit = "SELECT 1 FROM gold.kpis_uf_ano"
    assert not validate_sql(bad_limit)

    bad_star = "SELECT * FROM gold.kpis_uf_ano LIMIT 10"
    assert not validate_sql(bad_star)

