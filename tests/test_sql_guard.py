"""Structural Text-to-SQL validation (sqlglot) — no Spark needed."""
from __future__ import annotations

import pytest

from maritimeiq.governance.sql_guard import validate_select

ALLOWED = {"gold_kpi_customs_holds", "gold_dim_port"}


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM csv.`/etc/passwd`",
        "SELECT * FROM delta.`/tmp/bronze/customers`",
        "SELECT reflect('java.lang.Runtime', 'getRuntime')",
        "SELECT java_method('java.lang.System', 'exit', 0)",
        "INSERT INTO gold_dim_port SELECT * FROM gold_dim_port",
        "SELECT 1; DROP TABLE x",
        "CACHE TABLE gold_dim_port",
        "SET spark.sql.ansi.enabled=false",
        "SELECT * FROM read_files('/etc')",
        "SELECT * FROM gold_secret_table",
        "SELECT * FROM gold_dim_port WHERE master_port IN (SELECT x FROM bronze_customers)",
    ],
)
def test_blocked(sql: str) -> None:
    assert not validate_select(sql, ALLOWED).ok


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT master_port, containers_on_hold FROM gold_kpi_customs_holds ORDER BY 2 DESC LIMIT 10",
        "WITH t AS (SELECT * FROM gold_kpi_customs_holds) SELECT p.port_name, t.hold_events "
        "FROM t JOIN gold_dim_port p ON t.master_port = p.master_port",
        "SELECT a.master_port FROM gold_dim_port a, gold_kpi_customs_holds b WHERE a.master_port = b.master_port",
        "SELECT 1 UNION ALL SELECT 2",
    ],
)
def test_allowed(sql: str) -> None:
    res = validate_select(sql, ALLOWED)
    assert res.ok, res.reason
