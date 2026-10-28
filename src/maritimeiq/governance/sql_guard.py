"""Structural SQL validation for agent-generated Text-to-SQL (pure Python, no Spark needed)."""
from __future__ import annotations

from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp

# Functions that reach outside the query sandbox (JVM reflection, file reads, session state)
DENIED_FUNCTIONS = {
    "reflect",
    "java_method",
    "try_reflect",
    "read_files",
    "input_file_name",
    "input_file_block_start",
    "input_file_block_length",
    "current_user",
    "session_user",
    "user",
    "assert_true",
    "raise_error",
}

QUERY_ROOTS = (exp.Select, exp.Union, exp.Intersect, exp.Except, exp.Subquery)

# Any of these nodes anywhere in the tree means the statement is not a pure read
WRITE_NODES = tuple(
    getattr(exp, name)
    for name in (
        "Insert",
        "Update",
        "Delete",
        "Merge",
        "Create",
        "Drop",
        "Alter",
        "Command",
        "Set",
        "Cache",
        "Uncache",
        "TruncateTable",
        "LoadData",
        "Use",
        "Describe",
        "Refresh",
    )
    if hasattr(exp, name)
)


@dataclass
class SQLCheck:
    ok: bool
    reason: str = "ok"
    tables: list[str] = field(default_factory=list)


def validate_select(sql: str, allowed_tables: set[str] | None = None) -> SQLCheck:
    """Allow exactly one read-only query over allowlisted views.

    ``allowed_tables`` holds lower-case view names (e.g. ``gold_dim_port``). CTE names are
    always allowed. ``None`` skips the allowlist but keeps the structural checks.
    """
    if not sql or not sql.strip():
        return SQLCheck(False, "Empty SQL")
    try:
        statements = [s for s in sqlglot.parse(sql, read="spark") if s is not None]
    except Exception as exc:  # noqa: BLE001
        return SQLCheck(False, f"SQL could not be parsed: {str(exc)[:160]}")
    if len(statements) != 1:
        return SQLCheck(False, "Exactly one SQL statement is allowed")
    root = statements[0]
    if not isinstance(root, QUERY_ROOTS):
        return SQLCheck(False, f"Only SELECT queries are allowed (got {type(root).__name__.upper()})")
    for node in root.walk():
        if isinstance(node, WRITE_NODES):
            return SQLCheck(False, f"Write/DDL operation not allowed: {type(node).__name__.upper()}")

    for fn in root.find_all(exp.Func):
        name = (fn.name if isinstance(fn, exp.Anonymous) else fn.sql_name()).lower()
        if name in DENIED_FUNCTIONS:
            return SQLCheck(False, f"Function `{name}` is not allowed")

    cte_names = {c.alias_or_name.lower() for c in root.find_all(exp.CTE)}
    tables: list[str] = []
    for t in root.find_all(exp.Table):
        if not isinstance(t.this, exp.Identifier):
            return SQLCheck(False, "Table-valued functions are not allowed")
        if t.args.get("db") or t.args.get("catalog"):
            return SQLCheck(False, f"Qualified/path table references are not allowed: {t.sql(dialect='spark')}")
        name = t.name.lower()
        if name in cte_names:
            continue
        if name.startswith("bronze") or "bronze" in name:
            return SQLCheck(False, "Bronze tables are not queryable")
        if allowed_tables is not None and name not in allowed_tables:
            return SQLCheck(False, f"Table `{name}` is not in the activated layer")
        tables.append(name)
    return SQLCheck(True, "ok", sorted(set(tables)))
