"""SPEC-003: exact money, ISO 4217 currency, workspace-defined scope, health commit.

The same file runs in all ten finance services; it reads this repository's own cell.yaml and migrations.
"""

from __future__ import annotations

import re
import uuid
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest

from cellkit import sqlgen
from cellkit.spec import SpecError, load, parse

ROOT = Path(__file__).resolve().parents[1]
# Fields that are not money and may stay binary floats (measurements, not amounts).
FLOAT_OK = {"engagement_rate", "usage_hours", "time_saved_hours"}


def cell():
    return load(ROOT / "cell.yaml")


def first_migration() -> str:
    return (ROOT / "migrations").glob("0001_*.sql").__next__().read_text()


# --- cell.yaml (AC-01, AC-05, AC-06) -----------------------------------------------------------


def test_no_amount_is_a_binary_float():
    floats = [
        f"{r.name}.{f.name}"
        for r in cell().resources
        for f in r.fields
        if f.type == "real" and f.name not in FLOAT_OK
    ]
    assert floats == []


def test_every_currency_field_is_iso_4217():
    loose = [
        f"{r.name}.{f.name}"
        for r in cell().resources
        for f in r.fields
        if (f.name == "currency" or f.name.endswith("_currency"))
        and f.type != "currency"
    ]
    assert loose == []


def test_owner_scope_became_a_workspace_label():
    resources = cell().resources
    assert all("owner_scope" not in r.columns for r in resources)
    scoped = [r for r in resources if "scope" in r.columns]
    if "owner_scope" in first_migration():
        assert scoped
    for r in scoped:
        scope = r.field("scope")
        assert (scope.type, scope.max_length, scope.default, scope.required) == (
            "text",
            64,
            "",
            False,
        )


# --- spec and SQL (AC-01..AC-05) ---------------------------------------------------------------


def _one(fields):
    return parse({"cell": "demo", "resources": {"item": {"fields": fields}}}).resource(
        "item"
    )


def test_money_decimal_and_currency_columns():
    item = _one(
        {
            "amount": {"type": "money", "min": 0, "required": True},
            "price": {"type": "decimal"},
            "currency": {"type": "currency", "default": "THB"},
            "other_currency": {"type": "currency"},
        }
    )
    c = parse(
        {"cell": "demo", "resources": {"item": {"fields": {"x": {"type": "text"}}}}}
    )
    col = {f.name: " ".join(sqlgen.column(c, f).split()) for f in item.fields}
    assert col["amount"] == "amount numeric(19,4) NOT NULL CHECK (amount >= 0)"
    assert col["price"] == "price numeric(30,10)"
    assert col["currency"] == (
        "currency text NOT NULL DEFAULT 'THB' CHECK (currency ~ '^[A-Z]{3}$')"
    )
    assert col["other_currency"] == (
        "other_currency text CHECK (other_currency IS NULL OR (other_currency ~ '^[A-Z]{3}$'))"
    )


def test_spec_refuses_a_bad_currency_default():
    with pytest.raises(SpecError):
        _one({"currency": {"type": "currency", "default": "thb"}})


# --- the SPEC-003 migration (AC-07) --------------------------------------------------------------------


def _describe(conn: psycopg.Connection, schema: str) -> dict[str, list]:
    columns = conn.execute(
        "SELECT table_name, column_name, data_type, numeric_precision, numeric_scale,"
        " is_nullable, column_default FROM information_schema.columns"
        " WHERE table_schema = %s ORDER BY 1, 2",
        (schema,),
    ).fetchall()
    constraints = conn.execute(
        "SELECT c.conrelid::regclass::text, c.contype, pg_get_constraintdef(c.oid)"
        " FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace"
        " WHERE n.nspname = %s ORDER BY 1, 2, 3",
        (schema,),
    ).fetchall()
    indexes = conn.execute(
        "SELECT tablename, indexdef FROM pg_indexes WHERE schemaname = %s ORDER BY 1, 2",
        (schema,),
    ).fetchall()
    unnamed = sorted((t, re.sub(r"INDEX \S+ ON", "INDEX ON", d)) for t, d in indexes)
    return {"columns": columns, "constraints": constraints, "indexes": unnamed}


def _fresh_db(harness, name: str) -> None:
    with harness.cluster.connect("postgres", "postgres", autocommit=True) as conn:
        conn.execute(f"DROP DATABASE IF EXISTS {name}")
        conn.execute(f"CREATE DATABASE {name}")


def test_saas_migration_equals_a_fresh_schema(harness):
    spec = harness.cell
    _fresh_db(harness, "saas_fresh")
    with harness.cluster.connect("postgres", "saas_fresh") as conn:
        conn.execute(sqlgen.migration(spec))
        conn.commit()
        fresh = _describe(conn, spec.schema)
    # The harness database ran every file in migrations/ (the SPEC-003 one last).
    with harness.cluster.connect("postgres") as conn:
        upgraded = _describe(conn, spec.schema)
    assert upgraded == fresh


def test_saas_migration_keeps_existing_rows(harness):
    spec = harness.cell
    files = sorted((ROOT / "migrations").glob("*.sql"))
    assert files[-1].name.endswith("_saas_money_currency_scope.sql")
    _fresh_db(harness, "saas_upgrade")
    ws = uuid.uuid4()
    with harness.cluster.connect("postgres", "saas_upgrade") as conn:
        for earlier in files[:-1]:
            conn.execute(earlier.read_text())
        inserted = {}
        for r in spec.ordered():
            old = conn.execute(
                "SELECT column_name, data_type, is_nullable, column_default"
                " FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
                (spec.schema, r.table),
            ).fetchall()
            values: dict[str, object] = {"workspace_id": ws}
            for name, kind, nullable, default in old:
                if name in (
                    "workspace_id",
                    "id",
                    "legacy_id",
                    "version",
                    "created_at",
                    "updated_at",
                ):
                    continue
                if name == "owner_scope":
                    values[name] = "venture"
                elif name == "currency" or name.endswith("_currency"):
                    values[name] = " usd"  # the migration trims and upper-cases
                elif kind == "double precision":
                    values[name] = 1.25
                elif nullable == "NO" and default is None:
                    values[name] = _required_value(spec, r, name, kind, inserted)
            cols = ", ".join(values)
            marks = ", ".join(["%s"] * len(values))
            row_id = conn.execute(
                f"INSERT INTO {spec.schema}.{r.table} ({cols}) VALUES ({marks}) RETURNING id",
                tuple(values.values()),
            ).fetchone()[0]
            inserted[r.table] = (row_id, values)
        conn.execute(files[-1].read_text())
        for table, (row_id, values) in inserted.items():
            names = [
                n if n != "owner_scope" else "scope"
                for n in values
                if n != "workspace_id"
            ]
            row = conn.execute(
                f"SELECT {', '.join(names)} FROM {spec.schema}.{table} WHERE id = %s",
                (row_id,),
            ).fetchone()
            for name, got in zip(names, row, strict=True):
                want = values["owner_scope" if name == "scope" else name]
                if want == " usd":
                    assert got == "USD", (table, name)
                elif isinstance(want, float):
                    assert got == Decimal(str(want)), (table, name)
                elif isinstance(want, str) and hasattr(got, "isoformat"):
                    assert got == type(got).fromisoformat(want), (table, name)
                elif isinstance(want, str) and not isinstance(got, str):
                    assert str(got) == want, (table, name)
                else:
                    assert got == want, (table, name)
        conn.rollback()


def _required_value(spec, r, name, kind, inserted):
    f = r.field(name)
    if f.type == "ref":
        return inserted[spec.resource(f.target).table][0]
    if f.type == "enum":
        return f.values[0]
    if kind == "date":
        return "2026-01-01"
    if kind.startswith("timestamp"):
        return "2026-01-01T08:00:00+00:00"
    if kind == "boolean":
        return True
    if kind in ("bigint", "integer"):
        return int(f.min) if f.min is not None else 1
    if kind == "jsonb":
        return "{}"
    return "x"


# --- /health (AC-08) ---------------------------------------------------------------------------


def test_health_reports_the_deployed_commit(client, monkeypatch):
    monkeypatch.setenv("RAILWAY_GIT_COMMIT_SHA", "abc123")
    assert client.get("/health").json() == {"status": "ok", "commit": "abc123"}
