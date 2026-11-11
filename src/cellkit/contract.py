"""Contract tests every cell runs against its own spec (``from cellkit.contract import *``).

They are parametrized over the cell's resources and need the fixtures of
``cellkit.testing`` (the cell's conftest provides ``cell_root``).
"""

from __future__ import annotations

import json
import random
import time
import uuid
from typing import Any

import httpx
import jwt
import psycopg
import pytest

from cellkit.publisher import publish_pending
from cellkit.spec import LEGACY_LIMIT, Cell, Resource
from cellkit.testing import body, sample

__all__ = [
    "pytest_generate_tests",
    "test_authorization",
    "test_currency_is_iso_4217",
    "test_date_bounds_narrow_the_list",
    "test_erase_empties_the_workspace",
    "test_executions_are_bound_to_the_record",
    "test_idempotent_create",
    "test_import_keeps_the_old_id",
    "test_import_needs_admin",
    "test_inline_publish_failure_is_reported",
    "test_jwks_falls_back_to_the_pinned_key",
    "test_jwks_verification",
    "test_lifecycle",
    "test_missing_reference",
    "test_money_is_exact",
    "test_no_docs_pages",
    "test_no_reference_across_workspaces",
    "test_numbers_fit_their_columns",
    "test_platform_refusal_is_a_denial_not_an_outage",
    "test_platform_refusal_on_decide_is_403",
    "test_publisher_delivers_the_outbox",
    "test_ranges_keep_start_before_end",
    "test_spec_refuses_bad_ranges",
    "test_validation",
    "test_version_conflict",
    "test_workspace_isolation",
]


def pytest_generate_tests(metafunc):
    if "resource" in metafunc.fixturenames:
        import os
        from pathlib import Path

        from cellkit.testing import spec_from

        cell = spec_from(
            Path(os.environ.get("CELLKIT_ROOT") or metafunc.config.rootpath)
        )
        metafunc.parametrize(
            "resource", list(cell.resources), ids=[r.name for r in cell.resources]
        )


def make(
    client,
    headers,
    cell: Cell,
    r: Resource,
    n: int = 0,
    *,
    everything: bool = False,
    **extra: Any,
) -> dict[str, Any]:
    """Create one record of ``r`` and, first, one of every resource its refs point at."""
    refs = {}
    for f in r.refs:
        if (f.required or everything) and f.to != r.name and f.name not in extra:
            refs[f.target] = make(client, headers, cell, cell.resource(f.target), n)[
                "id"
            ]
    payload = {**body(r, refs, n, everything=everything), **extra}
    response = client.post(f"/v1/{r.plural}", json=payload, headers=headers)
    assert response.status_code == 201, response.text
    return response.json()


def test_lifecycle(client, owner, harness, resource):
    cell, r = harness.cell, resource
    record = make(client, owner, cell, r, everything=True)
    assert record["version"] == 1
    assert record["id"] >= LEGACY_LIMIT
    path = f"/v1/{r.plural}/{record['id']}"
    assert client.get(path, headers=owner).json() == record
    for name in r.filters:
        value = record[name]
        if value is None:
            continue
        listed = client.get(
            f"/v1/{r.plural}",
            headers=owner,
            params={name: str(value).lower() if isinstance(value, bool) else value},
        )
        assert listed.status_code == 200, listed.text
        assert record["id"] in [item["id"] for item in listed.json()["items"]]
    by_id = client.get(
        f"/v1/{r.plural}", headers=owner, params={"id": record["id"]}
    ).json()["items"]
    assert [item["id"] for item in by_id] == [record["id"]]
    editable = next((f for f in r.fields if f.type != "ref"), None)
    if editable is not None:
        new_value = sample(editable, 1)
        changed = client.patch(path, headers=owner, json={editable.name: new_value})
        assert changed.status_code == 200, changed.text
        expected_version = (
            1 if changed.json()[editable.name] == record[editable.name] else 2
        )
        assert changed.json()["version"] == expected_version
        again = client.patch(
            path, headers=owner, json={editable.name: changed.json()[editable.name]}
        ).json()
        assert (
            again["version"] == expected_version
        )  # unchanged writes do not bump the version
    if r.filters:
        groups = client.get(
            f"/v1/{r.plural}/stats", headers=owner, params={"group_by": r.filters[0]}
        ).json()
        assert sum(groups["groups"].values()) >= 1
    assert client.delete(path, headers=owner).status_code == 204
    assert client.get(path, headers=owner).status_code == 404
    if r.graph is not None:
        types = harness.superuser(
            f"SELECT type FROM {cell.schema}.outbox WHERE subject = %s ORDER BY occurred_at",
            (f"empire:{cell.name}:{r.name}:{record['id']}",),
        )
        assert types[0][0] == f"{cell.name}.{r.name}.created"
        assert types[-1][0] == f"{cell.name}.{r.name}.deleted"


def test_executions_are_bound_to_the_record(client, owner, harness, platform, resource):
    cell, r = harness.cell, resource
    record = make(client, owner, cell, r)
    client.patch(f"/v1/{r.plural}/{record['id']}", headers=owner, json={})
    client.delete(f"/v1/{r.plural}/{record['id']}", headers=owner)
    bound = [
        (e["permission"], e["bound_to"], e.get("resource_id"))
        for e in platform.executions.values()
        if e.get("resource_id") == str(record["id"])
        and e.get("resource_type") == r.name
    ]
    assert {p.rsplit(":", 1)[1] for p, _, _ in bound} >= {"write", "delete"}
    assert all(
        bound_to == str(record["id"]) == audited for _, bound_to, audited in bound
    )


def test_no_reference_across_workspaces(client, owner, harness, platform, resource):
    cell, r = harness.cell, resource
    refs = [f for f in r.refs if f.to != r.name]
    if not refs:
        pytest.skip("no refs to other resources")
    target = make(client, owner, cell, cell.resource(refs[0].to))
    other_actor = uuid.uuid4()
    platform.grants[other_actor] = set(cell.permissions())
    other = harness.token(other_actor, uuid.uuid4())
    own = {
        f.to: make(client, other, cell, cell.resource(f.to))["id"]
        for f in r.refs
        if f.required
    }
    payload = {**body(r, own), refs[0].name: target["id"]}
    assert (
        client.post(f"/v1/{r.plural}", headers=other, json=payload).status_code == 422
    )


def test_authorization(client, owner, harness, platform, resource):
    cell, r = harness.cell, resource
    record = make(client, owner, cell, r)
    refs = {
        f.to: make(client, owner, cell, cell.resource(f.to))["id"]
        for f in r.refs
        if f.required
    }
    stranger = harness.token(uuid.uuid4(), uuid.uuid4())
    assert client.get(f"/v1/{r.plural}", headers=stranger).status_code == 403
    assert (
        client.get(f"/v1/{r.plural}/{record['id']}", headers=stranger).status_code
        == 404
    )
    assert (
        client.post(f"/v1/{r.plural}", headers=stranger, json=body(r, refs)).status_code
        == 403
    )
    assert client.get(f"/v1/{r.plural}").status_code == 401
    assert (
        client.get(
            f"/v1/{r.plural}", headers={"Authorization": "Bearer nope"}
        ).status_code
        == 401
    )
    wrong_audience = harness.token(uuid.uuid4(), uuid.uuid4(), audience=["other-api"])
    assert client.get(f"/v1/{r.plural}", headers=wrong_audience).status_code == 401
    platform.down = True
    assert client.get(f"/v1/{r.plural}", headers=owner).status_code == 503
    platform.down = False
    reader = uuid.uuid4()
    platform.grants[reader] = {f"{cell.name}.{r.name}:read"}
    read_only = harness.token(reader, uuid.uuid4())
    assert (
        client.patch(
            f"/v1/{r.plural}/{record['id']}", headers=read_only, json={}
        ).status_code
        == 403
    )
    assert (
        client.delete(f"/v1/{r.plural}/{record['id']}", headers=read_only).status_code
        == 403
    )


def test_workspace_isolation(client, owner, harness, platform, resource):
    cell, r = harness.cell, resource
    record = make(client, owner, cell, r)
    other_actor = uuid.uuid4()
    platform.grants[other_actor] = set(cell.permissions())
    other = harness.token(other_actor, uuid.uuid4())
    assert record["id"] not in [
        i["id"] for i in client.get(f"/v1/{r.plural}", headers=other).json()["items"]
    ]
    assert (
        client.get(f"/v1/{r.plural}/{record['id']}", headers=other).status_code == 404
    )
    assert (
        client.patch(
            f"/v1/{r.plural}/{record['id']}", headers=other, json={}
        ).status_code
        == 404
    )
    assert (
        client.delete(f"/v1/{r.plural}/{record['id']}", headers=other).status_code
        == 404
    )


def test_idempotent_create(client, owner, harness, resource):
    cell, r = harness.cell, resource
    refs = {
        f.to: make(client, owner, cell, cell.resource(f.to))["id"]
        for f in r.refs
        if f.required
    }
    payload = body(r, refs)
    key = {"Idempotency-Key": f"k-{uuid.uuid4().hex}"}
    first = client.post(f"/v1/{r.plural}", headers={**owner, **key}, json=payload)
    second = client.post(f"/v1/{r.plural}", headers={**owner, **key}, json=payload)
    assert first.status_code == second.status_code == 201
    assert first.json()["id"] == second.json()["id"]
    rows = harness.superuser(
        f"SELECT count(*) FROM {cell.schema}.{r.table} WHERE id = %s",
        (first.json()["id"],),
    )
    assert rows[0][0] == 1


def test_import_keeps_the_old_id(client, owner, harness, resource):
    cell, r = harness.cell, resource
    legacy = random.randint(1, LEGACY_LIMIT - 1)
    record = make(client, owner, cell, r, legacy_id=legacy)
    assert record["id"] == legacy
    assert record["legacy_id"] == legacy
    assert (
        client.get(f"/v1/{r.plural}/legacy/{legacy}", headers=owner).json()["id"]
        == legacy
    )
    refs = {
        f.to: make(client, owner, cell, cell.resource(f.to))["id"]
        for f in r.refs
        if f.required
    }
    again = client.post(
        f"/v1/{r.plural}", headers=owner, json={**body(r, refs), "legacy_id": legacy}
    )
    assert again.status_code == 409
    assert (
        client.post(
            f"/v1/{r.plural}",
            headers=owner,
            json={**body(r, refs), "legacy_id": LEGACY_LIMIT},
        ).status_code
        == 422
    )


def test_import_needs_admin(client, harness, platform, resource):
    """legacy_id picks the id and may backdate timestamps: :write alone is refused."""
    cell, r = harness.cell, resource
    actor = uuid.uuid4()
    platform.grants[actor] = set(cell.permissions())
    writer = harness.token(actor, uuid.uuid4())
    refs = {
        f.target: make(client, writer, cell, cell.resource(f.target))["id"]
        for f in r.refs
        if f.required and f.to != r.name
    }
    legacy = random.randint(1, LEGACY_LIMIT - 1)
    refused = client.post(
        f"/v1/{r.plural}", headers=writer, json={**body(r, refs), "legacy_id": legacy}
    )
    assert refused.status_code == 403, refused.text
    rows = harness.superuser(
        f"SELECT count(*) FROM {cell.schema}.{r.table} WHERE legacy_id = %s", (legacy,)
    )
    assert rows[0][0] == 0
    # The same body without legacy_id is an ordinary :write.
    assert (
        client.post(f"/v1/{r.plural}", headers=writer, json=body(r, refs)).status_code
        == 201
    )


def test_no_docs_pages(client):
    for path in ("/docs", "/redoc"):
        assert client.get(path).status_code == 404, path
    assert client.get("/openapi.json").status_code == 200


def test_platform_refusal_is_a_denial_not_an_outage():
    from cellkit.platform import Action, Denied, HttpPlatform

    def answer(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json={"detail": {"error": "forbidden"}})

    hosted = HttpPlatform(
        "https://platform.example.com",
        client=httpx.Client(transport=httpx.MockTransport(answer)),
    )
    with pytest.raises(Denied) as refused:
        hosted.decide("t", "api", Action("x.y:read", "y", "*"))
    assert refused.value.status == 403


def test_platform_refusal_on_decide_is_403(client, owner, harness, platform, resource):
    from cellkit.platform import Denied

    def refuse(*_args: Any, **_kwargs: Any) -> bool:
        raise Denied(403, "deny", "platform_refused_action")

    platform.decide = refuse
    response = client.get(f"/v1/{resource.plural}", headers=owner)
    assert response.status_code == 403
    assert response.json()["detail"]["error"] == "deny"


def test_validation(client, owner, harness, resource):
    cell, r = harness.cell, resource
    record = make(client, owner, cell, r)
    assert (
        client.post(
            f"/v1/{r.plural}", headers=owner, json={"no_such_field": 1}
        ).status_code
        == 422
    )
    assert (
        client.get(
            f"/v1/{r.plural}", headers=owner, params={"no_such_filter": "x"}
        ).status_code
        == 422
    )
    assert (
        client.get(f"/v1/{r.plural}", headers=owner, params={"limit": 0}).status_code
        == 422
    )
    for f in r.fields:
        if f.required:
            assert (
                client.patch(
                    f"/v1/{r.plural}/{record['id']}", headers=owner, json={f.name: None}
                ).status_code
                == 422
            )
        if f.type == "enum":
            assert (
                client.patch(
                    f"/v1/{r.plural}/{record['id']}",
                    headers=owner,
                    json={f.name: "not-a-value"},
                ).status_code
                == 422
            )


def test_version_conflict(client, owner, harness, resource):
    cell, r = harness.cell, resource
    record = make(client, owner, cell, r)
    stale = client.patch(
        f"/v1/{r.plural}/{record['id']}", headers=owner, json={"expected_version": 99}
    )
    assert stale.status_code == 409
    assert stale.json()["detail"]["version"] == record["version"]


def test_missing_reference(client, owner, harness, resource):
    cell, r = harness.cell, resource
    if not r.refs:
        pytest.skip("no refs")
    f = r.refs[0]
    refs = {
        g.to: make(client, owner, cell, cell.resource(g.to))["id"]
        for g in r.refs
        if g.required
    }
    payload = {**body(r, refs), f.name: LEGACY_LIMIT * 50 + random.randint(1, 1000)}
    assert (
        client.post(f"/v1/{r.plural}", headers=owner, json=payload).status_code == 422
    )


def test_publisher_delivers_the_outbox(client, owner, harness, platform, resource):
    cell, r = harness.cell, resource
    if r.graph is None:
        pytest.skip("no graph events")
    record = make(client, owner, cell, r)
    token = owner["Authorization"].removeprefix("Bearer ")
    with harness.cluster.connect(cell.role("publisher")) as conn:
        publish_pending(conn, cell.schema, platform, token, limit=500)
    subjects = {e["subject"] for e in platform.published}
    assert f"empire:{cell.name}:{r.name}:{record['id']}" in subjects


def test_inline_publish_failure_is_reported(
    harness, platform, owner, resource, monkeypatch
):
    """A failed inline publish keeps the request successful (the cron sweep retries) but reaches Sentry."""
    from fastapi.testclient import TestClient

    from cellkit import app as app_module
    from cellkit.auth import TokenVerifier
    from cellkit.testing import ISSUER

    cell, r = harness.cell, resource
    if r.graph is None:
        pytest.skip("no graph events")
    captured: list[BaseException] = []
    monkeypatch.setattr(app_module, "capture", captured.append)

    def broken(*_args: Any, **_kwargs: Any) -> dict[str, int]:
        raise RuntimeError("publisher down")

    monkeypatch.setattr(app_module, "publish_pending", broken)
    token = owner["Authorization"].removeprefix("Bearer ")
    client = TestClient(
        app_module.create_app(
            cell,
            lambda: harness.cluster.connect(cell.role("api")),
            TokenVerifier(harness.public_key, ISSUER, cell.audience),
            platform,
            publisher_token=lambda: token,
        )
    )
    make(client, owner, cell, r)
    # One capture per write that publishes (make() also creates the records it refers to).
    assert captured
    assert {type(exc) for exc in captured} == {RuntimeError}


_LOW_HIGH: dict[str, tuple[Any, Any]] = {
    "date": ("2026-01-01", "2026-02-01"),
    "timestamp": ("2026-01-01T08:00:00+00:00", "2026-01-01T09:00:00+00:00"),
    "int": (1, 2),
    "bigint": (1, 2),
    "real": (1.0, 2.0),
    "money": ("1.0000", "2.0000"),
    "decimal": ("1.0000000000", "2.0000000000"),
}


def test_ranges_keep_start_before_end(client, owner, harness, resource):
    cell, r = harness.cell, resource
    if not r.ranges:
        pytest.skip("no ranges")
    refs = {
        f.target: make(client, owner, cell, cell.resource(f.target))["id"]
        for f in r.refs
        if f.required
    }
    for n, (start, end) in enumerate(r.ranges):
        low, high = _LOW_HIGH[r.field(start).type]
        payload = {**body(r, refs, 100 + n), start: high, end: low}
        refused = client.post(f"/v1/{r.plural}", json=payload, headers=owner)
        assert refused.status_code == 422, refused.text
        assert refused.json()["detail"] == {
            "error": "invalid_value",
            "fields": sorted({start, end}),
        }
        record = make(client, owner, cell, r, 200 + n, **{start: low, end: low})
        path = f"/v1/{r.plural}/{record['id']}"
        assert client.patch(path, headers=owner, json={end: high}).status_code == 200
        moved = client.patch(path, headers=owner, json={start: high, end: low})
        assert moved.status_code == 422, moved.text
        assert client.get(path, headers=owner).json()["version"] == 2
        # A row stored before the rule existed may already be inverted: resending its own values
        # (a no-op) is accepted, only a patch that changes a range field is checked.
        harness.superuser(
            f"UPDATE {cell.schema}.{r.table} SET {start} = %s, {end} = %s WHERE id = %s RETURNING id",
            (high, low, record["id"]),
        )
        resent = client.patch(path, headers=owner, json={start: high, end: low})
        assert resent.status_code == 200, resent.text


def test_date_bounds_narrow_the_list(client, owner, harness, resource):
    """SPEC-007: <field>_from / <field>_to on every date and timestamp field, inclusive."""
    cell, r = harness.cell, resource
    if not r.bounds:
        pytest.skip("no date or timestamp fields")
    for n, f in enumerate(x for x in r.fields if x.type in ("date", "timestamp")):
        low, high = _LOW_HIGH[f.type]
        # A field in a range gets the same value as its partner, so the range rule never refuses it.
        partners = {a if b == f.name else b for a, b in r.ranges if f.name in (a, b)}
        early, late = (
            make(
                client,
                owner,
                cell,
                r,
                300 + 10 * n + k,
                **{f.name: value, **dict.fromkeys(partners, value)},
            )["id"]
            for k, value in enumerate((low, high))
        )
        upper = high.replace("+00:00", "Z") if f.type == "timestamp" else high
        for bounds, expected in (
            ({f"{f.name}_from": high}, {late}),
            ({f"{f.name}_to": low}, {early}),
            ({f"{f.name}_from": low, f"{f.name}_to": upper}, {early, late}),
        ):
            listed = client.get(
                f"/v1/{r.plural}",
                headers=owner,
                params={"id": [early, late], **bounds},
            )
            assert listed.status_code == 200, listed.text
            assert {item["id"] for item in listed.json()["items"]} == expected, bounds
        inverted = client.get(
            f"/v1/{r.plural}",
            headers=owner,
            params={f"{f.name}_from": high, f"{f.name}_to": low},
        )
        assert inverted.status_code == 422, inverted.text
        bad = client.get(
            f"/v1/{r.plural}", headers=owner, params={f"{f.name}_from": "not-a-date"}
        )
        assert bad.json()["detail"] == {
            "error": "invalid_value",
            "fields": [f"{f.name}_from"],
        }


def test_erase_empties_the_workspace(
    client, owner, harness, platform, resource, monkeypatch
):
    """SPEC-008: bulk erase of the caller's own records, in batches, keeping references."""
    import hashlib

    cell, r = harness.cell, resource
    erase = f"/v1/{r.plural}/erase"
    ws = jwt.decode(
        owner["Authorization"].split()[1], options={"verify_signature": False}
    )["ws"]
    records = [make(client, owner, cell, r, n) for n in range(3)]
    other_actor = uuid.uuid4()
    platform.grants[other_actor] = set(cell.permissions())
    other = harness.token(other_actor, uuid.uuid4())
    kept = make(client, other, cell, r)
    # AC-02: the caller names their own workspace, nothing else.
    assert client.post(erase, headers=owner, json={}).status_code == 422
    for bad in ({"confirm": "nope"}, {"confirm": str(uuid.uuid4())}):
        response = client.post(erase, headers=owner, json=bad)
        assert response.status_code == 422, response.text
        assert response.json()["detail"] == {
            "error": "invalid_value",
            "fields": ["confirm"],
        }
    assert (
        client.post(erase, headers=owner, json={"confirm": ws, "x": 1}).status_code
        == 422
    )
    for limit in (0, 501):
        assert (
            client.post(
                erase, headers=owner, json={"confirm": ws, "limit": limit}
            ).status_code
            == 422
        )
    # AC-03
    reader = uuid.uuid4()
    platform.grants[reader] = {f"{cell.name}.{r.name}:read"}
    assert (
        client.post(
            erase, headers=harness.token(reader, uuid.UUID(ws)), json={"confirm": ws}
        ).status_code
        == 403
    )
    assert client.post(erase, json={"confirm": ws}).status_code == 401
    # AC-04: a record another record of this cell points at stays until that one goes.
    children = [
        (c, f) for c in cell.resources for f in c.refs if f.target == r.name and c != r
    ]
    child = None
    if children:
        c, f = children[0]
        child = (c, make(client, owner, cell, c, **{f.name: records[0]["id"]}))
    # AC-05: a live record of another cell points at the first record.
    if r.referenced_by:
        source = r.referenced_by[0]
        harness.superuser(
            f"INSERT INTO {cell.schema}.ref_sources (workspace_id, source, source_id, field,"
            " target_resource, target_id) VALUES (%s, %s, 1, %s, %s, %s) RETURNING 1",
            (ws, source.kind, source.field, r.name, records[0]["id"]),
        )
    blocked = 1 if (child or r.referenced_by) else 0
    before = len(platform.executions)
    first = client.post(erase, headers=owner, json={"confirm": ws, "limit": 1})
    assert first.status_code == 200, first.text
    assert first.json()["erased"] == 1
    erased = 1
    while True:
        step = client.post(erase, headers=owner, json={"confirm": ws}).json()
        erased += step["erased"]
        if step["erased"] == 0:
            break
    if blocked:  # a batch with nothing it may erase makes no execution
        count = len(platform.executions)
        assert (
            client.post(erase, headers=owner, json={"confirm": ws}).json()["erased"]
            == 0
        )
        assert len(platform.executions) == count
    left = {
        i["id"] for i in client.get(f"/v1/{r.plural}", headers=owner).json()["items"]
    }
    assert step["remaining"] == len(left) >= blocked
    if child is not None:
        assert records[0]["id"] in left  # AC-04
        c, row = child
        assert (
            client.delete(f"/v1/{c.plural}/{row['id']}", headers=owner).status_code
            == 204
        )
    if r.referenced_by:
        assert records[0]["id"] in left  # AC-05
        harness.superuser(
            f"UPDATE {cell.schema}.ref_sources SET deleted = true WHERE workspace_id = %s"
            " RETURNING 1",
            (ws,),
        )
    while True:
        step = client.post(erase, headers=owner, json={"confirm": ws}).json()
        erased += step["erased"]
        if step["erased"] == 0:
            break
    assert step["remaining"] == 0
    assert client.get(f"/v1/{r.plural}", headers=owner).json()["items"] == []
    assert erased >= len(records)
    # AC-08: the other workspace keeps its record.
    assert client.get(f"/v1/{r.plural}/{kept['id']}", headers=other).status_code == 200
    # AC-07: one execution per batch that erased something, bound to "erase".
    batches = [
        e
        for e in list(platform.executions.values())[before:]
        if e.get("bound_to") == "erase"
    ]
    assert batches
    for e in batches:
        assert e["permission"] == f"{cell.name}.{r.name}:delete"
        assert e["action"] == f"{cell.name}.{r.name}.erased"
        assert (e["resource_type"], e["resource_id"]) == (r.name, "erase")
    first_id = records[1 if blocked else 0]["id"]
    assert (
        batches[0]["before_hash"]
        == hashlib.sha256(
            json.dumps({"ids": [first_id]}, sort_keys=True).encode()
        ).hexdigest()
    )
    # AC-06: every erased record announced its deletion.
    if r.graph is not None:
        for record in records:
            types = harness.superuser(
                f"SELECT type FROM {cell.schema}.outbox WHERE subject = %s ORDER BY occurred_at",
                (f"empire:{cell.name}:{r.name}:{record['id']}",),
            )
            assert types[-1][0] == f"{cell.name}.{r.name}.deleted"
            assert [t for (t,) in types].count(f"{cell.name}.{r.name}.deleted") == 1
    # Nothing left: still 200, no new execution.
    count = len(platform.executions)
    assert client.post(erase, headers=owner, json={"confirm": ws}).json() == {
        "erased": 0,
        "remaining": 0,
    }
    assert len(platform.executions) == count

    # AC-05: when the references cannot be read, erase nothing (fail closed).
    if r.referenced_by:
        from cellkit import refs

        survivor = make(client, owner, cell, r)
        monkeypatch.setattr(refs, "AUTOTRUST", False)
        refused = client.post(erase, headers=owner, json={"confirm": ws})
        assert refused.status_code == 503
        assert refused.json()["detail"] == {"error": "references_unavailable"}
        monkeypatch.setattr(refs, "AUTOTRUST", True)
        path = f"/v1/{r.plural}/{survivor['id']}"
        assert client.get(path, headers=owner).status_code == 200


def test_spec_refuses_bad_ranges():
    from cellkit.spec import SpecError, parse

    fields = {"a": {"type": "date"}, "b": {"type": "date"}, "c": {"type": "text"}}
    for ranges in ([["a"]], [["a", "missing"]], [["a", "c"]], ["ab"]):
        spec = {
            "cell": "demo",
            "resources": {"item": {"fields": fields, "ranges": ranges}},
        }
        with pytest.raises(SpecError):
            parse(spec)
    ok = parse(
        {
            "cell": "demo",
            "resources": {"item": {"fields": fields, "ranges": [["a", "b"]]}},
        }
    )
    assert ok.resource("item").ranges == (("a", "b"),)


def test_numbers_fit_their_columns(client, owner, harness, resource):
    """An integer past its column (int = 32-bit) or a real that is NaN/Infinity is a 422, not a 500 or a silent null."""
    cell, r = harness.cell, resource
    ints = [
        f for f in r.fields if f.type == "int" and (f.max is None or f.max > 2**31 - 1)
    ]
    reals = [f for f in r.fields if f.type == "real"]
    if not ints and not reals:
        pytest.skip("no int or real fields")
    refs = {
        f.target: make(client, owner, cell, cell.resource(f.target))["id"]
        for f in r.refs
        if f.required
    }
    headers = {**owner, "content-type": "application/json"}
    for f in ints:
        payload = {**body(r, refs, 300), f.name: 2**31}
        response = client.post(f"/v1/{r.plural}", json=payload, headers=owner)
        assert response.status_code == 422, (f.name, response.text)
    for f in reals:
        for raw in ("NaN", "Infinity", "1e400"):
            sent = json.dumps({**body(r, refs, 400), f.name: 0.5}).replace(
                "0.5", raw, 1
            )
            response = client.post(f"/v1/{r.plural}", content=sent, headers=headers)
            assert response.status_code == 422, (f.name, raw, response.text)


# Exact decimal values per type: (accepted input, stored and returned text, refused inputs).
# JSON number literals (sent as raw text) that a binary float would round.
_RAW_EXACT = {
    "money": ("12345678901234.5678", "12345678901234.5678"),
    "decimal": ("1234567890.1234567891", "1234567890.1234567891"),
}

_EXACT: dict[str, tuple[list[tuple[Any, str]], list[str]]] = {
    "money": (
        [("1234.5678", "1234.5678"), (0.1, "0.1000"), ("7", "7.0000")],
        ["1.23456", "1" + "0" * 15, "NaN", "Infinity", "abc", ""],
    ),
    "decimal": (
        [("0.1234567891", "0.1234567891"), ("12", "12.0000000000")],
        ["1.12345678901", "1" + "0" * 20, "NaN", "Infinity", "abc"],
    ),
}


def test_money_is_exact(client, owner, harness, resource):
    """SPEC-003 AC-01..AC-04: money and decimal fields store and return exact decimal strings."""
    cell, r = harness.cell, resource
    fields = [f for f in r.fields if f.type in _EXACT]
    if not fields:
        pytest.skip("no money or decimal fields")
    refs = {
        f.target: make(client, owner, cell, cell.resource(f.target))["id"]
        for f in r.refs
        if f.required
    }
    for n, f in enumerate(fields):
        good, bad = _EXACT[f.type]
        for i, (sent, stored) in enumerate(good):
            payload = {**body(r, refs, 500 + 10 * n + i), f.name: sent}
            created = client.post(f"/v1/{r.plural}", json=payload, headers=owner)
            assert created.status_code == 201, (f.name, sent, created.text)
            assert created.json()[f.name] == stored, (f.name, sent)
            path = f"/v1/{r.plural}/{created.json()['id']}"
            assert client.get(path, headers=owner).json()[f.name] == stored
            # Samples repeat across n; remove the record so unique fields cannot collide.
            assert client.delete(path, headers=owner).status_code == 204
        literal, stored = _RAW_EXACT[f.type]
        sent = json.dumps({**body(r, refs, 550 + n), f.name: 0.5}).replace(
            "0.5", literal, 1
        )
        created = client.post(
            f"/v1/{r.plural}",
            content=sent,
            headers={**owner, "content-type": "application/json"},
        )
        assert created.status_code == 201, (f.name, literal, created.text)
        assert created.json()[f.name] == stored, (f.name, literal)
        client.delete(f"/v1/{r.plural}/{created.json()['id']}", headers=owner)
        for raw in [*bad, *(["-1"] if f.min is not None and f.min >= 0 else [])]:
            payload = {**body(r, refs, 600 + n), f.name: raw}
            refused = client.post(f"/v1/{r.plural}", json=payload, headers=owner)
            assert refused.status_code == 422, (f.name, raw, refused.text)


def test_currency_is_iso_4217(client, owner, harness, resource):
    """SPEC-003 AC-05: currency fields take three upper-case letters only, in the API and the database."""
    cell, r = harness.cell, resource
    fields = [f for f in r.fields if f.type == "currency"]
    if not fields:
        pytest.skip("no currency fields")
    refs = {
        f.target: make(client, owner, cell, cell.resource(f.target))["id"]
        for f in r.refs
        if f.required
    }
    for n, f in enumerate(fields):
        for raw in ("thb", "BAHT", "US", "", "U$D"):
            payload = {**body(r, refs, 700 + n), f.name: raw}
            refused = client.post(f"/v1/{r.plural}", json=payload, headers=owner)
            assert refused.status_code == 422, (f.name, raw, refused.text)
        payload = {**body(r, refs, 710 + n), f.name: "USD"}
        created = client.post(f"/v1/{r.plural}", json=payload, headers=owner)
        assert created.status_code == 201, created.text
        assert created.json()[f.name] == "USD"
        with pytest.raises(psycopg.errors.CheckViolation):
            harness.superuser(
                f"UPDATE {cell.schema}.{r.table} SET {f.name} = 'usd' WHERE id = %s RETURNING id",
                (created.json()["id"],),
            )


def _jwks_pair():
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    public = (
        key.public_key()
        .public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )
        .decode()
    )
    jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key()))
    return (
        private,
        public,
        {**jwk, "alg": "RS256", "use": "sig", "kid": uuid.uuid4().hex},
    )


def _jwks_token(private: str, audience: str, kid: str | None) -> str:
    now = int(time.time())
    claims = {
        "iss": "empire-platform",
        "aud": audience,
        "sub": str(uuid.uuid4()),
        "ws": str(uuid.uuid4()),
        "iat": now,
        "exp": now + 300,
        "jti": uuid.uuid4().hex,
    }
    return jwt.encode(
        claims, private, algorithm="RS256", headers={"kid": kid} if kid else None
    )


def _jwks_client(document: dict, calls: list) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return httpx.Response(200, json=document)

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_jwks_verification(harness):
    from cellkit.auth import JwksKeys, TokenVerifier, Unauthenticated

    aud = harness.cell.audience
    private, _public, jwk = _jwks_pair()
    other_private, _, other_jwk = _jwks_pair()
    calls: list = []
    now = [0.0]
    keys = JwksKeys(
        "https://platform.test/.well-known/jwks.json",
        client=_jwks_client({"keys": [jwk]}, calls),
        clock=lambda: now[0],
    )
    only_jwks = TokenVerifier("", "empire-platform", aud, jwks=keys)
    # kid and kid-less tokens verify against the published key; the set is fetched once and cached
    assert (
        only_jwks.verify(
            "Bearer " + _jwks_token(private, aud, jwk["kid"]), None
        ).channel
        == "api"
    )
    assert only_jwks.verify("Bearer " + _jwks_token(private, aud, None), None)
    assert len(calls) == 1
    # an unknown kid refetches at most once a minute and is refused
    with pytest.raises(Unauthenticated):
        only_jwks.verify(
            "Bearer " + _jwks_token(other_private, aud, other_jwk["kid"]), None
        )
    with pytest.raises(Unauthenticated):
        only_jwks.verify(
            "Bearer " + _jwks_token(other_private, aud, other_jwk["kid"]), None
        )
    assert len(calls) == 1
    now[0] = 61
    with pytest.raises(Unauthenticated):
        only_jwks.verify(
            "Bearer " + _jwks_token(other_private, aud, other_jwk["kid"]), None
        )
    assert len(calls) == 2
    # wrong audience is still refused
    with pytest.raises(Unauthenticated):
        only_jwks.verify(
            "Bearer " + _jwks_token(private, "other-api", jwk["kid"]), None
        )


def test_jwks_falls_back_to_the_pinned_key(harness):
    from cellkit.auth import JwksKeys, TokenVerifier, Unauthenticated

    aud = harness.cell.audience
    private, public, jwk = _jwks_pair()

    def down(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("down")

    keys = JwksKeys(
        "https://platform.test/.well-known/jwks.json",
        client=httpx.Client(transport=httpx.MockTransport(down)),
    )
    both = TokenVerifier(public, "empire-platform", aud, jwks=keys)
    assert both.verify("Bearer " + _jwks_token(private, aud, jwk["kid"]), None)
    assert both.verify("Bearer " + _jwks_token(private, aud, None), None)
    with pytest.raises(Unauthenticated):
        TokenVerifier("", "empire-platform", aud, jwks=keys).verify(
            "Bearer " + _jwks_token(private, aud, None), None
        )
