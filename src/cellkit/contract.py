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
import pytest

from cellkit.publisher import publish_pending
from cellkit.spec import LEGACY_LIMIT, Cell, Resource
from cellkit.testing import body, sample

__all__ = [
    "test_lifecycle",
    "test_executions_are_bound_to_the_record",
    "test_no_reference_across_workspaces",
    "test_authorization",
    "test_workspace_isolation",
    "test_idempotent_create",
    "test_import_keeps_the_old_id",
    "test_validation",
    "test_version_conflict",
    "test_missing_reference",
    "test_publisher_delivers_the_outbox",
    "test_jwks_verification",
    "test_jwks_falls_back_to_the_pinned_key",
    "pytest_generate_tests",
]


def pytest_generate_tests(metafunc):
    if "resource" in metafunc.fixturenames:
        from cellkit.testing import spec_from

        import os
        from pathlib import Path

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
            refs[f.to] = make(client, headers, cell, cell.resource(f.to), n)["id"]
    payload = {**body(r, refs, n, everything=everything), **extra}
    response = client.post(f"/v1/{r.plural}", json=payload, headers=headers)
    assert response.status_code == 201, response.text
    return response.json()


def test_lifecycle(client, owner, harness, resource):
    cell, r = harness.cell, resource
    record = make(client, owner, cell, r, everything=True)
    assert record["version"] == 1 and record["id"] >= LEGACY_LIMIT
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
    editable = next((f for f in r.fields if f.type not in ("ref",)), None)
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
        assert (
            types[0][0] == f"{cell.name}.{r.name}.created"
            and types[-1][0] == f"{cell.name}.{r.name}.deleted"
        )


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
    assert record["id"] == legacy and record["legacy_id"] == legacy
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
    assert (
        stale.status_code == 409
        and stale.json()["detail"]["version"] == record["version"]
    )


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
    private, public, jwk = _jwks_pair()
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
