"""SPEC-004 end to end on this cell's own schema: customer tokens from platform-identity, two workspaces.

The real app and a throwaway PostgreSQL with forced row-level security; only the platform is faked.
Positive path: an identity-signed member creates, lists, reads, patches and deletes in its own workspace.
Negative path: a member of another workspace sees and changes nothing, and the rows carry the right workspace.
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient

from cellkit.app import create_app
from cellkit.auth import JwksKeys, TokenVerifier, TrustedIssuer
from cellkit.contract import make, pytest_generate_tests  # noqa: F401  (parametrizes ``resource``)
from cellkit.testing import ISSUER, FakePlatform

IDENTITY = "https://identity.example"

_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
ID_PRIVATE = _key.private_bytes(
    serialization.Encoding.PEM,
    serialization.PrivateFormat.PKCS8,
    serialization.NoEncryption(),
).decode()
ID_JWK = {
    **json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(_key.public_key())),
    "alg": "RS256",
    "use": "sig",
    "kid": "identity-1",
}


def _identity_jwks() -> JwksKeys:
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json={"keys": [ID_JWK]})
        )
    )
    return JwksKeys(f"{IDENTITY}/.well-known/jwks.json", client=client)


def identity_token(
    audience: str, actor: uuid.UUID, workspace: uuid.UUID
) -> dict[str, str]:
    """What platform-identity mints for a member (Finance suite plan, option A: cell + platform audiences)."""
    now = int(time.time())
    claims = {
        "iss": IDENTITY,
        "aud": [audience, "platform-api"],
        "sub": str(actor),
        "ws": str(workspace),
        "iat": now,
        "exp": now + 3600,
        "jti": uuid.uuid4().hex,
    }
    token = jwt.encode(
        claims, ID_PRIVATE, algorithm="RS256", headers={"kid": ID_JWK["kid"]}
    )
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def identity_client(harness, platform) -> TestClient:
    verifier = TokenVerifier(
        harness.public_key,
        ISSUER,
        harness.cell.audience,
        identity=TrustedIssuer(IDENTITY, _identity_jwks()),
    )
    return TestClient(
        create_app(
            harness.cell,
            lambda: harness.cluster.connect(harness.cell.role("api")),
            verifier,
            platform,
        )
    )


def _member(harness, platform: FakePlatform) -> tuple[uuid.UUID, dict[str, str]]:
    actor, workspace = uuid.uuid4(), uuid.uuid4()
    platform.grants[actor] = set(harness.cell.permissions())
    return workspace, identity_token(harness.cell.audience, actor, workspace)


def test_identity_members_work_in_their_own_workspace_only(
    identity_client, harness, platform, resource
):
    cell, r = harness.cell, resource
    ws_a, alice = _member(harness, platform)
    _ws_b, bob = _member(harness, platform)

    # positive path: create, list, read, patch, delete in workspace A
    record: dict[str, Any] = make(identity_client, alice, cell, r)
    listed = identity_client.get(f"/v1/{r.plural}", headers=alice)
    assert listed.status_code == 200
    assert record["id"] in [item["id"] for item in listed.json()["items"]]
    assert (
        identity_client.get(f"/v1/{r.plural}/{record['id']}", headers=alice).status_code
        == 200
    )
    assert (
        identity_client.patch(
            f"/v1/{r.plural}/{record['id']}", headers=alice, json={}
        ).status_code
        == 200
    )
    rows = harness.superuser(
        f"SELECT workspace_id FROM {cell.schema}.{r.table} WHERE id = %s",
        (record["id"],),
    )
    assert rows == [(ws_a,)]

    # negative path: workspace B sees and changes nothing of A
    assert record["id"] not in [
        item["id"]
        for item in identity_client.get(f"/v1/{r.plural}", headers=bob).json()["items"]
    ]
    assert (
        identity_client.get(f"/v1/{r.plural}/{record['id']}", headers=bob).status_code
        == 404
    )
    assert (
        identity_client.patch(
            f"/v1/{r.plural}/{record['id']}", headers=bob, json={}
        ).status_code
        == 404
    )
    assert (
        identity_client.delete(
            f"/v1/{r.plural}/{record['id']}", headers=bob
        ).status_code
        == 404
    )

    # and A can still delete its own record
    assert identity_client.delete(
        f"/v1/{r.plural}/{record['id']}", headers=alice
    ).status_code in (200, 204)


def test_identity_tokens_without_a_grant_are_refused(
    identity_client, harness, resource
):
    token = identity_token(harness.cell.audience, uuid.uuid4(), uuid.uuid4())
    assert (
        identity_client.get(f"/v1/{resource.plural}", headers=token).status_code == 403
    )


def test_main_issuer_tokens_still_work_next_to_identity(
    identity_client, harness, platform, resource
):
    actor = uuid.uuid4()
    platform.grants[actor] = set(harness.cell.permissions())
    response = identity_client.get(
        f"/v1/{resource.plural}", headers=harness.token(actor, uuid.uuid4())
    )
    assert response.status_code == 200
