"""SPEC-002 in the reference reader: report counts, enforce refuses what is not validly signed."""

from __future__ import annotations

import base64
import json
import uuid

import pytest
import test_cross_references as xr
from cellkit import signing
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
)

_ids, _referencing, event = xr._ids, xr._referencing, xr.event
reader = xr.reader  # the shared fixture


def _seed_and_public():
    key = Ed25519PrivateKey.generate()
    seed = base64.b64encode(
        key.private_bytes(Encoding.Raw, PrivateFormat.Raw, NoEncryption())
    ).decode()
    public = base64.b64encode(
        key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    ).decode()
    return seed, public


def _wire(envelope):
    return json.loads(json.dumps(envelope, separators=(",", ":"), default=str))


def _known(harness, ws):
    return {
        row[0]
        for row in harness.superuser(
            f"SELECT record_id FROM {harness.cell.schema}.ref_targets WHERE workspace_id = %s",
            (str(ws),),
        )
    }


@pytest.fixture
def setup(harness, reader, monkeypatch):
    from cellkit import refs

    if not _referencing():
        pytest.skip("this cell points into no other cell")
    _, f = _referencing()[0]
    producer = f.references.split(".")[0]
    seed, public = _seed_and_public()
    _, stranger = _seed_and_public()
    env = harness.cell.env
    monkeypatch.setenv(f"{env}_EVENT_KEYS", json.dumps({producer: [public]}))
    ws = uuid.uuid4()

    def make(signed_with=seed, **data):
        e = event(f.references, "created", ws, next(_ids), status="active", **data)
        return (
            _wire(signing.sign(e, signing.private_key(signed_with)))
            if signed_with
            else _wire(e)
        )

    def run():
        with refs.sync_connection(harness.cell) as conn:
            refs.catch_up(conn, harness.cell, reader)

    other_seed, _ = _seed_and_public()
    return {
        "make": make,
        "run": run,
        "ws": ws,
        "env": env,
        "other": other_seed,
        "stranger": stranger,
    }


def test_enforce_applies_only_validly_signed_events(
    harness, reader, setup, monkeypatch
):
    from cellkit import refs

    monkeypatch.setenv(f"{setup['env']}_EVENT_SIGNATURES", "enforce")
    good = setup["make"]()
    unsigned = setup["make"](signed_with=None)
    forged = setup["make"](signed_with=setup["other"])
    tampered = setup["make"]()
    tampered["data"]["record_id"] += 1_000
    for message in (good, unsigned, forged, tampered):
        reader.add(message)
    refs.SIGNATURES.clear()
    setup["run"]()
    assert _known(harness, setup["ws"]) == {good["data"]["record_id"]}
    assert dict(refs.SIGNATURES) == {"valid": 1, "missing": 1, "invalid": 2}


def test_report_applies_everything_and_counts(harness, reader, setup, monkeypatch):
    from cellkit import refs

    monkeypatch.setenv(f"{setup['env']}_EVENT_SIGNATURES", "report")
    good, unsigned = setup["make"](), setup["make"](signed_with=None)
    for message in (good, unsigned):
        reader.add(message)
    refs.SIGNATURES.clear()
    setup["run"]()
    assert _known(harness, setup["ws"]) == {
        good["data"]["record_id"],
        unsigned["data"]["record_id"],
    }
    assert dict(refs.SIGNATURES) == {"valid": 1, "missing": 1}


def test_off_checks_nothing(harness, reader, setup, monkeypatch):
    from cellkit import refs

    monkeypatch.delenv(f"{setup['env']}_EVENT_SIGNATURES", raising=False)
    reader.add(setup["make"](signed_with=None))
    refs.SIGNATURES.clear()
    setup["run"]()
    assert len(_known(harness, setup["ws"])) == 1
    assert not refs.SIGNATURES


def test_the_relay_signs_when_a_key_is_set_and_reports_counts(harness, monkeypatch):
    from cellkit import relay, signing

    seed, public = _seed_and_public()
    row = {
        "id": uuid.uuid4(),
        "type": f"{harness.cell.name}.x.created",
        "subject": "empire:c:x:5",
        "payload": {"version": 1},
        "occurred_at": __import__("datetime").datetime(2026, 11, 13),
        "correlation_id": None,
        "execution_id": None,
        "workspace_id": uuid.uuid4(),
        "seq": 3,
    }
    plain = relay.envelope(harness.cell, row)
    assert "signature" not in plain
    signed = _wire(signing.maybe_sign(plain, signing.private_key(seed)))
    keys = signing.parse_keys(json.dumps({harness.cell.name: [public]}))
    assert signing.verdict(signed, harness.cell.name, keys) == "valid"


def _job_env(harness, monkeypatch):
    from cellkit import relay

    monkeypatch.setenv(
        f"{harness.cell.env}_RELAY_DATABASE_URL",
        f"host={harness.cluster.socket_dir} port={getattr(harness.cluster, 'port', 5434)}"
        f" user={harness.cell.role('publisher')} dbname={harness.cell.schema}",
    )
    monkeypatch.setattr(relay, "load", lambda _path: harness.cell)


def test_the_relay_job_signs_what_it_sends(harness, monkeypatch, capsys):
    import test_finance_events as fe
    from cellkit import relay

    fe._drain(harness)
    fe._insert_event(
        harness,
        uuid.uuid4(),
        f"empire:{harness.cell.name}:{harness.cell.resources[0].name}:424242",
        1,
        "1 minute",
    )
    seed, public = _seed_and_public()
    monkeypatch.setenv(f"{harness.cell.env}_EVENT_SIGNING_KEY", seed)
    fake = fe.FakePublisher()
    monkeypatch.setattr(relay, "publisher_from_env", lambda _cell: fake)
    _job_env(harness, monkeypatch)
    assert relay.main([]) == 0
    assert seed not in capsys.readouterr().out
    keys = signing.parse_keys(json.dumps({harness.cell.name: [public]}))
    assert fake.sent
    assert {
        signing.verdict(_wire(v), harness.cell.name, keys) for _t, _k, v in fake.sent
    } == {"valid"}


def test_a_bad_signing_key_sends_nothing_and_never_prints_the_value(
    harness, monkeypatch, capsys
):
    import test_finance_events as fe
    from cellkit import relay

    fe._drain(harness)
    fe._insert_event(
        harness,
        uuid.uuid4(),
        f"empire:{harness.cell.name}:{harness.cell.resources[0].name}:434343",
        1,
        "1 minute",
    )
    secret = base64.b64encode(b"too-short").decode()  # decodes, but not 32 bytes
    monkeypatch.setenv(f"{harness.cell.env}_EVENT_SIGNING_KEY", secret)
    fake = fe.FakePublisher()
    monkeypatch.setattr(relay, "publisher_from_env", lambda _cell: fake)
    _job_env(harness, monkeypatch)
    assert relay.main([]) == 2
    out = capsys.readouterr().out
    assert json.loads(out) == {"error": "bad signing key"}
    assert secret not in out
    assert fake.sent == []
    monkeypatch.delenv(f"{harness.cell.env}_EVENT_SIGNING_KEY")
    fe._drain(harness)


def test_the_job_prints_signature_counts_when_checking(
    harness, reader, setup, monkeypatch, capsys
):
    import test_finance_events as fe
    from cellkit import refs, relay

    fe._drain(harness)
    monkeypatch.setenv(f"{setup['env']}_EVENT_SIGNATURES", "report")
    reader.add(setup["make"]())
    refs.SIGNATURES.clear()
    monkeypatch.setattr(relay, "publisher_from_env", lambda _cell: fe.FakePublisher())
    _job_env(harness, monkeypatch)
    assert relay.main([]) == 0
    printed = json.loads(capsys.readouterr().out)
    assert printed["signatures"] == {
        "valid": 1,
        "missing": 0,
        "invalid": 0,
        "unknown_producer": 0,
    }


def test_under_enforce_a_missing_signing_key_sends_nothing(
    harness, monkeypatch, capsys
):
    """A lost secret ("" in GitHub Actions) must not send unsigned events that enforce would drop."""
    import test_finance_events as fe
    from cellkit import relay

    fe._drain(harness)
    fe._insert_event(
        harness,
        uuid.uuid4(),
        f"empire:{harness.cell.name}:{harness.cell.resources[0].name}:454545",
        1,
        "1 minute",
    )
    monkeypatch.setenv(f"{harness.cell.env}_EVENT_SIGNING_KEY", "")
    monkeypatch.setenv(f"{harness.cell.env}_EVENT_SIGNATURES", "enforce")
    _seed, public = _seed_and_public()
    monkeypatch.setenv(f"{harness.cell.env}_EVENT_KEYS", json.dumps({"x": [public]}))
    fake = fe.FakePublisher()
    monkeypatch.setattr(relay, "publisher_from_env", lambda _cell: fake)
    _job_env(harness, monkeypatch)
    assert relay.main([]) == 2
    assert json.loads(capsys.readouterr().out) == {"error": "missing signing key"}
    assert fake.sent == []
    monkeypatch.delenv(f"{harness.cell.env}_EVENT_SIGNATURES")
    fe._drain(harness)


def test_bad_event_keys_under_enforce_are_a_json_error_from_the_job(
    harness, monkeypatch, capsys
):
    import test_finance_events as fe
    from cellkit import relay

    fe._drain(harness)
    seed, _public = _seed_and_public()
    monkeypatch.setenv(f"{harness.cell.env}_EVENT_SIGNING_KEY", seed)
    monkeypatch.setenv(f"{harness.cell.env}_EVENT_SIGNATURES", "enforce")
    monkeypatch.setenv(f"{harness.cell.env}_EVENT_KEYS", "not json")
    monkeypatch.setattr(relay, "publisher_from_env", lambda _cell: fe.FakePublisher())
    from cellkit import refs

    def unavailable(_conn, _cell):
        raise refs.Unavailable("bad event keys")

    monkeypatch.setattr(relay, "_catch_up_references", unavailable)
    _job_env(harness, monkeypatch)
    assert relay.main([]) == 2
    assert json.loads(capsys.readouterr().out) == {"error": "bad event keys"}
