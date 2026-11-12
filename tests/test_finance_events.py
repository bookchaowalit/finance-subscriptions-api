"""SPEC-005: finance events through the outbox to Kafka (the same file runs in all ten finance services)."""

from __future__ import annotations

import json
import re
import uuid
from pathlib import Path
from typing import Any

import psycopg
import pytest
from cellkit.contract import (
    make,
    pytest_generate_tests,  # noqa: F401 - parametrizes `resource`
)
from cellkit.spec import load

ROOT = Path(__file__).resolve().parents[1]
TYPE = re.compile(r"^[a-z0-9_-]+(\.[a-z0-9_-]+){1,3}$")
ENVELOPE_KEYS = {
    "id",
    "type",
    "version",
    "occurred_at",
    "producer",
    "correlation_id",
    "causation_id",
    "data",
}


class FakePublisher:
    def __init__(self, fail: int = 0) -> None:
        self.sent: list[tuple[str, str, dict[str, Any]]] = []
        self.fail = fail

    def publish(self, topic: str, key: str, value: dict[str, Any]) -> None:
        if self.fail:
            self.fail -= 1
            raise RuntimeError("broker down")
        self.sent.append((topic, key, value))


def cell():
    return load(ROOT / "cell.yaml")


def _reference(f) -> bool:
    return (
        f.type in ("enum", "ref", "bool", "date")
        or (f.type in ("bigint", "int") and f.name.endswith("_id"))
        or f.name in ("scope", "currency")
    )


# --- AC-02: what an event may carry ---------------------------------------------------------------


def test_every_resource_emits_events():
    assert [r.name for r in cell().resources if r.graph is None] == []


def test_events_carry_reference_data_only():
    loose = [
        f"{r.name}.{name}"
        for r in cell().resources
        for name in r.graph or ()
        if not _reference(r.field(name))
    ]
    assert loose == []


# --- AC-01: one outbox row per write, in the write's transaction --------------------------------


def _outbox(harness, subject: str) -> list[tuple]:
    return harness.superuser(
        f"SELECT type, payload FROM {harness.cell.schema}.outbox WHERE subject = %s ORDER BY occurred_at, id",
        (subject,),
    )


def _patchable(r):
    for f in r.fields:
        if f.type in ("text", "enum") and f.name not in {
            a for pair in r.ranges for a in pair
        }:
            return f
    return None


def test_writes_leave_one_outbox_row_each(client, owner, harness, resource):
    cell_, r = harness.cell, resource
    record = make(client, owner, cell_, r)
    subject = f"empire:{cell_.name}:{r.name}:{record['id']}"
    path = f"/v1/{r.plural}/{record['id']}"
    verbs = ["created"]
    f = _patchable(r)
    if f is not None:
        # A no-op patch writes no event, so send a value that differs from the record's.
        new = next(v for v in (*f.values, "x", "y") if v != record.get(f.name))
        if f.type == "text":
            new = (str(record.get(f.name) or "") + "-changed")[-(f.max_length or 200) :]
        changed = client.patch(path, headers=owner, json={f.name: new})
        assert changed.status_code == 200, changed.text
        verbs.append("updated")
    assert client.delete(path, headers=owner).status_code == 204
    verbs.append("deleted")
    rows = _outbox(harness, subject)
    assert [t for t, _ in rows] == [f"{cell_.name}.{r.name}.{v}" for v in verbs]
    payload = rows[0][1]
    assert set(payload) == {*r.graph, "version", "deleted"}
    assert rows[-1][1]["deleted"] is True


def test_a_refused_write_leaves_no_outbox_row(client, owner, harness, resource):
    before = harness.superuser(f"SELECT count(*) FROM {harness.cell.schema}.outbox")[0][
        0
    ]
    refused = client.post(
        f"/v1/{resource.plural}", json={"no_such_field": 1}, headers=owner
    )
    assert refused.status_code == 422
    after = harness.superuser(f"SELECT count(*) FROM {harness.cell.schema}.outbox")[0][
        0
    ]
    assert after == before


# --- AC-03..AC-05, AC-08: the relay as the least-privilege publisher role ----------------------


def _publisher_conn(harness) -> psycopg.Connection:
    return harness.cluster.connect(harness.cell.role("publisher"))


def _pending(harness) -> int:
    return harness.superuser(
        f"SELECT count(*) FROM {harness.cell.schema}.outbox WHERE relayed_at IS NULL"
    )[0][0]


def _drain(harness) -> None:
    from cellkit.relay import relay_once

    with _publisher_conn(harness) as conn:
        while relay_once(conn, harness.cell, FakePublisher()).sent:
            pass


def test_relay_publishes_envelopes_and_marks_them(client, owner, harness):
    from cellkit.relay import TOPIC, relay_once

    _drain(harness)
    r = harness.cell.resources[0]
    record = make(client, owner, harness.cell, r)
    fake = FakePublisher()
    with _publisher_conn(harness) as conn:
        result = relay_once(conn, harness.cell, fake)
        again = relay_once(conn, harness.cell, FakePublisher())
    assert (result.sent, result.failed, again.sent) == (1, 0, 0)
    topic, key, envelope = fake.sent[0]
    assert topic == TOPIC == "finance.events.v1"
    assert key == f"empire:{harness.cell.name}:{r.name}:{record['id']}"
    assert set(envelope) == ENVELOPE_KEYS
    assert TYPE.fullmatch(envelope["type"])
    assert envelope["type"] == f"{harness.cell.name}.{r.name}.created"
    assert envelope["version"] == 1
    assert envelope["producer"] == harness.cell.name
    assert uuid.UUID(envelope["id"])
    data = envelope["data"]
    assert data["record_id"] == record["id"]
    assert data["subject"] == key
    assert uuid.UUID(data["workspace_id"])
    assert data["deleted"] is False
    assert set(data) == {
        "workspace_id",
        "record_id",
        "subject",
        "record_version",
        "event_seq",
        "deleted",
        *r.graph,
    }
    json.dumps(envelope)  # JSON-safe as sent


def test_a_failed_publish_backs_off_and_keeps_the_record_order(client, owner, harness):
    from cellkit.relay import relay_once

    _drain(harness)
    r = harness.cell.resources[0]
    record = make(client, owner, harness.cell, r)
    assert (
        client.delete(f"/v1/{r.plural}/{record['id']}", headers=owner).status_code
        == 204
    )
    with _publisher_conn(harness) as conn:
        result = relay_once(conn, harness.cell, FakePublisher(fail=1))
    assert (result.sent, result.failed) == (0, 1)
    rows = harness.superuser(
        f"SELECT type, relay_attempts, relay_error, relay_next_at > now() FROM {harness.cell.schema}.outbox"
        " WHERE subject = %s ORDER BY occurred_at, id",
        (f"empire:{harness.cell.name}:{r.name}:{record['id']}",),
    )
    assert rows[0][1:] == (1, "RuntimeError", True)
    assert rows[1][1:] == (0, None, None)  # waits behind the failed one
    harness.superuser(
        f"UPDATE {harness.cell.schema}.outbox SET relay_next_at = now() WHERE relay_error IS NOT NULL RETURNING 1"
    )
    fake = FakePublisher()
    with _publisher_conn(harness) as conn:
        assert relay_once(conn, harness.cell, fake).sent == 2
    assert [v["type"].rsplit(".", 1)[1] for _, _, v in fake.sent] == [
        "created",
        "deleted",
    ]


def test_only_one_relay_works_at_a_time(harness):
    from cellkit.relay import relay_once

    with _publisher_conn(harness) as holder, _publisher_conn(harness) as other:
        holder.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (f"finance-relay:{harness.cell.schema}",),
        )
        assert relay_once(other, harness.cell, FakePublisher()).skipped is True
        holder.rollback()


def test_the_hourly_relay_waits_for_an_inline_relay_instead_of_skipping(harness):
    """An inline relay holds the lock for seconds; the sweep must not lose its hour to it."""
    import threading

    from cellkit.relay import relay_once

    with _publisher_conn(harness) as holder, _publisher_conn(harness) as job:
        holder.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (f"finance-relay:{harness.cell.schema}",),
        )
        threading.Timer(0.3, holder.rollback).start()
        assert relay_once(job, harness.cell, FakePublisher(), wait=5).skipped is False


def test_the_relay_role_cannot_read_business_rows(harness):
    r = harness.cell.resources[0]
    with (
        _publisher_conn(harness) as conn,
        pytest.raises(psycopg.errors.InsufficientPrivilege),
    ):
        conn.execute(f"SELECT * FROM {harness.cell.schema}.{r.table}")


def test_oldest_unsent_seconds(client, owner, harness):
    from cellkit.relay import oldest_unsent_seconds

    _drain(harness)
    with _publisher_conn(harness) as conn:
        assert oldest_unsent_seconds(conn, harness.cell) == 0
    make(client, owner, harness.cell, harness.cell.resources[0])
    with _publisher_conn(harness) as conn:
        assert oldest_unsent_seconds(conn, harness.cell) >= 0
    assert _pending(harness) >= 1


# --- AC-06: the API relays its own rows after a write -------------------------------------------


def test_the_api_relays_after_a_write_when_kafka_is_configured(
    client, owner, harness, monkeypatch
):
    from cellkit import relay

    monkeypatch.setattr(relay, "configured", lambda _cell: True)
    _drain(harness)
    fake = FakePublisher()
    monkeypatch.setattr(relay, "publisher_from_env", lambda _cell: fake)
    record = make(client, owner, harness.cell, harness.cell.resources[0])
    assert [v["data"]["record_id"] for _, _, v in fake.sent] == [record["id"]]
    assert _pending(harness) == 0


def test_a_broken_broker_never_fails_the_write(client, owner, harness, monkeypatch):
    from cellkit import relay

    monkeypatch.setattr(relay, "configured", lambda _cell: True)
    monkeypatch.setattr(
        relay, "publisher_from_env", lambda _cell: FakePublisher(fail=99)
    )
    record = make(client, owner, harness.cell, harness.cell.resources[0])
    assert record["id"] > 0  # make() asserted the 201


def test_a_broken_producer_setup_never_fails_the_write(
    client, owner, harness, monkeypatch
):
    from cellkit import relay

    monkeypatch.setattr(relay, "configured", lambda _cell: True)

    def broken(_cell):
        raise RuntimeError("bad Kafka settings")

    monkeypatch.setattr(relay, "publisher_from_env", broken)
    record = make(client, owner, harness.cell, harness.cell.resources[0])
    assert record["id"] > 0


def _insert_event(harness, ws, subject, version, ago, verb="updated"):
    r = harness.cell.resources[0]
    harness.superuser(
        f"INSERT INTO {harness.cell.schema}.outbox (workspace_id, type, subject, payload, occurred_at)"
        f" VALUES (%s, %s, %s, %s, now() - interval '{ago}') RETURNING 1",
        (
            str(ws),
            f"{harness.cell.name}.{r.name}.{verb}",
            subject,
            json.dumps({"version": version, "deleted": verb == "deleted"}),
        ),
    )


def _sent_versions(fake, subject):
    return [
        (v["type"].rsplit(".", 1)[1], v["data"]["record_version"])
        for _, key, v in fake.sent
        if key == subject
    ]


def test_a_record_is_relayed_in_write_order(harness):
    """occurred_at is the transaction's start; the row lock makes the insert order the commit order."""
    from cellkit.relay import relay_once

    _drain(harness)
    r = harness.cell.resources[0]
    ws, subject = uuid.uuid4(), f"empire:{harness.cell.name}:{r.name}:999999"
    _insert_event(harness, ws, subject, 1, "1 minute")
    _insert_event(
        harness, ws, subject, 2, "2 minutes"
    )  # started earlier, committed later
    fake = FakePublisher()
    with _publisher_conn(harness) as conn:
        relay_once(conn, harness.cell, fake)
    assert _sent_versions(fake, subject) == [("updated", 1), ("updated", 2)]


def test_a_recreated_record_keeps_its_order(harness):
    """An import may re-create a deleted legacy id; its version starts again at 1."""
    from cellkit.relay import relay_once

    _drain(harness)
    r = harness.cell.resources[0]
    ws, subject = uuid.uuid4(), f"empire:{harness.cell.name}:{r.name}:999998"
    _insert_event(harness, ws, subject, 1, "3 minutes", "created")
    _insert_event(harness, ws, subject, 2, "2 minutes", "deleted")
    _insert_event(harness, ws, subject, 1, "1 minute", "created")
    fake = FakePublisher()
    with _publisher_conn(harness) as conn:
        relay_once(conn, harness.cell, fake)
    assert _sent_versions(fake, subject) == [
        ("created", 1),
        ("deleted", 2),
        ("created", 1),
    ]


def test_one_workspaces_failing_event_never_blocks_another(harness):
    """Legacy imports keep their old ids, so two workspaces can share a subject."""
    from cellkit.relay import relay_once

    _drain(harness)
    r = harness.cell.resources[0]
    subject = f"empire:{harness.cell.name}:{r.name}:999997"
    a, b = uuid.uuid4(), uuid.uuid4()
    _insert_event(harness, a, subject, 1, "2 minutes")
    _insert_event(harness, b, subject, 1, "1 minute")
    with _publisher_conn(harness) as conn:
        first = relay_once(conn, harness.cell, FakePublisher(fail=1))
        fake = FakePublisher()
        second = relay_once(conn, harness.cell, fake)
    assert (first.sent, first.failed) == (1, 1)
    assert (second.sent, second.failed) == (
        0,
        0,
    )  # a waits out its backoff; b already went


def test_the_job_moves_past_a_batch_that_failed_entirely(harness, monkeypatch, capsys):
    from cellkit import relay

    _drain(harness)
    r = harness.cell.resources[0]
    for n in (1, 2):
        _insert_event(
            harness,
            uuid.uuid4(),
            f"empire:{harness.cell.name}:{r.name}:99990{n}",
            1,
            f"{n} minutes",
        )
    monkeypatch.setattr(relay, "BATCH_SIZE", 1)
    monkeypatch.setenv(
        f"{harness.cell.env}_RELAY_DATABASE_URL",
        f"host={harness.cluster.socket_dir} port={getattr(harness.cluster, 'port', 5434)}"
        f" user={harness.cell.role('publisher')} dbname={harness.cell.schema}",
    )
    monkeypatch.setattr(
        relay, "publisher_from_env", lambda _cell: FakePublisher(fail=1)
    )
    monkeypatch.setattr(relay, "load", lambda _path: harness.cell)
    relay.main([])
    printed = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert (printed["sent"], printed["failed"]) == (1, 1)


def test_without_kafka_settings_nothing_is_published(monkeypatch):
    from cellkit import relay

    for name in list(relay.os.environ):
        if "KAFKA" in name:
            monkeypatch.delenv(name)
    assert relay.publisher_from_env(cell()) is None


# --- AC-07: the job prints counts only -----------------------------------------------------------


def test_the_relay_job_prints_counts(harness, monkeypatch, capsys):
    from cellkit import relay

    names = harness.cell
    monkeypatch.setenv(
        f"{names.env}_RELAY_DATABASE_URL",
        f"host={harness.cluster.socket_dir} port={getattr(harness.cluster, 'port', 5434)}"
        f" user={names.role('publisher')} dbname={names.schema}",
    )
    monkeypatch.setattr(relay, "publisher_from_env", lambda _cell: FakePublisher())
    monkeypatch.setattr(relay, "load", lambda _path: harness.cell)
    assert relay.main([]) == 0
    printed = json.loads(capsys.readouterr().out)
    assert set(printed) == {
        "sent",
        "failed",
        "skipped",
        "oldest_unsent_seconds",
        "refs_read",
    }
