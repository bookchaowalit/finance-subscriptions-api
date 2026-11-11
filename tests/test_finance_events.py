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
    assert set(data) == {"workspace_id", "record_id", "subject", "deleted", *r.graph}
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


def test_a_record_is_relayed_in_write_order(harness):
    """occurred_at is the transaction's start; a later-starting write can commit first.

    Version 1 takes the row lock (and inserts its event) first although its transaction started
    a minute after the one that writes version 2.
    """
    from cellkit.relay import relay_once

    _drain(harness)
    r = harness.cell.resources[0]
    schema = harness.cell.schema
    ws, subject = str(uuid.uuid4()), f"empire:{harness.cell.name}:{r.name}:999999"
    for version, ago in ((1, "1 minute"), (2, "2 minutes")):
        harness.superuser(
            f"INSERT INTO {schema}.outbox (workspace_id, type, subject, payload, occurred_at)"
            f" VALUES (%s, %s, %s, %s, now() - interval '{ago}') RETURNING 1",
            (
                ws,
                f"{harness.cell.name}.{r.name}.updated",
                subject,
                json.dumps({"version": version, "deleted": False}),
            ),
        )
    fake = FakePublisher()
    with _publisher_conn(harness) as conn:
        relay_once(conn, harness.cell, fake)
    versions = [
        harness.superuser(
            f"SELECT payload->>'version' FROM {schema}.outbox WHERE id = %s",
            (v["id"],),
        )[0][0]
        for _, key, v in fake.sent
        if key == subject
    ]
    assert versions == ["1", "2"]


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
    assert set(printed) == {"sent", "failed", "skipped", "oldest_unsent_seconds"}


# --- Review fixes (2026-11-11): workspaces, re-created ids, the job loop -------------------------


def _insert_event(harness, ws: str, subject: str, version: int, verb: str = "updated"):
    r = harness.cell.resources[0]
    harness.superuser(
        f"INSERT INTO {harness.cell.schema}.outbox (workspace_id, type, subject, payload)"
        " VALUES (%s, %s, %s, %s) RETURNING 1",
        (
            ws,
            f"{harness.cell.name}.{r.name}.{verb}",
            subject,
            json.dumps({"version": version, "deleted": verb == "deleted"}),
        ),
    )


def _settle(harness, *subjects: str) -> None:
    """Mark a test's rows relayed so later tests see an outbox without them."""
    harness.superuser(
        f"UPDATE {harness.cell.schema}.outbox SET relayed_at = now()"
        " WHERE subject = ANY(%s) AND relayed_at IS NULL RETURNING 1",
        (list(subjects),),
    )


def test_a_failure_in_one_workspace_does_not_block_the_same_subject_in_another(harness):
    """Two workspaces may import the same legacy id, so they share a subject but not a record."""
    from cellkit.relay import relay_once

    _drain(harness)
    r = harness.cell.resources[0]
    subject = f"empire:{harness.cell.name}:{r.name}:999991"
    first, second = str(uuid.uuid4()), str(uuid.uuid4())
    _insert_event(harness, first, subject, 1)
    _insert_event(harness, second, subject, 1)
    fake = FakePublisher(fail=1)
    with _publisher_conn(harness) as conn:
        result = relay_once(conn, harness.cell, fake)
    assert (result.sent, result.failed) == (1, 1)
    assert [v["data"]["workspace_id"] for _, _, v in fake.sent] == [second]
    # While the first workspace's event backs off, the second workspace's next event still goes.
    _insert_event(harness, second, subject, 2)
    fake = FakePublisher()
    with _publisher_conn(harness) as conn:
        assert relay_once(conn, harness.cell, fake).sent == 1
    assert [v["data"]["workspace_id"] for _, _, v in fake.sent] == [second]
    _settle(harness, subject)


def test_a_re_created_legacy_id_keeps_its_events_in_write_order(client, owner, harness):
    """Delete then re-import the same legacy id: versions go 1, 2, 1, the order must not."""
    from cellkit.relay import relay_once

    _drain(harness)
    r = harness.cell.resources[0]
    legacy = 987_651
    record = make(client, owner, harness.cell, r, legacy_id=legacy)
    assert (
        client.delete(f"/v1/{r.plural}/{record['id']}", headers=owner).status_code
        == 204
    )
    again = make(client, owner, harness.cell, r, legacy_id=legacy)
    assert again["id"] == record["id"]
    subject = f"empire:{harness.cell.name}:{r.name}:{record['id']}"
    fake = FakePublisher()
    with _publisher_conn(harness) as conn:
        while relay_once(conn, harness.cell, fake).sent:
            pass
    assert [
        v["type"].rsplit(".", 1)[1] for _, key, v in fake.sent if key == subject
    ] == [
        "created",
        "deleted",
        "created",
    ]
    assert (
        client.delete(f"/v1/{r.plural}/{record['id']}", headers=owner).status_code
        == 204
    )
    _drain(harness)


def test_the_relay_job_goes_on_after_a_batch_of_failures_only(
    harness, monkeypatch, capsys
):
    """A batch whose first row fails and whose rest wait behind it must not end the job."""
    import functools

    from cellkit import relay

    _drain(harness)
    r = harness.cell.resources[0]
    ws = str(uuid.uuid4())
    stuck = f"empire:{harness.cell.name}:{r.name}:999992"
    other = f"empire:{harness.cell.name}:{r.name}:999993"
    _insert_event(harness, ws, stuck, 1)
    _insert_event(harness, ws, stuck, 2)
    _insert_event(harness, ws, other, 1)
    names = harness.cell
    monkeypatch.setenv(
        f"{names.env}_RELAY_DATABASE_URL",
        f"host={harness.cluster.socket_dir} port={getattr(harness.cluster, 'port', 5434)}"
        f" user={names.role('publisher')} dbname={names.schema}",
    )
    fake = FakePublisher(fail=1)
    monkeypatch.setattr(relay, "publisher_from_env", lambda _cell: fake)
    monkeypatch.setattr(relay, "load", lambda _path: harness.cell)
    monkeypatch.setattr(
        relay, "relay_once", functools.partial(relay.relay_once, batch_size=2)
    )
    assert relay.main([]) == 1
    printed = json.loads(capsys.readouterr().out)
    assert (printed["sent"], printed["failed"]) == (1, 1)
    assert [key for _, key, _ in fake.sent] == [other]
    _settle(harness, stuck, other)


def test_the_relay_order_migration_numbers_every_workspaces_rows_as_the_migrator(
    harness,
):
    """The migrator has no BYPASSRLS and the outbox forces RLS: the backfill must still see all rows."""
    schema = harness.cell.schema
    sql = next((ROOT / "migrations").glob("*_relay_order.sql")).read_text()
    sql = re.sub(r"ALTER TABLE \S+ ADD COLUMN relay_seq[^;]*;", "", sql)
    _drain(harness)
    r = harness.cell.resources[0]
    subject = f"empire:{harness.cell.name}:{r.name}:999994"
    ws = str(uuid.uuid4())
    for version in (1, 2):
        _insert_event(harness, ws, subject, version)
    # As if ADD COLUMN had numbered them in the wrong physical order.
    harness.superuser(
        f"UPDATE {schema}.outbox SET relay_seq = 1000000 - relay_seq WHERE subject = %s RETURNING 1",
        (subject,),
    )
    with harness.cluster.connect(harness.cell.role("migrator")) as conn:
        conn.execute(sql)
    seqs = harness.superuser(
        f"SELECT payload->>'version' FROM {schema}.outbox WHERE subject = %s ORDER BY relay_seq",
        (subject,),
    )
    assert [v for (v,) in seqs] == ["1", "2"]
    top = harness.superuser(f"SELECT max(relay_seq) FROM {schema}.outbox")[0][0]
    _insert_event(harness, ws, subject, 3)
    newest = harness.superuser(
        f"SELECT relay_seq FROM {schema}.outbox WHERE subject = %s"
        " AND payload->>'version' = '3'",
        (subject,),
    )[0][0]
    assert newest > top
    forced = harness.superuser(
        "SELECT relforcerowsecurity FROM pg_class WHERE oid = %s::regclass",
        (f"{schema}.outbox",),
    )[0][0]
    assert forced is True
    _settle(harness, subject)
