"""SPEC-006: cross-service references (the same file runs in all ten finance services)."""

from __future__ import annotations

import itertools
import uuid
from pathlib import Path
from typing import Any

import pytest

from cellkit.contract import make
from cellkit.spec import SpecError, load, parse
from cellkit.testing import body

ROOT = Path(__file__).resolve().parents[1]
_ids = itertools.count(7_000_000)


def cell():
    return load(ROOT / "cell.yaml")


def _referencing():
    """(resource, field) pairs of this cell that point into another cell."""
    return [(r, f) for r in cell().resources for f in r.fields if f.references]


def _referenced():
    return [r for r in cell().resources if r.referenced_by]


class FakeReader:
    """Kafka finance.events.v1 as a list; serves what lies past the stored offsets."""

    def __init__(self) -> None:
        self.messages: list[tuple[int, int, dict[str, Any]]] = []
        self.reads = 0

    def add(self, envelope: dict[str, Any], partition: int = 0) -> None:
        offset = sum(1 for p, _, _ in self.messages if p == partition)
        self.messages.append((partition, offset, envelope))

    def read(
        self, start: dict[int, int], deadline: float
    ) -> list[tuple[int, int, dict[str, Any]]]:
        del deadline
        self.reads += 1
        return [m for m in self.messages if m[1] >= start.get(m[0], 0)]


_seq = itertools.count(1)


def event(
    target: str, verb: str, ws: uuid.UUID, record_id: int, version: int = 1, **data: Any
):
    producer, resource = target.split(".")
    return {
        "id": str(uuid.uuid4()),
        "type": f"{producer}.{resource}.{verb}",
        "version": 1,
        "occurred_at": "2026-11-11T08:00:00+00:00",
        "producer": producer,
        "correlation_id": "c",
        "causation_id": None,
        "data": {
            **data,
            "workspace_id": str(ws),
            "record_id": record_id,
            "subject": f"empire:{producer}:{resource}:{record_id}",
            "record_version": version,
            "event_seq": data.pop("seq", None) or next(_seq),
            "deleted": verb == "deleted",
        },
    }


@pytest.fixture
def reader(harness, monkeypatch):
    from cellkit import refs

    fake = FakeReader()
    # Each test's fake topic starts at offset 0; forget the position an earlier test stored.
    harness.superuser(f"DELETE FROM {harness.cell.schema}.ref_offsets RETURNING 1")
    monkeypatch.setattr(refs, "AUTOTRUST", False)
    monkeypatch.setattr(refs, "reader_from_env", lambda _cell: fake)
    monkeypatch.setattr(
        refs,
        "sync_connection",
        lambda _cell: harness.cluster.connect(harness.cell.role("publisher")),
    )
    return fake


@pytest.fixture
def tenant(harness, platform):
    actor, ws = uuid.uuid4(), uuid.uuid4()
    platform.grants[actor] = set(harness.cell.permissions())
    return ws, harness.token(actor, ws)


# --- spec -----------------------------------------------------------------------------------------


def test_spec_accepts_references_only_on_bigint_fields():
    ok = parse(
        {
            "cell": "demo",
            "resources": {
                "item": {
                    "fields": {
                        "account_id": {
                            "type": "bigint",
                            "references": "finance-accounts.account",
                            "refuse_status": ["closed"],
                        },
                    }
                }
            },
        }
    )
    f = ok.resource("item").field("account_id")
    assert (f.references, f.refuse_status) == ("finance-accounts.account", ("closed",))
    for bad in (
        {"type": "text", "references": "a.b"},
        {"type": "bigint", "references": "nodot"},
        {"type": "bigint", "refuse_status": ["closed"]},
    ):
        with pytest.raises(SpecError):
            parse({"cell": "demo", "resources": {"item": {"fields": {"x_id": bad}}}})


def test_this_cell_declares_its_finance_references():
    known = {
        ("transactions", "transaction", "account_id"): "finance-accounts.account",
        (
            "transactions",
            "transaction",
            "subscription_id",
        ): "subscriptions.subscription",
        ("investments", "investment", "account_id"): "finance-accounts.account",
        ("liabilities", "liability", "account_id"): "finance-accounts.account",
        ("reconciliations", "reconciliation", "account_id"): "finance-accounts.account",
        ("subscriptions", "subscription", "account_id"): "finance-accounts.account",
    }
    c = cell()
    declared = {(c.name, r.name, f.name): f.references for r, f in _referencing()}
    assert declared == {k: v for k, v in known.items() if k[0] == c.name}
    inbound = {
        r.name: sorted(f"{s.cell}.{s.resource}.{s.field}" for s in r.referenced_by)
        for r in _referenced()
    }
    expected = {
        "finance-accounts": {
            "account": sorted(
                f"{a}.{b}.{x}"
                for (a, b, x), t in known.items()
                if t == "finance-accounts.account"
            )
        },
        "subscriptions": {"subscription": ["transactions.transaction.subscription_id"]},
    }
    assert inbound == expected.get(c.name, {})


# --- AC-01..AC-04, AC-06, AC-07: checks on write ---------------------------------------------------


def _skip_without_references():
    if not _referencing():
        pytest.skip("this cell points into no other cell")


def _post(client, headers, r, field, value):
    payload = {**body(r, {}, 900), field.name: value}
    return client.post(f"/v1/{r.plural}", json=payload, headers=headers)


def test_an_unknown_reference_is_refused(client, tenant, reader):
    _skip_without_references()
    _, headers = tenant
    for r, f in _referencing():
        refused = _post(client, headers, r, f, next(_ids))
        assert refused.status_code == 422, refused.text
        assert refused.json()["detail"] == {
            "error": "reference_not_found",
            "fields": [f.name],
        }


def test_a_target_just_created_elsewhere_is_accepted_after_a_catch_up(
    client, tenant, reader
):
    _skip_without_references()
    ws, headers = tenant
    for r, f in _referencing():
        target = next(_ids)
        reader.add(event(f.references, "created", ws, target, status="active"))
        created = _post(client, headers, r, f, target)
        assert created.status_code == 201, created.text
        assert created.json()[f.name] == target


def test_another_workspaces_target_never_counts(client, tenant, reader):
    _skip_without_references()
    _, headers = tenant
    for r, f in _referencing():
        target = next(_ids)
        reader.add(
            event(f.references, "created", uuid.uuid4(), target, status="active")
        )
        assert _post(client, headers, r, f, target).status_code == 422


def test_deleted_and_closed_targets_are_refused(client, tenant, reader):
    _skip_without_references()
    ws, headers = tenant
    for r, f in _referencing():
        gone, closed = next(_ids), next(_ids)
        reader.add(event(f.references, "created", ws, gone, status="active"))
        reader.add(event(f.references, "deleted", ws, gone, version=2))
        assert (
            _post(client, headers, r, f, gone).json()["detail"]["error"]
            == "reference_not_found"
        )
        if f.refuse_status:
            reader.add(
                event(f.references, "updated", ws, closed, status=f.refuse_status[0])
            )
            refused = _post(client, headers, r, f, closed)
            assert refused.status_code == 422
            assert refused.json()["detail"] == {
                "error": "reference_closed",
                "fields": [f.name],
            }


def test_the_highest_version_wins(client, tenant, reader):
    _skip_without_references()
    ws, headers = tenant
    for r, f in _referencing():
        target = next(_ids)
        reader.add(
            event(f.references, "deleted", ws, target, version=3, seq=10**9 + target)
        )
        reader.add(
            event(
                f.references, "updated", ws, target, version=2, status="active", seq=1
            )
        )
        assert _post(client, headers, r, f, target).status_code == 422


def test_a_target_recreated_after_a_delete_counts_again(client, tenant, reader):
    _skip_without_references()
    ws, headers = tenant
    for r, f in _referencing():
        target = next(_ids)
        reader.add(event(f.references, "created", ws, target, status="active"))
        reader.add(event(f.references, "deleted", ws, target, version=2))
        reader.add(event(f.references, "created", ws, target, status="active"))
        assert _post(client, headers, r, f, target).status_code == 201


def test_a_late_duplicate_create_never_revives_a_deleted_target(client, tenant, reader):
    _skip_without_references()
    ws, headers = tenant
    for r, f in _referencing():
        target = next(_ids)
        created = event(f.references, "created", ws, target, status="active")
        reader.add(created)
        reader.add(event(f.references, "deleted", ws, target, version=2))
        reader.add(created)  # redelivered after the delete
        assert _post(client, headers, r, f, target).status_code == 422


def test_a_patch_checks_only_a_changed_reference(client, tenant, reader):
    _skip_without_references()
    ws, headers = tenant
    for r, f in _referencing():
        target = next(_ids)
        reader.add(event(f.references, "created", ws, target, status="active"))
        record = _post(client, headers, r, f, target).json()
        path = f"/v1/{r.plural}/{record['id']}"
        moved = client.patch(path, headers=headers, json={f.name: next(_ids)})
        assert moved.status_code == 422
        if f.refuse_status:
            reader.add(
                event(
                    f.references,
                    "updated",
                    ws,
                    target,
                    version=2,
                    status=f.refuse_status[0],
                )
            )
        assert (
            client.patch(path, headers=headers, json={f.name: target}).status_code
            == 200
        )


def test_without_reference_data_the_write_fails_closed(
    client, tenant, harness, monkeypatch
):
    _skip_without_references()
    from cellkit import refs

    monkeypatch.setattr(refs, "AUTOTRUST", False)
    monkeypatch.setattr(refs, "reader_from_env", lambda _cell: None)
    _, headers = tenant
    r, f = _referencing()[0]
    refused = _post(client, headers, r, f, next(_ids))
    assert refused.status_code == 503
    assert refused.json()["detail"] == {"error": "references_unavailable"}


def test_the_api_role_sees_only_its_workspaces_targets(harness, tenant, reader):
    _skip_without_references()
    from cellkit import refs

    ws, _ = tenant
    other = uuid.uuid4()
    _, f = _referencing()[0]
    mine, theirs = next(_ids), next(_ids)
    reader.add(event(f.references, "created", ws, mine, status="active"))
    reader.add(event(f.references, "created", other, theirs, status="active"))
    with refs.sync_connection(harness.cell) as conn:
        refs.catch_up(conn, harness.cell, reader)
    with harness.cluster.connect(harness.cell.role("api")) as conn:
        conn.execute("SELECT set_config('app.workspace_id', %s, false)", (str(ws),))
        seen = {
            row[0]
            for row in conn.execute(
                f"SELECT record_id FROM {harness.cell.schema}.ref_targets"
            )
        }
    assert mine in seen
    assert theirs not in seen


def test_offsets_advance_with_the_rows(harness, reader):
    _skip_without_references()
    from cellkit import refs

    _, f = _referencing()[0]
    reader.add(
        event(f.references, "created", uuid.uuid4(), next(_ids), status="active")
    )
    with refs.sync_connection(harness.cell) as conn:
        first = refs.catch_up(conn, harness.cell, reader)
        again = refs.catch_up(conn, harness.cell, reader)
    assert first >= 1
    assert again == 0


# --- AC-05: a referenced record cannot be deleted ---------------------------------------------------


def test_a_referenced_record_cannot_be_deleted_until_its_dependents_go(
    client, owner, harness, tenant, reader
):
    if not _referenced():
        pytest.skip("nothing points at this cell")
    ws, headers = tenant
    for r in _referenced():
        record = make(client, headers, harness.cell, r)
        source = r.referenced_by[0]
        dependent = next(_ids)
        reader.add(
            event(
                f"{source.cell}.{source.resource}",
                "created",
                ws,
                dependent,
                **{source.field: record["id"]},
            )
        )
        path = f"/v1/{r.plural}/{record['id']}"
        blocked = client.delete(path, headers=headers)
        assert blocked.status_code == 409, blocked.text
        assert blocked.json()["detail"] == {"error": "has_dependents"}
        reader.add(
            event(
                f"{source.cell}.{source.resource}", "deleted", ws, dependent, version=2
            )
        )
        assert client.delete(path, headers=headers).status_code == 204


def test_a_delete_without_the_permission_reads_no_reference_data(
    client, harness, platform, reader
):
    if not _referenced():
        pytest.skip("nothing points at this cell")
    r = _referenced()[0]
    actor, ws = uuid.uuid4(), uuid.uuid4()
    platform.grants[actor] = {
        p for p in harness.cell.permissions() if not p.endswith(":delete")
    }
    headers = harness.token(actor, ws)
    reads = reader.reads
    assert client.delete(f"/v1/{r.plural}/1", headers=headers).status_code == 403
    assert reader.reads == reads  # no Kafka catch-up for a caller who may not delete


def test_a_dependent_that_moved_away_no_longer_blocks(client, harness, tenant, reader):
    if not _referenced():
        pytest.skip("nothing points at this cell")
    ws, headers = tenant
    r = _referenced()[0]
    record = make(client, headers, harness.cell, r)
    source = r.referenced_by[0]
    dependent = next(_ids)
    kind = f"{source.cell}.{source.resource}"
    reader.add(event(kind, "created", ws, dependent, **{source.field: record["id"]}))
    reader.add(
        event(
            kind,
            "updated",
            ws,
            dependent,
            version=2,
            **{source.field: record["id"] + 1},
        )
    )
    assert (
        client.delete(f"/v1/{r.plural}/{record['id']}", headers=headers).status_code
        == 204
    )


# --- AC-08 and the envelope ---------------------------------------------------------------------------


def test_envelopes_carry_the_record_version(client, owner, harness):
    from cellkit.relay import envelope

    row = {
        "id": uuid.uuid4(),
        "workspace_id": uuid.uuid4(),
        "type": "x.y.updated",
        "subject": "empire:x:y:5",
        "execution_id": None,
        "payload": {"version": 4, "deleted": False, "status": "active"},
        "occurred_at": __import__("datetime").datetime(2026, 11, 11),
        "correlation_id": None,
    }
    assert envelope(harness.cell, row)["data"]["record_version"] == 4


def test_the_migration_announces_rows_written_before_events():
    text = "\n".join(
        p.read_text()
        for p in sorted((ROOT / "migrations").glob("*_cross_references.sql"))
    )
    for r in cell().resources:
        assert f"'{cell().name}.{r.name}.snapshot'" in text, r.name


# --- sync wiring --------------------------------------------------------------------------------------


def test_sync_is_unavailable_without_kafka_or_a_database_url(harness, monkeypatch):
    from cellkit import refs

    monkeypatch.setattr(refs, "reader_from_env", lambda _cell: None)
    with pytest.raises(refs.Unavailable):
        refs.sync(harness.cell)
    monkeypatch.setattr(refs, "reader_from_env", lambda _cell: FakeReader())
    monkeypatch.setattr(refs, "sync_connection", lambda _cell: None)
    with pytest.raises(refs.Unavailable):
        refs.sync(harness.cell)


def test_settings_come_from_the_environment(harness):
    from cellkit import refs

    env = {f"{harness.cell.env}_KAFKA_BOOTSTRAP": "x"}
    assert refs.reader_from_env(harness.cell, env) is None
    assert refs.sync_connection(harness.cell, {}) is None


def test_the_hourly_job_catches_up_references(harness, reader, monkeypatch):
    from cellkit import relay

    if not __import__("cellkit.refs", fromlist=["x"]).has_references(harness.cell):
        pytest.skip("no references")
    target = next(_ids)
    kind = (_referencing() and _referencing()[0][1].references) or (
        f"{_referenced()[0].referenced_by[0].kind}"
    )
    reader.add(event(kind, "created", uuid.uuid4(), target, status="active"))
    with harness.cluster.connect(harness.cell.role("publisher")) as conn:
        assert relay._catch_up_references(conn, harness.cell) == 1


def test_messages_that_are_not_finance_events_are_skipped(harness, reader):
    from cellkit import refs

    if not refs.has_references(harness.cell):
        pytest.skip("no references")
    reader.add({"probe": True})
    with refs.sync_connection(harness.cell) as conn:
        assert refs.catch_up(conn, harness.cell, reader) == 1


# --- units that run in every cell (no references needed) ----------------------------------------------


class _Msg:
    def __init__(self, partition, offset, value, error=None):
        self._p, self._o, self._v, self._e = partition, offset, value, error

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def value(self):
        return self._v

    def error(self):
        return self._e


class _Consumer:
    def __init__(self, messages, end=2):
        self.messages, self.end, self.assigned = list(messages), end, None

    def list_topics(self, topic, timeout):
        from types import SimpleNamespace

        return SimpleNamespace(topics={topic: SimpleNamespace(partitions={0: None})})

    def get_watermark_offsets(self, tp, timeout):
        return (0, self.end)

    def assign(self, tps):
        self.assigned = tps

    def poll(self, timeout):
        return self.messages.pop(0) if self.messages else None

    def unassign(self):
        self.assigned = None


def _reader(consumer):
    from cellkit import refs

    reader = refs.KafkaReader.__new__(refs.KafkaReader)
    reader.consumer = consumer
    return reader


def test_the_kafka_reader_reads_to_the_high_watermark():
    import json as _json
    import time as _time

    got = _reader(_Consumer([_Msg(0, 0, b'{"a": 1}'), _Msg(0, 1, b"not json")])).read(
        {}, _time.monotonic() + 5
    )
    assert [(p, o) for p, o, _ in got] == [(0, 0), (0, 1)]
    assert got[0][2] == {"a": 1}
    assert got[1][2] == {}
    assert _reader(_Consumer([], end=0)).read({}, _time.monotonic() + 1) == []
    del _json


def test_the_kafka_reader_fails_on_a_broker_error_or_the_deadline():
    import time as _time

    from cellkit import refs

    class _Err:
        def name(self):
            return "BROKER"

    with pytest.raises(refs.Unavailable):
        _reader(_Consumer([_Msg(0, 0, b"{}", _Err())])).read({}, _time.monotonic() + 5)
    with pytest.raises(refs.Unavailable):
        _reader(_Consumer([], end=3)).read({}, _time.monotonic() + 0.6)


def test_a_reader_is_built_from_complete_kafka_settings(harness, monkeypatch):
    from cellkit import refs

    env = {
        f"{harness.cell.env}_KAFKA_{k}": "x"
        for k in ("BOOTSTRAP", "CA_PEM", "USERNAME", "PASSWORD")
    }
    built = []

    class _Stub:
        def __init__(self, config):
            built.append(config)

    monkeypatch.setattr(refs, "_READERS", {})
    monkeypatch.setattr(refs, "KafkaReader", _Stub)
    assert isinstance(refs.reader_from_env(harness.cell, env), _Stub)
    assert built[0]["security.protocol"] == "SASL_SSL"
    assert built[0]["group.id"] == f"{harness.cell.name}-refs"


def test_a_fresh_reader_starts_at_the_oldest_message_left():
    import time as _time

    class _Expired(_Consumer):
        def get_watermark_offsets(self, tp, timeout):
            return (5, 5)  # everything before offset 5 is gone, nothing new

    assert _reader(_Expired([])).read({}, _time.monotonic() + 1) == []


def test_unread_history_that_expired_is_not_reported_as_read():
    """A stored position below the low watermark lost events (retention): fail closed."""
    import time as _time

    from cellkit import refs

    class _Expired(_Consumer):
        def get_watermark_offsets(self, tp, timeout):
            return (5, 9)

    with pytest.raises(refs.Unavailable, match="expired"):
        _reader(_Expired([])).read({0: 2}, _time.monotonic() + 1)


def test_a_topic_metadata_error_is_not_an_empty_topic():
    import time as _time
    from types import SimpleNamespace

    from cellkit import refs

    class _NoTopic(_Consumer):
        def list_topics(self, topic, timeout):
            return SimpleNamespace(
                topics={topic: SimpleNamespace(partitions={}, error="UNKNOWN_TOPIC")}
            )

    class _Missing(_Consumer):
        def list_topics(self, topic, timeout):
            return SimpleNamespace(topics={})

    for consumer in (_NoTopic([]), _Missing([])):
        with pytest.raises(refs.Unavailable, match="metadata"):
            _reader(consumer).read({}, _time.monotonic() + 1)


def test_sync_turns_setup_failures_into_unavailable(harness, monkeypatch):
    from cellkit import refs

    def boom(_cell):
        raise RuntimeError("bad settings")

    monkeypatch.setattr(refs, "reader_from_env", boom)
    with pytest.raises(refs.Unavailable):
        refs.sync(harness.cell)
    monkeypatch.setattr(refs, "reader_from_env", lambda _cell: FakeReader())
    monkeypatch.setattr(refs, "sync_connection", boom)
    with pytest.raises(refs.Unavailable):
        refs.sync(harness.cell)


def test_the_harness_never_sees_real_kafka_settings(harness):
    import os

    assert not [k for k in os.environ if k.startswith(f"{harness.cell.env}_KAFKA_")]
    assert not os.environ.get(f"{harness.cell.env}_REFS_DATABASE_URL")


def test_sync_reads_through_the_publisher_role(harness, monkeypatch):
    from cellkit import refs

    harness.superuser(f"DELETE FROM {harness.cell.schema}.ref_offsets RETURNING 1")
    fake = FakeReader()
    fake.add({"probe": True})
    monkeypatch.setattr(refs, "reader_from_env", lambda _cell: fake)
    monkeypatch.setattr(
        refs,
        "sync_connection",
        lambda _cell: harness.cluster.connect(harness.cell.role("publisher")),
    )
    assert refs.sync(harness.cell) == 1


def test_events_update_the_reference_rows_they_concern():
    from cellkit import refs

    demo = parse(
        {
            "cell": "demo",
            "resources": {
                "item": {
                    "fields": {
                        "account_id": {
                            "type": "bigint",
                            "references": "finance-accounts.account",
                        }
                    }
                },
                "thing": {
                    "referenced_by": ["other.row.thing_id"],
                    "fields": {"name": {"type": "text"}},
                },
            },
        }
    )

    class _Cur:
        def __init__(self):
            self.sql = []

        def execute(self, sql, params=()):
            self.sql.append(sql.split(" AS ")[0])

    cur = _Cur()
    ws = str(uuid.uuid4())
    refs._apply(
        cur,
        demo,
        event("finance-accounts.account", "created", uuid.UUID(ws), 5, status="active"),
    )
    refs._apply(cur, demo, event("other.row", "created", uuid.UUID(ws), 9, thing_id=5))
    refs._apply(cur, demo, {"type": "x.y.created", "data": {"record_id": "bad"}})
    assert cur.sql == ["INSERT INTO demo.ref_targets", "INSERT INTO demo.ref_sources"]


def test_a_bad_or_forged_message_is_skipped_and_the_position_moves_on(harness, reader):
    from cellkit import refs

    if not _referencing():
        pytest.skip("this cell points into no other cell")
    _, f = _referencing()[0]
    ws = uuid.uuid4()
    forged = event(f.references, "created", ws, next(_ids), status="active")
    forged["producer"] = "someone-else"
    huge = event(f.references, "created", ws, 2**70, status="active")
    infinite = event(f.references, "created", ws, 1, status="active")
    infinite["data"]["record_id"] = float("inf")
    fractional = event(f.references, "created", ws, 1, status="active")
    fractional["data"]["record_version"] = 1.5
    good = event(f.references, "created", ws, next(_ids), status="active")
    for message in (forged, huge, infinite, fractional, good):
        reader.add(message)
    with refs.sync_connection(harness.cell) as conn:
        assert refs.catch_up(conn, harness.cell, reader) == 5
        assert refs.catch_up(conn, harness.cell, reader) == 0
    known = {
        row[0]
        for row in harness.superuser(
            f"SELECT record_id FROM {harness.cell.schema}.ref_targets WHERE workspace_id = %s",
            (str(ws),),
        )
    }
    assert known == {good["data"]["record_id"]}


def test_a_database_failure_keeps_the_position_for_a_retry(
    harness, reader, monkeypatch
):
    import psycopg as _psycopg

    from cellkit import refs

    if not _referencing():
        pytest.skip("this cell points into no other cell")
    _, f = _referencing()[0]
    reader.add(
        event(f.references, "created", uuid.uuid4(), next(_ids), status="active")
    )

    def down(*_args, **_kwargs):
        raise _psycopg.OperationalError("server closed the connection")

    real = refs._apply
    monkeypatch.setattr(refs, "_apply", down)
    with (
        refs.sync_connection(harness.cell) as conn,
        pytest.raises(_psycopg.OperationalError),
    ):
        refs.catch_up(conn, harness.cell, reader)
    monkeypatch.setattr(refs, "_apply", real)
    with refs.sync_connection(harness.cell) as conn:
        assert refs.catch_up(conn, harness.cell, reader) == 1  # read again, not lost
