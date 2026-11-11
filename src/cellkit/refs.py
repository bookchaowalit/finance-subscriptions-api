"""Cross-service references between the finance cells (SPEC-006).

A field declared ``references: <cell>.<resource>`` must name a live record of that cell in the caller's
workspace; a resource with ``referenced_by`` cannot be deleted while records of other cells point at it.
Neither asks the other service: each cell keeps event-fed copies (``ref_targets``, ``ref_sources``) and,
on a miss, first catches up from Kafka ``finance.events.v1`` (bounded), then decides. The hourly relay
job catches up too. The read position lives in ``ref_offsets``, written in the same transaction as the
rows, so a crash replays at most the last batch (idempotent by record version).

Environment: the SPEC-005 Kafka settings (``<CELL>_KAFKA_*``) and ``<CELL>_REFS_DATABASE_URL`` (the
publisher role; the hourly job falls back to ``<CELL>_RELAY_DATABASE_URL``).
"""

from __future__ import annotations

import json
import os
import tempfile
import time
import uuid
from collections.abc import Mapping
from typing import Any, Protocol

import psycopg

from cellkit import relay
from cellkit.spec import Cell, Field, Resource

TOPIC = relay.TOPIC
CATCH_UP_SECONDS = 5.0
READ_SHARE = 0.6  # of the catch-up deadline; applying gets the rest
# Only the test harness sets this (cellkit.testing): writes then accept any id without a lookup.
AUTOTRUST = False


class Unavailable(RuntimeError):
    """The reference data cannot be read (no Kafka settings or database URL, or the broker failed)."""


class Incomplete(Unavailable):
    """The deadline came first: ``messages`` were read and are saved; the rest waits for next time."""

    def __init__(self, messages: list[tuple[int, int, dict[str, Any]]]) -> None:
        super().__init__("catch-up deadline")
        self.messages = messages


class Missing(Exception):
    """Ids not (yet) known here; the caller catches up once and retries."""

    def __init__(self, fields: list[str]) -> None:
        super().__init__(", ".join(fields))
        self.fields = fields


class Refused(Exception):
    """A definite no: the target was deleted (``reference_not_found``) or is closed."""

    def __init__(self, error: str, fields: list[str]) -> None:
        super().__init__(error)
        self.error = error
        self.fields = fields


class Reader(Protocol):
    def read(
        self, start: dict[int, int], deadline: float
    ) -> list[tuple[int, int, dict[str, Any]]]:
        """Messages (partition, offset, envelope) from ``start`` up to the end, until ``deadline``."""


def has_references(cell: Cell) -> bool:
    return any(f.references for r in cell.resources for f in r.fields) or any(
        r.referenced_by for r in cell.resources
    )


# --- checks on the API's own (workspace-bound) connection ---------------------------------------


def check(
    conn: psycopg.Connection[Any], cell: Cell, fields: Mapping[Field, int]
) -> None:
    """Raise Refused for a deleted or closed target, Missing for one not known yet."""
    if AUTOTRUST or not fields:
        return
    missing: list[str] = []
    gone: list[str] = []
    closed: list[str] = []
    for f, value in fields.items():
        row = conn.execute(
            f"SELECT status, deleted FROM {cell.schema}.ref_targets WHERE target = %s AND record_id = %s",
            (f.references, value),
        ).fetchone()
        if row is None:
            missing.append(f.name)
            continue
        status, deleted = (
            (row["status"], row["deleted"]) if isinstance(row, dict) else row
        )
        if deleted:
            gone.append(f.name)
        elif status in f.refuse_status:
            closed.append(f.name)
    if gone:
        raise Refused("reference_not_found", sorted(gone))
    if closed:
        raise Refused("reference_closed", sorted(closed))
    if missing:
        raise Missing(sorted(missing))


def has_dependents(
    conn: psycopg.Connection[Any], cell: Cell, r: Resource, record_id: int
) -> bool:
    if not r.referenced_by:
        return False
    row = conn.execute(
        f"SELECT 1 FROM {cell.schema}.ref_sources WHERE target_resource = %s AND target_id = %s"
        " AND NOT deleted LIMIT 1",
        (r.name, record_id),
    ).fetchone()
    return row is not None


# --- catching up from Kafka (publisher role, every workspace) ------------------------------------


def _kind(envelope: Mapping[str, Any]) -> str:
    return ".".join(str(envelope.get("type", "")).split(".")[:2])


BIGINT = (-(2**63), 2**63 - 1)
NEWER = (
    "excluded.event_seq > t.event_seq OR (excluded.event_seq = 0 AND t.event_seq = 0"
    " AND excluded.record_version >= t.record_version)"
)


def _bigint(value: Any) -> int | None:
    """An int that fits a bigint column, or None (bools and anything else are refused)."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str) and value.lstrip("-").isdigit():
        number = int(value)
    elif isinstance(value, int):
        number = value
    else:
        raise ValueError("not an integer")  # floats (also inf/NaN) and anything else
    if not BIGINT[0] <= number <= BIGINT[1]:
        raise ValueError("out of bigint range")
    return number


def _apply(cur: Any, cell: Cell, envelope: Mapping[str, Any]) -> bool:
    """Apply one event; False when it is not one this cell learns from. Raises on a bad message."""
    kind = _kind(envelope)
    targets = {f.references for r in cell.resources for f in r.fields if f.references}
    sources = [
        (r, s) for r in cell.resources for s in r.referenced_by if s.kind == kind
    ]
    if kind not in targets and not sources:
        return False
    # Only the producer named in the type may speak for that kind (the Kafka ACL limits writers to
    # the ten finance producers; a forged producer field is refused here).
    if str(envelope.get("producer")) != kind.split(".")[0]:
        return False
    data = envelope.get("data")
    if not isinstance(data, Mapping):
        return False
    ws = str(uuid.UUID(str(data["workspace_id"])))
    record_id = _bigint(data["record_id"])
    if record_id is None:
        return False
    version = _bigint(data.get("record_version")) or 0
    # The producer's outbox seq orders a record's events, also across a re-created legacy id (its
    # version starts again at 1), and makes a redelivered old event a no-op.
    seq = _bigint(data.get("event_seq")) or 0
    deleted = bool(data.get("deleted"))
    if not deleted:
        for _r, source in sources:
            # A live record's event always carries its reference fields (None when unset); one
            # without them is malformed and must not clear a dependent (KeyError: skipped).
            if source.field not in data:
                raise KeyError(source.field)
    status = data.get("status")
    status = str(status)[:64] if status is not None else None
    s = cell.schema
    if kind in targets:
        cur.execute(
            f"INSERT INTO {s}.ref_targets AS t (workspace_id, target, record_id, status, deleted,"
            " record_version, event_seq) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            " ON CONFLICT (workspace_id, target, record_id) DO UPDATE SET"
            " status = coalesce(excluded.status, t.status), deleted = excluded.deleted,"
            " record_version = excluded.record_version, event_seq = excluded.event_seq,"
            f" updated_at = now() WHERE {NEWER}",
            (ws, kind, record_id, status, deleted, version, seq),
        )
    for r, source in sources:
        target_id = None if deleted else _bigint(data.get(source.field))
        cur.execute(
            f"INSERT INTO {s}.ref_sources AS t (workspace_id, source, source_id, field, target_resource,"
            " target_id, deleted, record_version, event_seq)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            " ON CONFLICT (workspace_id, source, source_id, field) DO UPDATE SET"
            " target_id = CASE WHEN excluded.deleted THEN t.target_id ELSE excluded.target_id END,"
            " deleted = excluded.deleted, record_version = excluded.record_version,"
            f" event_seq = excluded.event_seq, updated_at = now() WHERE {NEWER}",
            (
                ws,
                kind,
                record_id,
                source.field,
                r.name,
                target_id,
                deleted,
                version,
                seq,
            ),
        )
    return True


def catch_up(
    conn: psycopg.Connection[Any],
    cell: Cell,
    reader: Reader,
    seconds: float = CATCH_UP_SECONDS,
) -> int:
    """Read what is new on the topic into the reference tables; returns the messages read."""
    s = cell.schema
    with conn.transaction():
        cur = conn.cursor()
        cur.execute(
            "SELECT set_config('lock_timeout', %s, true)", (f"{int(seconds * 1000)}ms",)
        )
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))", (f"finance-refs:{s}",)
        )
        start = {
            int(p): int(o)
            for p, o in cur.execute(
                f"SELECT partition, next_offset FROM {s}.ref_offsets WHERE topic = %s",
                (TOPIC,),
            ).fetchall()
        }
        # A backlog bigger than one deadline still drains: what was read is applied and its position
        # saved, then the caller hears Unavailable (this time) and the next catch-up goes on from there.
        # One deadline covers reading and applying: reading gets most of it, applying stops at the
        # end (after at least one message, so a backlog always moves) and the rest is read again.
        began = time.monotonic()
        incomplete: Incomplete | None = None
        try:
            messages = reader.read(start, began + seconds * READ_SHARE)
        except Incomplete as exc:
            incomplete, messages = exc, exc.messages
        ends: dict[int, int] = {}
        left: set[int] = set()
        applied = 0
        for index, (partition, offset, envelope) in enumerate(messages):
            if index and time.monotonic() > began + seconds:
                left = {p for p, _, _ in messages[index:]}
                incomplete = incomplete or Incomplete(messages[:index])
                break
            applied += 1
            # One bad message (malformed, out of range) is skipped, never a stuck offset: each is
            # applied in its own savepoint and the read position moves past it either way.
            try:
                with conn.transaction():
                    _apply(cur, cell, envelope)
            except (
                KeyError,
                TypeError,
                ValueError,
                ArithmeticError,
                psycopg.DataError,
                psycopg.IntegrityError,
            ):
                # Its content is bad: skip it. Anything else (the database or the lock failing)
                # rolls the whole batch back, so the position stays and the batch is read again.
                pass
            ends[partition] = max(ends.get(partition, 0), offset + 1)
        for partition, position in getattr(reader, "positions", {}).items():
            if partition not in left:  # never move past a message that was not applied
                ends[partition] = max(ends.get(partition, 0), int(position))
        for partition, next_offset in ends.items():
            cur.execute(
                f"INSERT INTO {s}.ref_offsets (topic, partition, next_offset) VALUES (%s, %s, %s)"
                " ON CONFLICT (topic, partition) DO UPDATE SET"
                " next_offset = greatest(ref_offsets.next_offset, excluded.next_offset)",
                (TOPIC, partition, next_offset),
            )
    if incomplete is not None:
        raise incomplete  # after the commit: the partial progress is kept
    return applied


class KafkaReader:
    """Reads finance.events.v1 from stored offsets to the high watermark (no consumer-group commits)."""

    def __init__(self, config: dict[str, Any]) -> None:
        from confluent_kafka import Consumer

        self.positions: dict[int, int] = {}

        self.consumer = Consumer(
            {**config, "enable.auto.commit": False, "auto.offset.reset": "earliest"}
        )

    def read(
        self, start: dict[int, int], deadline: float
    ) -> list[tuple[int, int, dict[str, Any]]]:
        from confluent_kafka import OFFSET_BEGINNING, TopicPartition

        self.positions = {}  # never carry positions over from an earlier read

        meta = self.consumer.list_topics(
            TOPIC, timeout=max(1.0, deadline - time.monotonic())
        )
        topic = meta.topics.get(TOPIC)
        # A missing topic or a metadata error is "cannot tell", never an empty topic.
        if (
            topic is None
            or getattr(topic, "error", None) is not None
            or not topic.partitions
        ):
            raise Unavailable("topic metadata")
        partitions = sorted(topic.partitions)
        marks = {
            p: self.consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=5)
            for p in partitions
        }
        # A stored position below the low watermark means events expired unread (retention):
        # the reference state may be missing deletes, so refuse rather than call it complete.
        # Only partitions that delivered something store a position, so once the reader has any
        # position, a partition without one started at offset 0.
        if start and any(start.get(p, 0) < low for p, (low, _high) in marks.items()):
            raise Unavailable("unread history expired")
        ends = {p: high for p, (_low, high) in marks.items()}
        # A reader with no stored position starts at the oldest message left.
        first = {p: max(start.get(p, 0), low) for p, (low, _high) in marks.items()}
        todo = {p for p in partitions if first[p] < ends[p]}
        # Every partition looked at gets a position (read to its end, or where it starts), so a
        # quiet partition is never mistaken for one whose history expired on the next read.
        self.positions = {p: (first[p] if p in todo else ends[p]) for p in partitions}
        if not todo:
            return []
        self.consumer.assign(
            [
                TopicPartition(TOPIC, p, first[p] if p in start else OFFSET_BEGINNING)
                for p in todo
            ]
        )
        out: list[tuple[int, int, dict[str, Any]]] = []
        try:
            while todo and time.monotonic() < deadline:
                message = self.consumer.poll(0.5)
                if message is None:
                    continue
                error = message.error()
                if error is not None:
                    raise Unavailable(str(error.name()))
                p, o = int(message.partition() or 0), int(message.offset() or 0)
                try:
                    value = json.loads(message.value() or b"")
                except (TypeError, ValueError):
                    value = {}
                out.append((p, o, value if isinstance(value, dict) else {}))
                if o + 1 >= ends[p]:
                    todo.discard(p)
        finally:
            self.consumer.unassign()
        if todo:
            raise Incomplete(out)
        return out


_READERS: dict[str, KafkaReader] = {}


def reader_from_env(
    cell: Cell, environ: Mapping[str, str] = os.environ
) -> Reader | None:
    if not relay.configured(cell, environ):
        return None
    if cell.name not in _READERS:
        prefix = f"{cell.env}_KAFKA_"
        with tempfile.NamedTemporaryFile(
            "w", delete=False, suffix=".pem", prefix="kafka-ca-"
        ) as ca:
            ca.write(environ[prefix + "CA_PEM"])
        _READERS[cell.name] = KafkaReader(
            {
                "bootstrap.servers": environ[prefix + "BOOTSTRAP"],
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "SCRAM-SHA-256",
                "sasl.username": environ[prefix + "USERNAME"],
                "sasl.password": environ[prefix + "PASSWORD"],
                "ssl.ca.location": ca.name,
                "group.id": f"{cell.name}-refs",
                "client.id": f"{cell.name}-refs",
            }
        )
    return _READERS[cell.name]


def sync_connection(
    cell: Cell, environ: Mapping[str, str] = os.environ
) -> psycopg.Connection[Any] | None:
    url = environ.get(f"{cell.env}_REFS_DATABASE_URL") or environ.get(
        f"{cell.env}_RELAY_DATABASE_URL"
    )
    return psycopg.connect(url, connect_timeout=10) if url else None


def sync(cell: Cell) -> int:
    """Catch up now; Unavailable when the reference data cannot be read."""
    try:
        reader = reader_from_env(cell)
        conn = sync_connection(cell) if reader is not None else None
    except Exception as exc:  # bad settings or an unreachable database: "cannot tell"
        raise Unavailable(type(exc).__name__) from exc
    if reader is None:
        raise Unavailable("no Kafka settings")
    if conn is None:
        raise Unavailable("no reference database URL")
    try:
        with conn:
            return catch_up(conn, cell, reader)
    except (psycopg.Error, Unavailable) as exc:
        raise Unavailable(type(exc).__name__) from exc
    except Exception as exc:
        raise Unavailable(type(exc).__name__) from exc
