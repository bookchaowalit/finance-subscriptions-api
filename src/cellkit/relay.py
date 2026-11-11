"""Relay a cell's outbox to Kafka ``finance.events.v1`` in the ADR-0008 envelope (SPEC-005).

Runs two ways:

- inline, after a write: the API relays its own workspace's rows in a background task
  (RLS-bound connection; best effort, never fails the request);
- as the hourly job ``<cell>-relay`` (GitHub Actions, scheduled by platform-jobs-worker):
  the ``<cell>_publisher`` role reads every workspace's outbox and retries with backoff.

At-least-once and ordered per record (key = subject); consumers dedupe on the envelope ``id``.
Only reference data leaves the cell (the resource's ``graph`` fields). The job prints counts only.

Environment (``<CELL>`` = the cell's env prefix, e.g. ``BUDGETS``):
    <CELL>_KAFKA_BOOTSTRAP, <CELL>_KAFKA_CA_PEM, <CELL>_KAFKA_USERNAME, <CELL>_KAFKA_PASSWORD
    <CELL>_RELAY_DATABASE_URL      the job only (the publisher role)
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import psycopg
from psycopg.rows import dict_row

from cellkit.spec import Cell, load

TOPIC = "finance.events.v1"
ENVELOPE_VERSION = 1
PUBLISH_TIMEOUT_SECONDS = 10.0
BATCH_TIME_BUDGET_SECONDS = 120.0
BATCH_SIZE = 100
MAX_BACKOFF_MINUTES_EXPONENT = 6  # 1, 2, 4 … 64 minutes


class Publisher(Protocol):
    def publish(self, topic: str, key: str, value: dict[str, Any]) -> None:
        """Return once the broker acknowledged the message; raise otherwise."""


@dataclass
class RelayResult:
    sent: int
    failed: int
    skipped: bool = False


def lock_key(cell: Cell) -> str:
    return f"finance-relay:{cell.schema}"


def envelope(cell: Cell, row: Mapping[str, Any]) -> dict[str, Any]:
    """The ADR-0008 envelope 1.0.0 for one outbox row (reference data only)."""
    payload = dict(row["payload"])
    subject = str(row["subject"])
    deleted = bool(payload.pop("deleted", False))
    record_version = int(payload.pop("version", 0) or 0)
    return {
        "id": str(row["id"]),
        "type": str(row["type"]),
        "version": ENVELOPE_VERSION,
        "occurred_at": row["occurred_at"].isoformat(),
        "producer": cell.name,
        "correlation_id": str(row["correlation_id"] or row["id"]),
        "causation_id": str(row["execution_id"]) if row["execution_id"] else None,
        "data": {
            **payload,
            "workspace_id": str(row["workspace_id"]),
            "record_id": int(subject.rsplit(":", 1)[1]),
            "subject": subject,
            "record_version": record_version,
            # The producer's insert order: consumers keep the state of the highest seq per record.
            "event_seq": int(row["seq"]) if row.get("seq") is not None else None,
            "deleted": deleted,
        },
    }


def _select(schema: str) -> str:
    table = f"{schema}.outbox"
    # A record's events go out in insert order (seq): the row lock serialises writes to one record,
    # so a later insert is a later commit, even when its transaction started first (occurred_at) or
    # it re-created a deleted legacy id (its version starts again at 1). While an earlier event of
    # the same record of the same workspace waits out its backoff, later ones wait too.
    return (
        f"SELECT o.id, o.workspace_id, o.type, o.subject, o.execution_id, o.payload, o.occurred_at,"
        f" o.correlation_id, o.seq FROM {table} o"
        " WHERE o.relayed_at IS NULL AND (o.relay_next_at IS NULL OR o.relay_next_at <= now())"
        f" AND NOT EXISTS (SELECT 1 FROM {table} f WHERE f.workspace_id = o.workspace_id"
        " AND f.subject = o.subject AND f.relayed_at IS NULL AND f.relay_next_at > now()"
        " AND f.seq < o.seq)"
        " ORDER BY o.seq LIMIT %s"
    )


def relay_once(
    conn: psycopg.Connection[Any],
    cell: Cell,
    publisher: Publisher,
    batch_size: int = 100,
    time_budget: float = BATCH_TIME_BUDGET_SECONDS,
    wait: float = 0,
) -> RelayResult:
    """Publish one batch in one transaction that holds the relay lock throughout.

    ``wait`` = 0 (inline relays) gives up at once when another relay holds the lock. The hourly
    job waits up to ``wait`` seconds instead, so frequent inline relays of one workspace cannot
    make the sweep of every workspace skip its run (an inline relay holds the lock for seconds).
    """
    try:
        with conn.transaction():
            cur = conn.cursor(row_factory=dict_row)
            cur.execute(
                "SELECT set_config('idle_in_transaction_session_timeout', '60s', true)"
            )
            if not _lock(cur, cell, wait):
                return RelayResult(0, 0, skipped=True)
            return _batch(cur, cell, publisher, batch_size, time_budget)
    except psycopg.errors.LockNotAvailable:
        return RelayResult(0, 0, skipped=True)


def _lock(cur: psycopg.Cursor[Any], cell: Cell, wait: float) -> bool:
    if wait <= 0:
        row = cur.execute(
            "SELECT pg_try_advisory_xact_lock(hashtext(%s)) AS ok", (lock_key(cell),)
        ).fetchone()
        return bool(row and row["ok"])
    cur.execute(
        "SELECT set_config('lock_timeout', %s, true)", (f"{int(wait * 1000)}ms",)
    )
    cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (lock_key(cell),))
    cur.execute("SELECT set_config('lock_timeout', '0', true)")
    return True


def _batch(
    cur: psycopg.Cursor[Any],
    cell: Cell,
    publisher: Publisher,
    batch_size: int,
    time_budget: float,
) -> RelayResult:
    schema = cell.schema
    sent = failed = 0
    rows = cur.execute(_select(schema), (batch_size,)).fetchall()
    blocked: set[tuple[str, str]] = set()
    deadline = time.monotonic() + time_budget
    for row in rows:
        if time.monotonic() >= deadline:
            break  # the rest wait for the next batch
        record = (str(row["workspace_id"]), str(row["subject"]))
        if record in blocked:
            continue  # an earlier event of this record failed in this run
        try:
            publisher.publish(TOPIC, str(row["subject"]), envelope(cell, row))
        except Exception as exc:  # noqa: BLE001 - recorded on the row (class only), retried after backoff
            cur.execute(
                f"UPDATE {schema}.outbox SET relay_attempts = relay_attempts + 1, relay_error = %s,"
                " relay_next_at = clock_timestamp() + interval '1 minute'"
                f" * power(2, least(relay_attempts, {MAX_BACKOFF_MINUTES_EXPONENT}))"
                " WHERE id = %s",
                (type(exc).__name__, row["id"]),
            )
            blocked.add(record)
            failed += 1
            continue
        cur.execute(
            f"UPDATE {schema}.outbox SET relayed_at = now(), relay_attempts = relay_attempts + 1,"
            " relay_error = NULL WHERE id = %s",
            (row["id"],),
        )
        sent += 1
    return RelayResult(sent, failed)


def oldest_unsent_seconds(conn: psycopg.Connection[Any], cell: Cell) -> float:
    row = conn.execute(
        "SELECT coalesce(extract(epoch FROM now() - min(occurred_at)), 0)"
        f" FROM {cell.schema}.outbox WHERE relayed_at IS NULL"
    ).fetchone()
    conn.commit()
    return float(row[0]) if row else 0.0


class KafkaPublisher:
    """confluent-kafka producer (kafka-python-ng cannot authenticate to Kafka 4.x brokers)."""

    def __init__(
        self, config: dict[str, Any], timeout: float = PUBLISH_TIMEOUT_SECONDS
    ) -> None:
        from confluent_kafka import Producer

        self.timeout = timeout
        self.producer = Producer({**config, "acks": "all", "enable.idempotence": True})

    def publish(self, topic: str, key: str, value: dict[str, Any]) -> None:
        result: dict[str, Any] = {}
        self.producer.produce(
            topic,
            key=key.encode(),
            value=json.dumps(value, separators=(",", ":"), default=str).encode(),
            on_delivery=lambda err, _message: result.setdefault("err", err),
        )
        deadline = time.monotonic() + self.timeout
        while "err" not in result and time.monotonic() < deadline:
            self.producer.poll(0.2)
        if "err" not in result:
            raise TimeoutError("broker did not acknowledge")
        if result["err"] is not None:
            raise RuntimeError(result["err"].name())


_PUBLISHERS: dict[str, KafkaPublisher] = {}
_KEYS = ("BOOTSTRAP", "CA_PEM", "USERNAME", "PASSWORD")


def configured(cell: Cell, environ: Mapping[str, str] = os.environ) -> bool:
    """True when every Kafka setting of this cell is present (cheap; builds nothing)."""
    return all(environ.get(f"{cell.env}_KAFKA_{key}") for key in _KEYS)


def publisher_from_env(
    cell: Cell, environ: Mapping[str, str] = os.environ
) -> Publisher | None:
    """The cell's Kafka producer (one per process), or None when the settings are missing."""
    prefix = f"{cell.env}_KAFKA_"
    if not configured(cell, environ):
        return None
    if cell.name not in _PUBLISHERS:
        with tempfile.NamedTemporaryFile(
            "w", delete=False, suffix=".pem", prefix="kafka-ca-"
        ) as ca:
            ca.write(environ[prefix + "CA_PEM"])
        _PUBLISHERS[cell.name] = KafkaPublisher(
            {
                "bootstrap.servers": environ[prefix + "BOOTSTRAP"],
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "SCRAM-SHA-256",
                "sasl.username": environ[prefix + "USERNAME"],
                "sasl.password": environ[prefix + "PASSWORD"],
                "ssl.ca.location": ca.name,
                "client.id": f"{cell.name}-relay",
            }
        )
    return _PUBLISHERS[cell.name]


def main(argv: list[str] | None = None) -> int:
    """``<cell>-relay``: relay until the outbox is drained or the time budget is spent; counts only."""
    del argv
    cell = load(Path(os.environ.get("CELL_APP_ROOT", ".")) / "cell.yaml")
    url = os.environ.get(f"{cell.env}_RELAY_DATABASE_URL")
    publisher = publisher_from_env(cell)
    if not url or publisher is None:
        print(json.dumps({"error": "missing configuration"}))
        return 2
    totals = {"sent": 0, "failed": 0, "skipped": False}
    try:
        with psycopg.connect(url, connect_timeout=15) as conn:
            started = time.monotonic()
            while time.monotonic() - started < BATCH_TIME_BUDGET_SECONDS:
                result = relay_once(conn, cell, publisher, BATCH_SIZE, wait=30)
                totals["sent"] += result.sent
                totals["failed"] += result.failed
                totals["skipped"] = result.skipped
                # A batch that failed entirely still moved on: failed rows wait out their backoff.
                if result.skipped or result.sent + result.failed == 0:
                    break
            backlog = oldest_unsent_seconds(conn, cell)
            refs_read = _catch_up_references(conn, cell)
    except psycopg.Error as exc:
        print(json.dumps({"error": type(exc).__name__}))
        return 1
    print(
        json.dumps(
            {**totals, "oldest_unsent_seconds": round(backlog), "refs_read": refs_read}
        )
    )
    return 1 if totals["failed"] else 0


def _catch_up_references(conn: psycopg.Connection[Any], cell: Cell) -> int:
    """SPEC-006: the hourly job also keeps the reference tables fresh (same publisher role)."""
    from cellkit import refs

    if not refs.has_references(cell):
        return 0
    reader = refs.reader_from_env(cell)
    if reader is None:
        return 0
    return refs.catch_up(conn, cell, reader, seconds=60)


def run() -> None:
    sys.exit(main(sys.argv[1:]))
