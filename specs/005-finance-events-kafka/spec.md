# SPEC-005 — Finance events: outbox → Kafka `finance.events.v1`

Standard: ADR-0004 — TDD + SDD. Messaging: ADR-0008 (architecture-docs `messaging/`: envelope 1.0.0, outbox, relay).
Phase 2 of the finance SaaS plan (2026-11-11). The same change lands in all ten finance services.
(SPEC-004 is taken by the identity-issuer work on a branch of finance-transactions-api.)

## Document control

| Field | Value |
|---|---|
| Spec ID / version | SPEC-005 / 1.3 |
| Status | approved (2026-11-11, see Approval) |
| Repository / path | bookchaowalit/finance-subscriptions-api / `specs/005-finance-events-kafka/spec.md` (copy of the reference in finance-budgets-api) |
| Owner (maker) | Claude Code (implementer session, 2026-11-11) |
| Reviewer (checker) | Codex review Stop hook + CI gates |
| Approver | owner, in chat 2026-11-11 ("ใช้แบบนี้ เริ่มเลย") |
| Main-only | yes |

## 1. Problem and intent

- **Problem:** the finance services will be combined into SaaS products, so each one must tell the others
  what changed (an account closed, a transaction recorded). No finance cell writes an event today: the
  kit's outbox exists, but no resource declares `graph` fields, and nothing delivers the outbox anywhere.
- **Outcome:** every create, update and delete writes one outbox row in the write's transaction; a relay
  publishes it to the Kafka topic `finance.events.v1` in the ADR-0008 envelope, right after the write
  (best effort) and in an hourly sweep that retries with backoff. The lake sink lands the topic in Bronze.
- **Why now:** phase 3 (cross-service reference checks) and phase 4 (read models) consume these events.

## 2. Scope

| In scope | Out of scope |
|---|---|
| `graph` fields per resource (reference data only); migration adding relay columns and grants; `src/cellkit/relay.py` (`relay_once`, `KafkaPublisher`, `<cell>-relay` CLI); inline relay after writes; `.github/workflows/relay-outbox.yml` (hourly via platform-jobs-worker) with heartbeat `finance-relay-<cell>`; Aiven topic, one producer user per cell, lake-sink read ACL; event-sink topic list | Consumers that act on the events (phase 3/4); the platform graph publisher (unchanged, still unused); amounts or free text in events |

## 3. Behaviour

| ID | Given / When / Then |
|---|---|
| AC-01 | **Given** any resource, **when** a record is created, updated or deleted, **then** exactly one outbox row is written in the same transaction (`type` `<cell>.<resource>.<created|updated|deleted>`, subject `empire:<cell>:<resource>:<id>`). A refused or rolled-back write leaves none. |
| AC-02 | **Given** a resource's `graph` fields, **then** they are reference data only: enums, refs and `*_id` numbers, `scope` and `currency`. No `money`/`decimal` amount, no free text (`name`, `notes`, `description`, `institution`, …), no `json`. |
| AC-03 | **Given** pending outbox rows, **when** the relay runs, **then** each is published to `finance.events.v1` with key = subject and an envelope 1.0.0 (`id` = outbox id, `type`, `version` 1, `occurred_at`, `producer` = cell, `correlation_id`, `causation_id` = execution id, `data` = workspace id, record id, subject, graph fields, `deleted`), and marked relayed after the broker ack. |
| AC-04 | **Given** a publish failure, **then** the row records only the error class, waits 1, 2, 4 … 64 minutes, and later rows of the same record wait behind it. A record is (workspace, subject), and its events go out in write order (`relay_seq`), also across a delete and a re-import of the same legacy id. The hourly job keeps going after a batch of failures only. |
| AC-05 | **Given** two relays at once, **then** only one works (advisory lock); the other reports `skipped`. |
| AC-06 | **Given** a write through the API and a configured Kafka producer, **then** the API relays its own workspace's rows in a background task; a relay error never fails the request. Without Kafka settings nothing changes. |
| AC-07 | **Given** the hourly run, **then** the job prints counts only (sent, failed, skipped, oldest unsent seconds) and posts heartbeat `finance-relay-<cell>`; a failure or a backlog older than 3 h marks it not ok. |
| AC-08 | **Given** the relay database role (`<cell>_publisher`), **then** it may read the outbox and update only the relay columns; it cannot read business tables. |

## 4. Interfaces and data

| Interface | Change |
|---|---|
| Migration `*_finance_events.sql` | `outbox` + `relayed_at`, `relay_attempts`, `relay_next_at`, `relay_error`; pending index; `UPDATE` on those columns to `<cell>_api` and `<cell>_publisher` |
| Migration `0005_relay_order.sql` | `outbox.relay_seq` (identity, taken at insert; existing rows numbered in the old order); pending index on `relay_seq` |
| Kafka | topic `finance.events.v1` (2 partitions); SASL user `finance-<cell>-producer` (write); `lake-sink` read |
| Env (API, Railway) | optional `<CELL>_KAFKA_BOOTSTRAP`, `_KAFKA_CA_PEM`, `_KAFKA_USERNAME`, `_KAFKA_PASSWORD` |
| Env (relay job, GitHub secrets) | the same four plus `<CELL>_RELAY_DATABASE_URL`, `HEARTBEAT_TOKEN` |

## 5. Non-functional requirements

| NFR | Target | How it is tested |
|---|---|---|
| Privacy | no amounts, names or notes leave the cell in an event | AC-02 test over this cell's spec |
| Reliability | at-least-once, ordered per record; consumers dedupe on envelope `id` | relay tests (fake publisher) |
| Cost | the hourly sweep wakes Neon once per hour; no always-on compute | jobs schedule |
| Security | the relay role cannot read business rows; public CI logs print counts only | AC-08 test; workflow review |

## 6. Test plan

- Red first: `tests/test_finance_events.py` fails before `src/` and `cell.yaml` change.
- Levels: integration against the throwaway PostgreSQL as the least-privilege roles (api, publisher); a fake publisher for the relay; one live check after deploy (an event on the topic, read back by `lake-sink`).
- Red/green log (this repository, 2026-11-11): red `.venv/bin/pytest -q tests/test_finance_events.py` before the `src/` and `cell.yaml` change: 12 failed, 5 passed; green `.venv/bin/pytest -q --cov`: 325 passed, 8 sk, total coverage 94.56% (CI gate 91); `mypy`, `ruff check` and `ruff format --check` clean.

## 7. Risks and open questions

| # | Risk | Impact | Resolution |
|---|---|---|---|
| 1 | Aiven free tier limits (users, topics) | relay cannot authenticate | checked before creating users |
| 2 | `confluent-kafka` wheel in the slim image and the blocking Trivy scan | build or scan fails | CI image job shows it before deploy |

## 8. Change log

| Version | Date | Change | Approved by |
|---|---|---|---|
| 1.0 | 2026-11-11 | finance events to Kafka | owner, chat 2026-11-11 |
| 1.1 | 2026-11-11 | Codex review: the inline relay builds the producer inside its guarded task (bad Kafka settings no longer fail the write); a record's events go out in record-version order, not transaction-start order. Red: `test_a_broken_producer_setup_never_fails_the_write`, `test_a_record_is_relayed_in_version_order`; green: 327 passed, 8 skipped, 1 xfailed  | owner, chat 2026-11-11 |
| 1.2 | 2026-11-11 | Security review: the hourly relay waits up to 30 s for the relay lock (`wait=30`, `lock_timeout`) instead of skipping, so frequent inline relays of one workspace cannot starve the sweep of every workspace. Red: `test_the_hourly_relay_waits_for_an_inline_relay_instead_of_skipping`; green: 356 passed, 8 skipped, 1 xfailed  | owner, chat 2026-11-11 |
| 1.3 | 2026-11-11 | Codex review round 3: (1) backoff and the in-run block are per record = (workspace, subject), not per subject (two workspaces may import the same legacy id); (2) a record's events go out in write order: new `relay_seq` (migration `0005_relay_order.sql`) instead of record version, which restarts at 1 when a deleted legacy id is imported again; (3) the hourly job stops only when a batch found nothing (sent + failed = 0), not after a batch of failures only. Red (old code, new tests): `test_a_failure_in_one_workspace_does_not_block_the_same_subject_in_another`, `test_a_re_created_legacy_id_keeps_its_events_in_write_order`, `test_the_relay_job_goes_on_after_a_batch_of_failures_only` failed; `test_a_record_is_relayed_in_version_order` renamed `..._in_write_order` (rows inserted in lock order). Green: 360 passed, 8 skipped, 1 xfailed; mypy, ruff check and format clean. Follow-up (same day, Codex P1): the migration lifts FORCE RLS around the backfill and `setval` (the migrator has no BYPASSRLS, so it saw no rows); red `test_the_relay_order_migration_numbers_every_workspaces_rows_as_the_migrator`, then green with one more passing test | owner, chat 2026-11-11 (review fixes inside the approved scope) |

## Approval

| Gate | Role | Name / date | Decision |
|---|---|---|---|
| Approved to implement | Approver | Owner, chat 2026-11-11 ("ใช้แบบนี้ เริ่มเลย") | approved |
