# SPEC-006 — Cross-service references between the finance cells

Standard: ADR-0004 — TDD + SDD. Builds on SPEC-005 (events on Kafka `finance.events.v1`).
Phase 3 of the finance SaaS plan (2026-11-11). The same change lands in all ten finance services.

## Document control

| Field | Value |
|---|---|
| Spec ID / version | SPEC-006 / 1.1 |
| Status | approved (2026-11-11, see Approval) |
| Repository / path | bookchaowalit/finance-subscriptions-api / `specs/006-cross-service-references/spec.md` (copy of the reference in finance-transactions-api) |
| Owner (maker) | Claude Code (implementer session, 2026-11-11) |
| Reviewer (checker) | Codex review Stop hook + CI gates |
| Approver | owner, in chat 2026-11-11 ("เริ่มระยะ 3"; plan approved earlier: event-fed local id tables, close-not-delete) |
| Main-only | yes |

## 1. Problem and intent

- **Problem:** `account_id` (transactions, investments, liabilities, reconciliations, subscriptions) and
  `subscription_id` (transactions) are plain numbers. A record can point at an account that never existed,
  belongs to another workspace, is closed, or was deleted; deleting an account leaves its records dangling.
- **Outcome:** a write that sets such a field is refused unless the target exists in the caller's workspace
  and is not deleted or closed; a referenced account or subscription cannot be deleted (close it instead).
- **How, without synchronous calls between services:** each cell keeps a local copy of the ids it points at
  and of the records that point at it, fed by the SPEC-005 events. On a miss the cell first catches up from
  Kafka (seconds), then decides. Every cell also catches up in its hourly relay job.

## 2. Scope

| In scope | Out of scope |
|---|---|
| `references: <cell>.<resource>` (+ `refuse_status`) on bigint fields; `referenced_by` on resources; tables `ref_targets`, `ref_sources`, `ref_offsets`; `src/cellkit/refs.py` (catch-up, checks); snapshot events for rows written before SPEC-005; Kafka read ACLs; `<CELL>_REFS_DATABASE_URL` (publisher role) | `project_id`, `goal_id` (systems outside finance: unchecked, documented); cascading closes; a UI |

## 3. Behaviour

| ID | Given / When / Then |
|---|---|
| AC-01 | **Given** a field with `references`, **when** a create or a patch sets it to an id that is unknown in the caller's workspace, **then** 422 `{"error": "reference_not_found", "fields": [..]}` and nothing is written. |
| AC-02 | **Given** the target was deleted, **then** the same 422; **given** its `status` is in `refuse_status` (account `closed`; subscription `cancelled`, `expired`), **then** 422 `reference_closed`. A patch that leaves the field unchanged is not checked again. |
| AC-03 | **Given** a miss, **then** the cell catches up from Kafka once (bounded, a few seconds) before refusing; a target created moments ago in the other service is accepted. |
| AC-04 | **Given** the reference data cannot be read (no Kafka settings, broker down) and the id is not already known, **then** 503 `references_unavailable` (fail closed). |
| AC-05 | **Given** a resource with `referenced_by`, **when** a record still referenced by a live record of another cell is deleted, **then** 409 `has_dependents`; once those records are deleted or point elsewhere, the delete succeeds. |
| AC-06 | **Given** events of another workspace, **then** they never satisfy a check for this workspace (tables are keyed and row-level-secured by workspace). |
| AC-07 | **Given** events arrive twice or out of order, **then** the stored state follows the highest record version (`data.record_version`). |
| AC-08 | **Given** rows that existed before SPEC-005, **then** the migration writes one `<cell>.<resource>.snapshot` event per row, so other cells learn about them. |

## 4. Interfaces and data

| Interface | Change |
|---|---|
| `cell.yaml` | `account_id: {type: bigint, references: finance-accounts.account, refuse_status: [closed]}`; `subscription_id` (transactions) → `subscriptions.subscription`, refuse `cancelled`, `expired`; `referenced_by` on `finance-accounts.account` and `subscriptions.subscription` |
| Migration `*_cross_references.sql` | `ref_targets`, `ref_sources` (RLS by workspace; the API reads, the publisher role writes), `ref_offsets`; snapshot outbox rows |
| Envelope `data` | + `record_version` (the record's version) |
| Env | `<CELL>_REFS_DATABASE_URL` (publisher role) on Railway; the hourly job reuses `<CELL>_RELAY_DATABASE_URL` |
| Kafka | `read` on `finance.events.v1` for every `finance-<cell>-producer` user (offsets live in `ref_offsets`, not a consumer group) |

## 5. Non-functional requirements

| NFR | Target | How it is tested |
|---|---|---|
| Isolation | one workspace's events never vouch for another's | AC-06 test as the API role |
| Latency | a hit costs one indexed lookup; a miss at most one bounded catch-up | design; catch-up deadline 5 s |
| Reliability | at-least-once input, idempotent by version; offsets stored with the rows in one transaction | AC-07 test |

## 6. Test plan

- Red first: `tests/test_cross_references.py` fails before the `src/` and `cell.yaml` change.
- Levels: integration on the throwaway PostgreSQL with a fake Kafka reader; one live check after deploy.
- Red/green log (this repository, 2026-11-11): red `.venv/bin/pytest -q tests/test_cross_references.py tests/test_finance_events.py` before the `src/` and `cell.yaml` change: 13 failed, 17 passed, 13 errors; green `.venv/bin/pytest -q --cov`: 379 passed, 8 skipped, 1 xfailed, total coverage 93.67% (CI gate 91); `mypy`, `ruff check` and `ruff format --check` clean.

## 7. Risks

| # | Risk | Resolution |
|---|---|---|
| 1 | Eventual consistency: a target created in the other service is unknown until its event is relayed | inline relay right after the write + catch-up on a miss; 503 rather than a wrong yes |
| 2 | Clients that wrote arbitrary account ids before | data is near empty (2026-11-11); existing rows are not re-checked |
| 3 | A compromised finance cell can still publish events in another cell's name (one shared topic, no signature) | accepted for now: the Kafka ACL limits writers to the ten producers; per-producer topics or signed envelopes when a topic slot or a signing key is available |

## 8. Change log

| Version | Date | Change | Approved by |
|---|---|---|---|
| 1.0 | 2026-11-11 | cross-service references | owner, chat 2026-11-11 |
| 1.1 | 2026-11-11 | Codex + security review before release: state per record follows the producer's outbox `seq` (`data.event_seq`), so a redelivered old `created` never revives a deleted target and a re-created legacy id still counts; the reader starts at the low watermark (expired messages); reader and connection setup errors become 503; the harness drops real Kafka/database settings; each message is applied in its own savepoint (a malformed or out-of-range message is skipped, the position moves on); only the producer named in the type may speak for that kind | owner, chat 2026-11-11 |

## Approval

| Gate | Role | Name / date | Decision |
|---|---|---|---|
| Approved to implement | Approver | Owner, chat 2026-11-11 ("เริ่มระยะ 3") | approved |
