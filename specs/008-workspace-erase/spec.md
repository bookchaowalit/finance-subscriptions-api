# SPEC-008 — Erase a workspace's records (PDPA)

Standard: ADR-0004 — TDD + SDD. Phase 6 (SaaS layer) of the finance SaaS plan (2026-11-11). The same cellkit
change lands in all ten finance services; finance-api SPEC-004 drives it across the suite.

## Document control

| Field | Value |
|---|---|
| Spec ID / version | SPEC-008 / 1.0 |
| Status | approved (2026-11-11, see Approval) |
| Repository / path | bookchaowalit/finance-subscriptions-api / `specs/008-workspace-erase/spec.md` (same text in the other nine finance services) |
| Owner (maker) | Claude Code (session "ระบบบัญชีหลังบ้าน", 2026-11-11) |
| Reviewer (checker) | independent reviewer subagent + Codex review Stop hook + CI gates |
| Approver | owner, in chat 2026-11-11 ("ทำ phase 6 ต่อเลย"; owner approved "all recommendations" for the finance SaaS plan) |
| Main-only | yes |

## 1. Problem and intent

- **Problem:** a customer who leaves (PDPA section 33, right to erasure) must be able to have their financial data
  deleted. Today a cell deletes one record per request; a workspace with thousands of rows would need thousands of
  calls, each one a platform execution, and a 300-second bundle token cannot cover that.
- **Outcome:** `POST /v1/<plural>/erase` deletes the caller's own records of one resource in bounded batches, keeps
  every rule a single delete keeps (permission, references, events, audit) and says how many are left.

## 2. Scope

| In scope | Out of scope |
|---|---|
| One erase route per resource; batch size; refusing rows that are still referenced (same cell or another cell); `deleted` events per row; one platform execution per batch | The order across resources and cells (finance-api SPEC-004); `create_keys` rows (idempotency keys, no customer content; a migration would be needed to grant DELETE); relayed outbox rows, Kafka, the R2 lake and backups (reference data only, no amounts/names/notes — retention is documented in finance-api SPEC-004); an MCP tool (erasing is not an agent action) |

## 3. Behaviour

| ID | Given / When / Then |
|---|---|
| AC-01 | **Given** a caller with `<cell>.<resource>:delete`, **when** they `POST /v1/<plural>/erase` with `{"confirm": "<their workspace id>"}`, **then** up to `limit` (default 200, 1–500) of their workspace's records are deleted in one transaction and the answer is 200 `{"erased": n, "remaining": m}`; `m` counts the records of that workspace still there. |
| AC-02 | **Given** `confirm` is not a UUID or not the caller's workspace (`ws` claim), **then** 422 `{"error": "invalid_value", "fields": ["confirm"]}` and nothing is deleted. A missing `confirm`, an unknown body field or a `limit` outside 1–500 is a validation error (422). |
| AC-03 | **Given** a caller without `:delete` on the resource, **then** 403, as for a single delete; no token → 401; the platform down → 503. |
| AC-04 | **Given** a record that another record of the same cell points at (a `ref` field, including a self reference from another row), **then** it is not erased in this batch and counts in `remaining`; once the pointing records are gone, a later call erases it. |
| AC-05 | **Given** a resource with `referenced_by` (SPEC-006), **then** the cell first catches up on references (503 `references_unavailable` when it cannot) and a record that a live record of another cell points at is not erased and counts in `remaining`. |
| AC-06 | **Given** a resource with graph events, **then** every erased record writes a `<cell>.<resource>.deleted` outbox event (same payload as a single delete), so other cells and the lake learn of it; the inline publish and relay run as after any write. |
| AC-07 | **Given** a batch that erased records, **then** it ran as one platform execution with permission `<cell>.<resource>:delete`, resource type `<resource>`, resource id `erase`, action `<cell>.<resource>.erased` and `before_hash` = SHA-256 of the erased ids; a batch that erased nothing makes no execution. |
| AC-08 | **Given** another workspace's records, **then** they are never read, counted or deleted (RLS, as everywhere). |
| AC-09 | **Given** two erase calls at the same time, **then** neither waits on rows the other has locked (`SKIP LOCKED`); repeated calls finish the job. |

## 4. Design

- Route registered next to the single delete in `cellkit/app.py`; it reuses `write()` (precheck, execution,
  commit, finish) with the precheck `(<cell>.<resource>:delete, "erase")`.
- Candidate rows: `SELECT id … ORDER BY id LIMIT n FOR UPDATE SKIP LOCKED` with one `NOT EXISTS` per local ref
  field that points at the resource and, for `referenced_by` resources, `NOT EXISTS` on live `ref_sources` rows.
- The rows are loaded (for the event payload) and deleted with `id = ANY(...)`; `remaining` is counted after.

## 5. Verification

- Contract test `test_erase_empties_the_workspace` in `cellkit/contract.py` (runs for every resource of every
  cell): AC-01, 02, 03, 06, 07, 08; local children (AC-04) when the cell has them; cross-cell dependents (AC-05)
  through a `ref_sources` row when the resource has `referenced_by`.
- Red: the test fails with 405 before the route exists. Green: the full suite of each of the ten cells.

## 6. Approval

- 2026-11-11 owner: "ทำ phase 6 ต่อเลย" — phase 6 = provisioning, billing, PDPA export/delete, audit; erasing is
  the PDPA delete part.
