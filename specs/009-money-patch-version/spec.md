# SPEC-009 — A money PATCH names the version it changes

Standard: ADR-0004 — TDD + SDD. Security review 2026-11-11 (P3 "expected_version optional on money PATCH"). The
same cellkit change lands in all ten finance services.

## Document control

| Field | Value |
|---|---|
| Spec ID / version | SPEC-009 / 1.0 |
| Status | approved (2026-11-11, see Approval) |
| Repository / path | bookchaowalit/finance-subscriptions-api / `specs/009-money-patch-version/spec.md` (same text in the other nine finance services) |
| Owner (maker) | Claude Code (session "ความปลอดภัยของระบบ", 2026-11-11) |
| Reviewer (checker) | Codex review Stop hook + CI gates; finance SaaS session consulted on the shape |
| Approver | owner, in chat 2026-11-11 ("เฉพาะที่มีฟิลด์เงิน": require it only when a money field is sent) |
| Main-only | yes |

## 1. Problem and intent

- **Problem:** `PATCH /v1/<plural>/{id}` takes `expected_version` but does not need it. Two writers that both
  read version 3 and change an amount both succeed; the second silently overwrites the first (lost update) on
  money.
- **Outcome:** a PATCH that sends a `money` field must say which version it changes. Other PATCHes are unchanged,
  so clients that only edit names, categories or references keep working.

## 2. Scope

| In scope | Out of scope |
|---|---|
| PATCH of every resource with `money` fields; the 428 answer; the MCP `update` tool description | `decimal`/`real` fields (quantities, rates: not money); POST and DELETE; an `If-Match` header form |

## 3. Behaviour

| ID | Given / When / Then |
|---|---|
| AC-01 | **Given** a PATCH body with at least one `money` field and no `expected_version`, **then** 428 `{"error": "version_required", "fields": [<money fields sent>], "version": <current version>}` and nothing changes. |
| AC-02 | **Given** the same body with the current `expected_version`, **then** 200 as before; with a stale one, 409 `version_conflict` as before. |
| AC-03 | **Given** a PATCH body without `money` fields (with or without `expected_version`), **then** behaviour is unchanged. |
| AC-04 | **Given** a caller who may not write the record, **then** the answer is the usual 403/404 before any version is shown (the check runs after the write precheck, inside the locked load). |

## 4. Design

- `cellkit/app.py` `update()`: the money fields named in the body are computed from `cell.yaml` field types;
  inside the locked operation, when that set is not empty and `expected_version` is missing, raise 428.
- `cellkit/mcp.py`: the `update` tool says `expected_version` is required when an amount changes.
- Callers checked on 2026-11-11: the importer's second pass PATCHes only references (AC-03); life-dashboard
  `ai-subscription-actions.ts` sends `expected_version` and only `category`.

## 5. Verification

- Contract test `test_money_patch_needs_the_version` (every resource with a money field, every cell): AC-01–03.
- Red: 200 instead of 428 before the change. Green: the full suite of each of the ten cells.
