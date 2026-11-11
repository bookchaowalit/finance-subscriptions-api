# SPEC-003 — SaaS data foundation: exact money, ISO 4217 currency, workspace-defined scope

Standard: ADR-0004 — TDD + SDD (architecture-docs, `docs/architecture/decisions/ADR-0004-tdd-sdd-standard.md`).
Phase 1 of the finance SaaS plan (2026-11-11). The same change lands in all ten finance services.

## Document control

| Field | Value |
|---|---|
| Spec ID / version | SPEC-003 / 1.3 |
| Status | approved (2026-11-11, see Approval) |
| Repository / path | bookchaowalit/finance-subscriptions-api / `specs/003-saas-money-currency-scope/spec.md` (copy of the reference in finance-budgets-api) |
| Owner (maker) | Claude Code (implementer session, 2026-11-11) |
| Reviewer (checker) | Codex review Stop hook + CI gates |
| Approver | owner, in chat 2026-11-11 ("ใช้ตามที่แนะนำทุกข้อ เริ่มระยะ 1 เลย") |
| Main-only | yes |
| Created / approved | 2026-11-11 / 2026-11-11 |

## 1. Problem and intent

- **Problem:** the finance services are to be sold as SaaS building blocks that call each other. Today
  every amount is a `real` (`double precision`), so sums and comparisons drift (`0.1 + 0.2`); currency is
  free text up to 12 characters (`THB`, `thb`, `baht` all pass); and `owner_scope` is an enum of the
  owner's own life areas (`personal`, `solo_empire`, `venture`, `client`, `shared`) that means nothing to
  a customer workspace.
- **Outcome:** money is exact end to end (database `numeric`, Python `Decimal`, JSON string); currency is
  an ISO 4217 alpha-3 code; `owner_scope` becomes `scope`, a free label each workspace defines.
- **Why now:** the hosted databases are almost empty (accounts: 2 rows, the rest 0 on 2026-11-11), so the
  type change needs no data migration work for customers.

## 2. Scope

| In scope | Out of scope (non-goals) |
|---|---|
| New field types `money`, `decimal`, `currency` in `src/cellkit` (spec, SQL, API models, query filters, import, MCP, test samples, contract tests); `cell.yaml` switched over; migration `0002`; `/health` returns the deployed commit | Multi-currency arithmetic or FX rates; one-currency-per-workspace enforcement; events (phase 2); cross-service reference checks (phase 3); renaming other fields |

## 3. Behaviour

| ID | Given / When / Then |
|---|---|
| AC-01 | **Given** a `money` field, **when** a client sends `"12345.6789"` (string) or `12345.6789` (number), **then** the record stores `12345.6789` exactly and every response returns the JSON string `"12345.6789"`. |
| AC-02 | **Given** a `money` field, **when** the value has more than 4 decimal places, more than 15 integer digits, or is `NaN`/`Infinity`, **then** the API answers 422 and writes nothing. |
| AC-03 | **Given** a `decimal` field (quantities, unit prices, rates), **then** it behaves as AC-01/AC-02 with 10 decimal places and 20 integer digits. |
| AC-04 | **Given** a `money` or `decimal` field with `min`/`max`, **then** bounds are checked as decimals (422 outside). |
| AC-05 | **Given** a `currency` field, **when** the value is not three upper-case letters (`thb`, `BAHT`, `US`, `""`), **then** 422; `USD` and `THB` are accepted; the database refuses the same values (CHECK). |
| AC-06 | **Given** the old `owner_scope` field, **then** it is gone; `scope` is optional free text (≤ 64 characters, default `""`), filterable, and keeps the old value of existing rows. |
| AC-07 | **Given** migration `0002` applied on top of `0001`, **then** the schema (columns, types, nullability, defaults, CHECK and UNIQUE constraints, indexes) equals a fresh schema generated from the new `cell.yaml`, and existing rows keep their values. |
| AC-08 | **Given** a deploy from GitHub, **then** `GET /health` returns `{"status": "ok", "commit": "<RAILWAY_GIT_COMMIT_SHA>"}`. |

## 4. Interfaces and data

| Interface | Change | Contract |
|---|---|---|
| `cell.yaml` | amounts `real` → `money`; quantities, unit prices and rates `real` → `decimal`; hours and engagement stay `real`; `currency` fields → `currency`; `owner_scope` (enum) → `scope` (text) | breaking for API clients |
| SQL | `money` = `numeric(19,4)`, `decimal` = `numeric(30,10)`, `currency` = `text CHECK (col ~ '^[A-Z]{3}$')` | migration `0002`, append-only |
| HTTP API | money/decimal fields are JSON strings in responses; requests accept string or number; `owner_scope` → `scope` in bodies and filters | MCP tools follow the same models |

## 5. Non-functional requirements

| NFR | Target | How it is tested |
|---|---|---|
| Correctness | no binary float on the money path (DB, API, outbox payload) | contract tests AC-01..AC-05 |
| Compatibility | `0002` is safe on the hosted data (2 rows in accounts, 0 elsewhere) | AC-07 test with rows inserted under `0001` |
| Security / privacy | no change to auth, RLS or grants | existing contract tests |

## 6. Test plan

- **Red first:** `tests/test_saas_foundation.py` and the new contract checks fail before the `src/` change (log below).
- **Levels:** unit (spec parsing, SQL generation), integration against the throwaway PostgreSQL (contract tests, migration equivalence).
- **Red/green log (this repository, 2026-11-11):**
  - red: `.venv/bin/pytest -q tests/test_saas_foundation.py` before the `src/` and `cell.yaml` change: 6 failed (test_no_amount_is_a_binary_float test_every_currency_field_is_iso_4217 test_owner_scope_became_a_workspace_label test_money_decimal_and_currency_columns test_saas_migration_keeps_existing_rows test_health_reports_the_deployed_commit ).
  - green: `.venv/bin/pytest -q --cov`: 295 passed, 12 skipped, 1 xfailed; total coverage 92.16% (CI gate 91); `mypy` clean; `ruff check` and `ruff format --check` clean.

## 7. Risks and open questions

| # | Risk or question | Impact | Owner | Resolution / date |
|---|---|---|---|---|
| 1 | Clients that read amounts as numbers (`finance-portfolio-web`, MCP users) | wrong display or type errors | implementer | update known consumers in the same rollout |
| 2 | The old API code runs for a few minutes against the migrated schema (`owner_scope` renamed) | writes fail during the deploy window | owner | accepted: no customers yet; migrate right before the push |

## 8. Change log

| Version | Date | Change | Scenarios affected | Approved by |
|---|---|---|---|---|
| 1.0 | 2026-11-11 | money/decimal/currency types, scope label, migration 0002, health commit | AC-01..AC-08 | owner, chat 2026-11-11 |
| 1.1 | 2026-11-11 | Codex review fixes: JSON number literals are read exactly (`ExactRoute`, no binary float on the way in); numeric bounds built from text (the default 28-digit context rounded the `decimal` ceiling to 10^20); importer maps the old `owner_scope` column (`legacy_columns`); the new contract tests are in `__all__` (they had not run). Red: `test_imports_map_the_old_owner_scope_column`, `test_numeric_bounds_are_exact`, `test_money_is_exact` failed; green: 301 passed, 14 skipped, 1 xfailed, mypy and ruff clean | AC-01, AC-03, AC-06 | owner, chat 2026-11-11 (review fixes inside the approved scope) |
| 1.2 | 2026-11-11 | Codex review round 2: the `decimal` floor is built from text (negation rounded it to -10^20); the exact-number reader turns `InvalidOperation` into a 422; a plain binary float (MCP arguments) is accepted only while exact (at most 15 significant digits), else 422 asking for a string. Red: `test_numeric_bounds_are_exact`, `test_exact_reader_refuses_bad_numbers_as_validation_errors`; green: 302 passed, 14 skipped, 1 xfailed , mypy and ruff clean | AC-01..AC-04 | owner, chat 2026-11-11 |
| 1.3 | 2026-11-11 | Codex review round 3: a plain binary float counts significant digits after normalizing, so trailing zeros do not count (`1e14`, `999999999999999.0` are exact and accepted, also through the MCP argument model). Red: `test_trailing_zeros_of_a_float_are_not_significant_digits`; green: 392 passed, 8 skipped, 1 xfailed; mypy and ruff clean | AC-01..AC-04 | owner, chat 2026-11-11 (review fix inside the approved scope) |

## Approval

| Gate | Role | Name / date | Decision | Evidence |
|---|---|---|---|---|
| Approved to implement | Approver | Owner, chat 2026-11-11 | approved | finance SaaS plan, phase 1 |
