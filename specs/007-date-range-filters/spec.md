# SPEC-007 — Date-range filters on list and stats

Standard: ADR-0004 — TDD + SDD. Phase 5 (ops) of the finance SaaS plan (2026-11-11). The same cellkit change
lands in all ten finance services.

## Document control

| Field | Value |
|---|---|
| Spec ID / version | SPEC-007 / 1.0 |
| Status | approved (2026-11-11, see Approval) |
| Repository / path | bookchaowalit/finance-subscriptions-api / `specs/007-date-range-filters/spec.md` (same text in the other nine finance services) |
| Owner (maker) | Claude Code (session "ระบบบัญชีหลังบ้าน", 2026-11-11) |
| Reviewer (checker) | Codex review Stop hook + CI gates |
| Approver | owner, in chat 2026-11-11 ("ทำ date-range filter ต่อเลย") |
| Main-only | yes |

## 1. Problem and intent

- **Problem:** a list can filter by equality only. finance-api (`/v1/overview`, `/v1/budget-vs-actual`) reads every
  transaction and plan of a workspace (up to 10 000 rows) and drops the ones outside the month in Python.
- **Outcome:** every `date` or `timestamp` field can be bounded on the server: `<field>_from` and `<field>_to`
  on `GET /v1/<plural>` and `GET /v1/<plural>/stats`, so a caller reads only the period it needs.

## 2. Scope

| In scope | Out of scope |
|---|---|
| `<field>_from` / `<field>_to` for each `date`/`timestamp` field of a resource; list, stats and the MCP list tool; spec check against name clashes | `created_at` ranges (`updated_since` stays the only standard-column bound); indexes (the tables are small; add one per cell when a plan shows a scan); switching finance-api to the new parameters (its own change) |

## 3. Behaviour

| ID | Given / When / Then |
|---|---|
| AC-01 | **Given** a `date` field `d`, **when** a list sends `d_from=A`, **then** only rows with `d >= A` are returned; `d_to=B` returns only `d <= B`. Both bounds are inclusive and can be combined with each other and with every other filter. |
| AC-02 | **Given** a row whose `d` is null, **when** any bound on `d` is sent, **then** that row is not returned. |
| AC-03 | **Given** a `timestamp` field, **then** the bounds are instants (ISO 8601, `Z` or an offset; no offset = UTC); a date-only value means 00:00 UTC of that day. Send `+` as `%2B` (a raw `+` decodes to a space and is refused, 422); `Z` needs no encoding. An empty bound (`d_from=`) is ignored, like `updated_since`. |
| AC-04 | **Given** a bound that does not parse as the field's type, **then** 422 `{"error": "invalid_value", "fields": ["<name>"]}`. |
| AC-05 | **Given** `d_from` after `d_to`, **then** 422 `{"error": "invalid_value", "fields": ["d_from", "d_to"]}`. |
| AC-06 | **Given** a bound on a field that is not `date`/`timestamp` (e.g. `name_from`), **then** 422 `unknown_filter`, as for any unknown parameter. |
| AC-07 | **Given** `GET /v1/<plural>/stats`, **then** the same bounds narrow the counted rows. |
| AC-08 | **Given** the MCP `list` tool, **then** it takes the same optional bounds and passes them through. |
| AC-09 | **Given** a `cell.yaml` field whose name equals a generated bound name (`<date field>_from`/`_to`), **then** the spec does not load (`SpecError`). |

## 4. Interfaces and data

| Interface | Change |
|---|---|
| `GET /v1/<plural>`, `GET /v1/<plural>/stats` | + optional `<field>_from`, `<field>_to` per date/timestamp field |
| `cellkit.spec.Resource.bounds` | new: `{param: (field, ">=" or "<=")}` |
| MCP `<cell>.<resource>.list` | + the same optional arguments |
| Database | none (no migration) |

## 5. Non-functional requirements

| NFR | Target | How it is tested |
|---|---|---|
| Injection | bound names come from the spec, values are `%s` parameters | code review; unknown names are refused before SQL is built |
| Compatibility | every existing query keeps its result | full test suite unchanged and green in all ten services |
| Byte-identical kit | `src/cellkit` is the same in the ten services | `sha256sum src/cellkit/*.py` compared before push |

## 6. Tests (red → green)

Contract test `test_date_bounds_narrow_the_list` (AC-01, 03–05, every date/timestamp field of this cell);
the full AC set incl. MCP (AC-08) and spec clash (AC-09) is tested in finance-accounts-api;
the other nine run their whole suites against the same kit.

## 7. Rollout and rollback

Merge to `main` deploys each service (Railway GitHub source + Wait for CI). Check `/health` and one bounded read per
service. Rollback: revert the commit; no data changed.

## Approval

Owner in chat 2026-11-11: "ทำ date-range filter ต่อเลย" (after the phase 5 review in the same session).
