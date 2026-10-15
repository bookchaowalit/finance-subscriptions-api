# finance-subscriptions-api
The Subscriptions cell: its own API (Railway, serverless) and its own Neon
database, first generated from `cell.yaml` (one of the ten finance
services split on 2026-10-12). It
reuses empire-platform for identity, authorization, executions and audit over
HTTP with the caller's token.

| | |
|---|---|
| API | Railway project `finance-subscriptions-api`, service `subscriptions-api` (Serverless) |
| Data | Neon project `finance-subscriptions-api`, database `subscriptions` |
| Registry | solo-empire `repository-catalog/registries/online-systems.yaml` (`subscriptions`) |

## API

Every resource has list, stats, get, get-by-legacy-id, create, patch and delete
(see `cellkit/app.py`). Permissions are `subscriptions.<resource>:read|write|delete`.

| Path | Resource | Filters |
|---|---|---|
| `/v1/subscriptions` | subscription | `status`, `owner_scope`, `provider`, `billing_interval`, `category` |
| `/v1/subscription-reviews` | subscription-review | `subscription_id`, `verdict` |
| `/v1/subscription-aliases` | subscription-alias | `subscription_id`, `mapping_status` |
| `/v1/subscription-usages` | subscription-usage | `subscription_id`, `outcome` |
| `/v1/subscription-charges` | subscription-charge | `subscription_id` |

### AI subscription evidence

Providers are free text, including a custom provider; `category=ai` selects AI
plans. The subscription amount describes the plan. Actual money paid belongs in
`subscription-charges`: `paid_on`, inclusive `period_start` / `period_end`,
`amount`, `currency`, and a required receipt or source reference in `evidence`.
An end date before the start date is rejected with HTTP 422 on create and patch
and by a database constraint. Charge lists default to `paid_on` order.

`subscription-usages` records a dated work item (`used_on`, `title`), its
`outcome` (`completed`, `partial`, `discarded`), optional `baseline_minutes`
(manual estimate without AI), optional `actual_minutes` (including waiting),
and `rework_minutes` (extra verification or repairs, default 0). Unknown baseline
or actual time stays null. A completed item can lose time: retain the signed
result of baseline minus actual minus rework when reporting. Partial and
discarded work do not earn saved baseline time. Usage lists default to `used_on`
order. Both resources accept optional `source` (default `manual`) and
`source_ref`; usage also accepts `task_ref` and `evidence`.

New resources have the usual workspace isolation and
`subscriptions.subscription-usage:read|write|delete` /
`subscriptions.subscription-charge:read|write|delete` permissions. Send an
`Idempotency-Key` header (8–200 characters) to retry a create without duplicating
the row; source references are evidence, not global deduplication keys.

A review defaults `usage_complete` to false. Set it true only when the owner
states that logs cover its exact inclusive review period; it is not inferred
from the number of logged items. Optional `hourly_value` and `value_currency`
record the owner's time valuation. Reports must keep currencies separate,
validate uppercase currency input, and use a valuation currency matching the
subscription. These records preserve evidence; totals and value judgments are
computed by the client. Existing review fields and records remain compatible.

Schema changes are in append-only migration
`migrations/0003_subscription_usage_and_charges.sql`. Apply it before running the
updated API, and refresh platform permissions from the expanded cell spec before
authorizing new resource access. No production migration is run by tests.

## Develop and operate

```bash
uv venv .venv && uv pip install --python .venv/bin/python -e ".[dev]"
.venv/bin/pytest -q
NEON_API_KEY=... .venv/bin/subscriptions-db provision
npm install && railway config apply && railway up --ci --service subscriptions-api
railway scale --service subscriptions-api asia-southeast1-eqsg3a=1 us-east4-eqdc4a=0   # next to Neon and the platform
```

Railway variables: `SUBSCRIPTIONS_DB_MODE=hosted`, `SUBSCRIPTIONS_DB_URL_API`,
`SUBSCRIPTIONS_JWT_PUBLIC_KEY_PEM`, `SUBSCRIPTIONS_JWT_ISSUER`, `EMPIRE_PLATFORM_URL`.
