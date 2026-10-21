# Product

<!-- impeccable:product-schema 1 -->

## Platform

api

## Users

- The Solo Empire owner, through `life-dashboard-web` (the AI subscription value pages reach the
  cell through `EMPIRE_SUBSCRIPTIONS_API`), the cell's own MCP server (`subscriptions-mcp`) and
  the hosted tool hub (`platform-mcp-remote-worker`).
- Automations with an `empire-platform` token, within the permissions
  `subscriptions.subscription:read|write|delete`, `subscriptions.subscription-review:read|write|delete` and
  `subscriptions.subscription-alias:read|write|delete`.
- `finance-transactions-api` records payments with a `subscription_id` (a plain number, not a
  foreign key). The cell points at `account_id`, `project_id` and `goal_id` of other services the
  same way.
- Note: `life-dashboard-web` also calls `/v1/subscription-usages` and `/v1/subscription-charges`,
  which this cell does not provide [ต้องยืนยัน].

## Product Purpose

The register of recurring costs (software and other subscriptions), a periodic review of whether
each one is worth its price, and the aliases that map a statement line to a subscription.

Success: each recurring cost exists once per name, provider and owner scope; each review covers a
valid period and a known verdict; each statement alias maps to one subscription or is visibly
unresolved.

## Positioning

One capability, one service: its own API (Railway), its own database (Neon) with row-level
security per workspace, and authorization, executions and audit through `empire-platform`. It is
workspace-scoped, so the same service can serve other tenants without code changes.

## Operating Context

- Production: `https://subscriptions-api-production-b1d4.up.railway.app` (`/health`), deployed by
  merging to `main` after CI.
- Data starts empty (created 2026-10-12 from the retired `finance_*` tables' fields); imports keep
  old ids in `legacy_id`.
- Monitored by `platform-uptime-worker`; errors go to Sentry when `SENTRY_DSN` is set.

## Capabilities and Constraints

- `/v1/subscriptions`: list (filters `status`, `owner_scope`, `provider`, `billing_interval`),
  stats, get, get-by-legacy-id, create (with `Idempotency-Key`), patch (with `expected_version`),
  delete.
- `/v1/subscription-reviews`: the same routes; filters `subscription_id`, `verdict`; ordered by
  `review_date`, then id.
- `/v1/subscription-aliases`: the same routes; filters `subscription_id`, `mapping_status`;
  ordered by id.
- `stats` groups by one declared filter only; any other field is a 422.
- Subscription rules: `name`, `provider` (each at most 200 characters) and `amount` (not negative)
  are required; `name` + `provider` + `owner_scope` is unique (409); `status` is trial, active,
  paused, cancelled or expired (default active); `billing_interval` is weekly, monthly, quarterly,
  yearly, one_time or usage (default monthly); `owner_scope` defaults to solo_empire; `currency`
  defaults to THB; `auto_renew` defaults to true.
- Review rules: `subscription_id` must be an existing subscription of the same workspace (422
  `reference_not_found` otherwise); `review_date`, `period_start` and `period_end` are required;
  one review per subscription and `review_date` (409); `value_score` is 0 to 100; `usage_events`,
  `usage_hours`, `time_saved_hours` and `revenue_attributed` cannot be negative; `verdict` is
  worth_it, conditional, not_worth_it or insufficient_data (default insufficient_data);
  `period_end` cannot be before `period_start` (422 `invalid_value` naming both fields, on
  create and on patch).
- Alias rules: `alias` is required, at most 300 characters and unique within a workspace (409);
  `subscription_id` is optional but, when given, must exist in the same workspace;
  `mapping_status` is resolved or unresolved (default unresolved); `source` defaults to
  `subscription-review`.
- A subscription that still has reviews or aliases cannot be deleted (409 `has_dependents`).
- Dates on a subscription (`started_on`, `next_billing_date`, `cancelled_on`) are not checked
  against each other.
- An import (`legacy_id`) keeps the old id and may carry the original `created_at`/`updated_at`.
- Out of scope: charging or cancelling at the provider, and matching statement lines.

## Measures

Proposed (nothing collects them yet):

- Active subscriptions without a review in the last 90 days.
- Aliases left `unresolved` (target: 0).
- Share of reviews with verdict `not_worth_it` whose subscription is still active.
- 5xx rate and p95 latency on `/v1/subscriptions` (uptime worker and Sentry).
