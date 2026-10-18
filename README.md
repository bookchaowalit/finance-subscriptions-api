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
| `/v1/subscriptions` | subscription | `status`, `owner_scope`, `provider`, `billing_interval` |
| `/v1/subscription-reviews` | subscription-review | `subscription_id`, `verdict` |
| `/v1/subscription-aliases` | subscription-alias | `subscription_id`, `mapping_status` |

## Develop and operate

```bash
uv venv .venv && uv pip install --python .venv/bin/python -e ".[dev]"
.venv/bin/pytest -q
NEON_API_KEY=... .venv/bin/subscriptions-db provision
npm install && railway config apply   # service settings only; code deploys from GitHub (below)
railway scale --service subscriptions-api asia-southeast1-eqsg3a=1 us-east4-eqdc4a=0   # next to Neon and the platform
```

Deploys (CD, since 2026-10-18): `subscriptions-api` is connected to this repository's `main`
with Railway's Wait for CI (`checkSuites`). A merge to `main` deploys only after the CI and
security checks pass; a failing check leaves production on the previous deployment. Roll back
by redeploying an earlier deployment in Railway. `railway up` is no longer used.

Railway variables: `SENTRY_DSN` (error reporting; unset = off, see `src/cellkit/_sentry.py`),
`SUBSCRIPTIONS_DB_MODE=hosted`, `SUBSCRIPTIONS_DB_URL_API`,
`SUBSCRIPTIONS_JWT_PUBLIC_KEY_PEM`, `SUBSCRIPTIONS_JWT_ISSUER`, `EMPIRE_PLATFORM_URL`.
