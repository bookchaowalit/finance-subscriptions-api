# SPEC-004 — Trust a second token issuer (platform-identity) in the cellkit

Standard: ADR-0004 — TDD + SDD (architecture-docs, `docs/architecture/decisions/ADR-0004-tdd-sdd-standard.md`).
Phase 0 of the Finance suite plan (solo-empire `repository-catalog/portfolio/FINANCE-SUITE-PLAN-2026-11-11.md`).
The same `src/cellkit` change lands in all ten finance services, so the copy stays byte-identical.

## Document control

| Field | Value |
|---|---|
| Spec ID / version | SPEC-004 / 1.0 |
| Status | approved for branch work; production activation needs a separate owner go |
| Repository / path | bookchaowalit/finance-subscriptions-api / `specs/004-identity-issuer-trust/spec.md` (copy of the reference in finance-transactions-api) |
| Owner (maker) | Claude Code (session "Microservice architecture และ frontend", 2026-11-11) |
| Reviewer (checker) | Codex review Stop hook + CI gates |
| Approver | owner in chat 2026-11-11 ("เริ่มเฟส 0 เลย") for branch work only |
| Main-only | no: branch `feat/identity-issuer-trust`, rebased on `main` before merge |

## 1. Problem and intent

- **Problem:** a cell verifies tokens from one issuer only (`<CELL>_JWT_ISSUER`, today `empire-platform`). Customer
  sign-in goes through platform-identity (`POST /v1/token`, issuer
  `https://identity-api-production-fad1.up.railway.app`), whose tokens every cell refuses. Switching the one issuer
  would break the owner's existing `empire-platform` tokens (owner-web, MCP, CLI).
- **Outcome:** a cell can trust platform-identity **next to** the main issuer, with a narrower contract, so the move is a
  cutover that can be rolled back by removing two variables.
- **Not enough alone:** the cell forwards the same bearer token to `EMPIRE_PLATFORM_URL` (`/v1/authz/decide`,
  `/v1/executions`); tasks-api has to trust the identity issuer too (separate change in platform-empire).

## 2. Scope

| In scope | Out of scope (non-goals) |
|---|---|
| `src/cellkit/auth.py` (`TrustedIssuer`, `TokenVerifier.identity`), `src/cellkit/server.py` (two optional variables), tests | Setting the variables on Railway; tasks-api trust; token exchange to per-cell audiences (finance phase 4, coordinated with the finance SaaS session); signup |

## 3. Behaviour

| ID | Given / When / Then |
|---|---|
| AC-01 | **Given** no identity variables, **then** behaviour is unchanged: only the main issuer verifies. |
| AC-02 | **Given** `<CELL>_JWT_IDENTITY_ISSUER` and `<CELL>_JWT_IDENTITY_JWKS_URL`, **when** a token with `iss` = that issuer, the cell audience, `sub`, `ws`, `exp`, `iat`, `jti` arrives signed by a key in that JWK Set, **then** it verifies to a principal bound to `ws`. |
| AC-03 | **Given** an identity-issuer token, **when** it is signed by the main issuer's key (pinned or JWK Set) instead of the identity JWK Set, **then** 401: each issuer verifies only with its own keys. |
| AC-04 | **Given** a main-issuer token signed with an identity key, **then** 401. |
| AC-05 | **Given** an identity-issuer token carrying `del`, `obo` or `act`, **then** 401: identity tokens never act for someone else. |
| AC-06 | **Given** an identity-issuer token whose lifetime exceeds 3600 s, **then** 401. |
| AC-07 | **Given** an identity-issuer token for another audience, or an unknown `iss`, **then** 401. |
| AC-08 | **Given** only one of the two identity variables, or an identity issuer equal to the main issuer, or a non-https JWK Set URL (http only on loopback), **then** `<cell>-api` refuses to start. In hosted mode the dev issuer is refused as the identity issuer too. |

## 4. Interfaces

- New optional variables: `<CELL>_JWT_IDENTITY_ISSUER`, `<CELL>_JWT_IDENTITY_JWKS_URL` (both or neither).
- `cellkit.auth.TrustedIssuer(issuer: str, jwks: JwksKeys)`; `TokenVerifier(..., identity: TrustedIssuer | None = None)`.
- No API, schema or event change.

## 5. Rollout and rollback

1. Merge to `main` in the ten repos (no variable set ⇒ no behaviour change, AC-01).
2. With an owner go: tasks-api trusts the identity issuer (platform-empire), then set the two variables on one cell,
   exchange a Google id_token at `/v1/token` for that cell's audience and read/write one record (positive path) and
   try another workspace (negative path).
3. Rollback: delete the two variables and redeploy; main-issuer tokens are unaffected throughout.

## 6. TDD log

- Red: `tests/test_cellkit_identity_issuer.py` and the new cases in `tests/test_cellkit_server.py` fail on `main`.
- Green: after the `auth.py`/`server.py` change.
