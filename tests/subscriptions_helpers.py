"""Shared helpers for the subscriptions domain tests."""

import itertools
from typing import Any

SUBSCRIPTIONS = "/v1/subscriptions"
REVIEWS = "/v1/subscription-reviews"
ALIASES = "/v1/subscription-aliases"

_names = itertools.count(1)


def create(client: Any, owner: dict[str, str], **fields: Any) -> Any:
    """A subscription; the name is unique per call unless one is given."""
    body = {
        "name": f"Plan {next(_names)}",
        "provider": "Acme",
        "amount": 199.0,
        **fields,
    }
    return client.post(SUBSCRIPTIONS, headers=owner, json=body)


def review(
    client: Any, owner: dict[str, str], subscription_id: int, **fields: Any
) -> Any:
    body = {
        "subscription_id": subscription_id,
        "review_date": "2026-10-01",
        "period_start": "2026-09-01",
        "period_end": "2026-09-30",
        **fields,
    }
    return client.post(REVIEWS, headers=owner, json=body)


def alias(client: Any, owner: dict[str, str], **fields: Any) -> Any:
    body = {"alias": f"ACME*{next(_names)}", **fields}
    return client.post(ALIASES, headers=owner, json=body)
