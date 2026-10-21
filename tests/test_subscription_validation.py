"""What the subscriptions cell refuses: bad subscriptions, reviews and aliases, missing references, backwards periods."""

import uuid

import pytest
from subscriptions_helpers import ALIASES, REVIEWS, SUBSCRIPTIONS, alias, create, review

MISSING_ID = 987654321


# --- subscription ---------------------------------------------------------------------------


@pytest.mark.parametrize("missing", ["name", "provider", "amount"])
def test_a_subscription_needs_a_name_a_provider_and_an_amount(client, owner, missing):
    body = {"name": "Notion", "provider": "Notion Labs", "amount": 10.0}
    del body[missing]
    response = client.post(SUBSCRIPTIONS, headers=owner, json=body)
    assert response.status_code == 422, response.text


def test_a_subscription_amount_cannot_be_negative(client, owner):
    assert create(client, owner, amount=-1).status_code == 422
    assert create(client, owner, amount=0).status_code == 201


def test_name_and_provider_are_limited_to_200_characters(client, owner):
    assert create(client, owner, name="n" * 201).status_code == 422
    assert create(client, owner, provider="p" * 201).status_code == 422
    assert create(client, owner, name="n" * 200, provider="p" * 200).status_code == 201


def test_currency_code_is_at_most_12_characters(client, owner):
    assert create(client, owner, currency="C" * 13).status_code == 422
    assert create(client, owner, currency="USD").status_code == 201


def test_status_must_be_a_known_state(client, owner):
    assert create(client, owner, status="dormant").status_code == 422
    for state in ("trial", "active", "paused", "cancelled", "expired"):
        assert create(client, owner, status=state).status_code == 201, state


def test_billing_interval_must_be_a_known_interval(client, owner):
    assert create(client, owner, billing_interval="hourly").status_code == 422
    for interval in ("weekly", "monthly", "quarterly", "yearly", "one_time", "usage"):
        assert create(client, owner, billing_interval=interval).status_code == 201


def test_owner_scope_must_be_a_known_scope(client, owner):
    assert create(client, owner, owner_scope="nobody").status_code == 422
    for scope in ("personal", "solo_empire", "venture", "client", "shared"):
        assert create(client, owner, owner_scope=scope).status_code == 201, scope


def test_auto_renew_must_be_true_or_false(client, owner):
    assert create(client, owner, auto_renew="maybe").status_code == 422
    assert create(client, owner, auto_renew=False).status_code == 201


def test_billing_dates_must_be_real_dates(client, owner):
    assert create(client, owner, started_on="2026-02-30").status_code == 422
    assert create(client, owner, next_billing_date="next week").status_code == 422


def test_account_project_and_goal_ids_must_be_whole_numbers(client, owner):
    for field in ("account_id", "project_id", "goal_id"):
        assert create(client, owner, **{field: "abc"}).status_code == 422, field


def test_a_subscription_cannot_carry_fields_the_cell_does_not_know(client, owner):
    assert create(client, owner, seats=3).status_code == 422


def test_a_required_subscription_field_cannot_be_cleared(client, owner):
    subscription = create(client, owner).json()
    path = f"{SUBSCRIPTIONS}/{subscription['id']}"
    for field in ("name", "provider", "amount"):
        cleared = client.patch(path, headers=owner, json={field: None})
        assert cleared.status_code == 422, field
    assert client.patch(path, headers=owner, json={"amount": -2}).status_code == 422
    assert client.get(path, headers=owner).json()["version"] == 1


# --- subscription review --------------------------------------------------------------------


@pytest.mark.parametrize(
    "missing", ["subscription_id", "review_date", "period_start", "period_end"]
)
def test_a_review_needs_a_subscription_a_date_and_a_period(client, owner, missing):
    subscription = create(client, owner).json()
    body = {
        "subscription_id": subscription["id"],
        "review_date": "2026-10-01",
        "period_start": "2026-09-01",
        "period_end": "2026-09-30",
    }
    del body[missing]
    refused = client.post(REVIEWS, headers=owner, json=body)
    assert refused.status_code == 422, refused.text


def test_a_review_must_point_at_an_existing_subscription(client, owner):
    refused = review(client, owner, MISSING_ID)
    assert refused.status_code == 422, refused.text
    assert refused.json()["detail"] == {"error": "reference_not_found"}


def test_a_review_cannot_point_at_another_workspaces_subscription(
    client, owner, harness, platform
):
    foreign = create(client, owner).json()
    stranger = uuid.uuid4()
    platform.grants[stranger] = set(harness.cell.permissions())
    other = harness.token(stranger, uuid.uuid4())
    refused = review(client, other, foreign["id"])
    assert refused.status_code == 422, refused.text


def test_a_review_subscription_id_must_be_positive(client, owner):
    assert review(client, owner, 0).status_code == 422
    assert review(client, owner, -4).status_code == 422


def test_value_score_is_between_0_and_100(client, owner):
    subscription = create(client, owner).json()
    sid = subscription["id"]
    assert review(client, owner, sid, value_score=-1).status_code == 422
    assert review(client, owner, sid, value_score=101).status_code == 422
    assert review(client, owner, sid, value_score=0).status_code == 201
    assert (
        review(
            client, owner, sid, review_date="2026-10-02", value_score=100
        ).status_code
        == 201
    )


def test_review_usage_and_savings_cannot_be_negative(client, owner):
    sid = create(client, owner).json()["id"]
    for field in (
        "usage_events",
        "usage_hours",
        "time_saved_hours",
        "revenue_attributed",
    ):
        refused = review(client, owner, sid, **{field: -1})
        assert refused.status_code == 422, field
    accepted = review(
        client,
        owner,
        sid,
        usage_events=0,
        usage_hours=0,
        time_saved_hours=0,
        revenue_attributed=0,
    )
    assert accepted.status_code == 201, accepted.text


def test_usage_events_must_be_a_whole_number(client, owner):
    sid = create(client, owner).json()["id"]
    assert review(client, owner, sid, usage_events=2.5).status_code == 422


def test_verdict_must_be_a_known_verdict(client, owner):
    sid = create(client, owner).json()["id"]
    assert review(client, owner, sid, verdict="maybe").status_code == 422
    for n, verdict in enumerate(
        ("worth_it", "conditional", "not_worth_it", "insufficient_data")
    ):
        accepted = review(
            client, owner, sid, verdict=verdict, review_date=f"2026-11-0{n + 1}"
        )
        assert accepted.status_code == 201, accepted.text


def test_revenue_currency_is_at_most_12_characters(client, owner):
    sid = create(client, owner).json()["id"]
    assert review(client, owner, sid, revenue_currency="C" * 13).status_code == 422
    assert review(client, owner, sid, revenue_currency="USD").status_code == 201


def test_a_review_period_cannot_end_before_it_starts(client, owner):
    sid = create(client, owner).json()["id"]
    backwards = review(
        client, owner, sid, period_start="2026-09-30", period_end="2026-09-01"
    )
    assert backwards.status_code == 422, backwards.text
    assert backwards.json()["detail"] == {
        "error": "invalid_value",
        "fields": ["period_end", "period_start"],
    }
    one_day = review(
        client, owner, sid, period_start="2026-09-30", period_end="2026-09-30"
    )
    assert one_day.status_code == 201, one_day.text


def test_moving_a_review_period_end_before_its_start_is_refused_on_update(
    client, owner
):
    sid = create(client, owner).json()["id"]
    record = review(client, owner, sid).json()
    path = f"{REVIEWS}/{record['id']}"
    refused = client.patch(path, headers=owner, json={"period_end": "2026-08-31"})
    assert refused.status_code == 422, refused.text
    assert refused.json()["detail"]["fields"] == ["period_end", "period_start"]
    stored = client.get(path, headers=owner).json()
    assert stored["period_end"] == "2026-09-30"
    assert stored["version"] == 1
    # A patch that touches neither date still works.
    scored = client.patch(path, headers=owner, json={"value_score": 80})
    assert scored.status_code == 200, scored.text


def test_moving_a_review_period_start_after_its_end_is_refused_on_update(client, owner):
    sid = create(client, owner).json()["id"]
    record = review(client, owner, sid).json()
    path = f"{REVIEWS}/{record['id']}"
    refused = client.patch(path, headers=owner, json={"period_start": "2026-10-01"})
    assert refused.status_code == 422, refused.text
    assert client.get(path, headers=owner).json()["period_start"] == "2026-09-01"


def test_a_review_cannot_be_moved_to_a_missing_subscription(client, owner):
    sid = create(client, owner).json()["id"]
    record = review(client, owner, sid).json()
    path = f"{REVIEWS}/{record['id']}"
    refused = client.patch(path, headers=owner, json={"subscription_id": MISSING_ID})
    assert refused.status_code == 422, refused.text
    assert client.get(path, headers=owner).json()["subscription_id"] == sid


# --- subscription alias ---------------------------------------------------------------------


def test_an_alias_needs_its_text(client, owner):
    assert client.post(ALIASES, headers=owner, json={}).status_code == 422
    assert alias(client, owner, alias="").status_code == 422


def test_alias_text_is_limited_to_300_characters(client, owner):
    assert alias(client, owner, alias="a" * 301).status_code == 422
    assert alias(client, owner, alias="b" * 300).status_code == 201


def test_mapping_status_must_be_resolved_or_unresolved(client, owner):
    assert alias(client, owner, mapping_status="guessed").status_code == 422
    assert alias(client, owner, mapping_status="resolved").status_code == 201


def test_an_alias_must_point_at_an_existing_subscription(client, owner):
    refused = alias(client, owner, subscription_id=MISSING_ID)
    assert refused.status_code == 422, refused.text
    assert refused.json()["detail"] == {"error": "reference_not_found"}


def test_an_alias_cannot_point_at_another_workspaces_subscription(
    client, owner, harness, platform
):
    foreign = create(client, owner).json()
    stranger = uuid.uuid4()
    platform.grants[stranger] = set(harness.cell.permissions())
    other = harness.token(stranger, uuid.uuid4())
    assert alias(client, other, subscription_id=foreign["id"]).status_code == 422
