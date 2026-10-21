"""Defaults and uniqueness in the subscriptions cell."""

import uuid
from datetime import UTC, datetime

from subscriptions_helpers import ALIASES, REVIEWS, SUBSCRIPTIONS, alias, create, review

# --- subscription ---------------------------------------------------------------------------


def test_new_subscription_defaults_to_active_monthly_thb_for_the_empire(client, owner):
    response = create(client, owner)
    assert response.status_code == 201, response.text
    subscription = response.json()
    assert subscription["status"] == "active"
    assert subscription["billing_interval"] == "monthly"
    assert subscription["owner_scope"] == "solo_empire"
    assert subscription["category"] == "software"
    assert subscription["currency"] == "THB"
    assert subscription["auto_renew"] is True
    assert subscription["version"] == 1


def test_defaults_can_be_overridden(client, owner):
    subscription = create(
        client,
        owner,
        status="trial",
        billing_interval="yearly",
        owner_scope="personal",
        category="media",
        currency="USD",
        auto_renew=False,
    ).json()
    assert subscription["status"] == "trial"
    assert subscription["billing_interval"] == "yearly"
    assert subscription["owner_scope"] == "personal"
    assert subscription["category"] == "media"
    assert subscription["currency"] == "USD"
    assert subscription["auto_renew"] is False


def test_amount_keeps_its_decimal_value(client, owner):
    assert create(client, owner, amount=1234.56).json()["amount"] == 1234.56


def test_name_provider_and_scope_together_are_unique(client, owner):
    assert create(client, owner, name="Plus", provider="Acme").status_code == 201
    repeat = create(client, owner, name="Plus", provider="Acme")
    assert repeat.status_code == 409, repeat.text


def test_the_same_name_is_allowed_for_another_provider_or_scope(client, owner):
    assert create(client, owner, name="Plus", provider="Acme").status_code == 201
    other_provider = create(client, owner, name="Plus", provider="Globex")
    assert other_provider.status_code == 201, other_provider.text
    other_scope = create(
        client, owner, name="Plus", provider="Acme", owner_scope="personal"
    )
    assert other_scope.status_code == 201, other_scope.text


def test_renaming_into_an_existing_subscription_is_refused(client, owner):
    create(client, owner, name="Taken", provider="Acme")
    other = create(client, owner, name="Free", provider="Acme").json()
    path = f"{SUBSCRIPTIONS}/{other['id']}"
    refused = client.patch(path, headers=owner, json={"name": "Taken"})
    assert refused.status_code == 409, refused.text
    assert client.get(path, headers=owner).json()["name"] == "Free"


def test_a_subscription_import_keeps_the_old_id(client, owner):
    imported = create(client, owner, legacy_id=5150)
    assert imported.status_code == 201, imported.text
    assert imported.json()["id"] == 5150
    found = client.get(f"{SUBSCRIPTIONS}/legacy/5150", headers=owner)
    assert found.status_code == 200, found.text
    assert create(client, owner, legacy_id=5150).status_code == 409


def test_an_import_may_carry_its_original_timestamps(client, owner):
    imported = create(
        client,
        owner,
        legacy_id=5151,
        created_at="2021-04-05T06:07:08+00:00",
        updated_at="2021-05-06T07:08:09+00:00",
    )
    assert imported.status_code == 201, imported.text
    created = datetime.fromisoformat(imported.json()["created_at"])
    assert created == datetime(2021, 4, 5, 6, 7, 8, tzinfo=UTC)
    refused = create(client, owner, created_at="2021-04-05T06:07:08+00:00")
    assert refused.status_code == 422, refused.text


def test_account_project_and_goal_ids_are_not_checked_against_other_services(
    client, owner
):
    response = create(client, owner, account_id=111, project_id=222, goal_id=333)
    assert response.status_code == 201, response.text
    assert response.json()["account_id"] == 111
    assert response.json()["project_id"] == 222
    assert response.json()["goal_id"] == 333


# --- subscription review --------------------------------------------------------------------


def test_new_review_defaults_to_insufficient_data(client, owner):
    sid = create(client, owner).json()["id"]
    response = review(client, owner, sid)
    assert response.status_code == 201, response.text
    record = response.json()
    assert record["verdict"] == "insufficient_data"
    assert record["value_score"] is None
    assert record["version"] == 1


def test_a_subscription_has_one_review_per_review_date(client, owner):
    sid = create(client, owner).json()["id"]
    assert review(client, owner, sid, review_date="2026-10-01").status_code == 201
    repeat = review(client, owner, sid, review_date="2026-10-01")
    assert repeat.status_code == 409, repeat.text


def test_reviews_on_other_dates_or_of_other_subscriptions_are_allowed(client, owner):
    first = create(client, owner).json()["id"]
    second = create(client, owner).json()["id"]
    assert review(client, owner, first, review_date="2026-10-01").status_code == 201
    next_day = review(client, owner, first, review_date="2026-10-02")
    assert next_day.status_code == 201, next_day.text
    other = review(client, owner, second, review_date="2026-10-01")
    assert other.status_code == 201, other.text


def test_moving_a_review_onto_a_taken_date_is_refused(client, owner):
    sid = create(client, owner).json()["id"]
    review(client, owner, sid, review_date="2026-10-01")
    later = review(client, owner, sid, review_date="2026-11-01").json()
    path = f"{REVIEWS}/{later['id']}"
    refused = client.patch(path, headers=owner, json={"review_date": "2026-10-01"})
    assert refused.status_code == 409, refused.text


def test_a_review_import_keeps_the_old_id(client, owner):
    sid = create(client, owner).json()["id"]
    imported = review(client, owner, sid, legacy_id=6160)
    assert imported.status_code == 201, imported.text
    assert imported.json()["id"] == 6160
    found = client.get(f"{REVIEWS}/legacy/6160", headers=owner)
    assert found.status_code == 200, found.text


# --- subscription alias ---------------------------------------------------------------------


def test_new_alias_defaults_to_unresolved_from_the_subscription_review(client, owner):
    response = alias(client, owner)
    assert response.status_code == 201, response.text
    record = response.json()
    assert record["mapping_status"] == "unresolved"
    assert record["source"] == "subscription-review"
    assert record["subscription_id"] is None
    assert record["version"] == 1


def test_an_alias_text_is_unique(client, owner):
    assert alias(client, owner, alias="NETFLIX.COM").status_code == 201
    repeat = alias(client, owner, alias="NETFLIX.COM")
    assert repeat.status_code == 409, repeat.text


def test_two_workspaces_can_use_the_same_alias(client, owner, harness, platform):
    assert alias(client, owner, alias="SHARED*NAME").status_code == 201
    stranger = uuid.uuid4()
    platform.grants[stranger] = set(harness.cell.permissions())
    other = harness.token(stranger, uuid.uuid4())
    assert alias(client, other, alias="SHARED*NAME").status_code == 201


def test_an_alias_can_be_resolved_to_a_subscription_later(client, owner):
    sid = create(client, owner).json()["id"]
    record = alias(client, owner, alias="ACME INC *1234").json()
    path = f"{ALIASES}/{record['id']}"
    resolved = client.patch(
        path, headers=owner, json={"subscription_id": sid, "mapping_status": "resolved"}
    )
    assert resolved.status_code == 200, resolved.text
    assert resolved.json()["subscription_id"] == sid
    assert resolved.json()["mapping_status"] == "resolved"
    assert resolved.json()["version"] == 2
