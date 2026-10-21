"""Listing, grouping, ordering and the lifecycle of subscriptions, reviews and aliases."""

from subscriptions_helpers import ALIASES, REVIEWS, SUBSCRIPTIONS, alias, create, review


def ids(response):
    assert response.status_code == 200, response.text
    return [item["id"] for item in response.json()["items"]]


# --- subscription ---------------------------------------------------------------------------


def test_list_filters_by_status_and_provider(client, owner):
    wanted = create(client, owner, status="paused", provider="Acme").json()
    create(client, owner, status="paused", provider="Globex")
    create(client, owner, status="active", provider="Acme")
    listed = client.get(
        SUBSCRIPTIONS, headers=owner, params={"status": "paused", "provider": "Acme"}
    )
    assert ids(listed) == [wanted["id"]]


def test_list_filters_by_owner_scope_and_billing_interval(client, owner):
    wanted = create(
        client, owner, owner_scope="venture", billing_interval="yearly"
    ).json()
    create(client, owner, owner_scope="venture", billing_interval="monthly")
    create(client, owner, owner_scope="personal", billing_interval="yearly")
    listed = client.get(
        SUBSCRIPTIONS,
        headers=owner,
        params={"owner_scope": "venture", "billing_interval": "yearly"},
    )
    assert ids(listed) == [wanted["id"]]


def test_a_filter_can_list_several_values(client, owner):
    trial = create(client, owner, status="trial").json()
    paused = create(client, owner, status="paused").json()
    create(client, owner, status="cancelled")
    listed = client.get(
        SUBSCRIPTIONS, headers=owner, params=[("status", "trial"), ("status", "paused")]
    )
    assert sorted(ids(listed)) == sorted([trial["id"], paused["id"]])


def test_an_unknown_filter_is_refused(client, owner):
    listed = client.get(SUBSCRIPTIONS, headers=owner, params={"category": "software"})
    assert listed.status_code == 422


def test_subscriptions_are_listed_by_id(client, owner):
    first = create(client, owner, name="Zeta").json()
    second = create(client, owner, name="Alpha").json()
    assert ids(client.get(SUBSCRIPTIONS, headers=owner)) == [first["id"], second["id"]]
    newest_first = client.get(SUBSCRIPTIONS, headers=owner, params={"order": "desc"})
    assert ids(newest_first) == [second["id"], first["id"]]


def test_stats_count_subscriptions_per_status(client, owner):
    create(client, owner, status="active")
    create(client, owner, status="active")
    create(client, owner, status="trial")
    stats = client.get(
        f"{SUBSCRIPTIONS}/stats", headers=owner, params={"group_by": "status"}
    )
    assert stats.status_code == 200, stats.text
    assert stats.json()["groups"] == {"active": 2, "trial": 1}


def test_stats_count_subscriptions_per_billing_interval_for_one_scope(client, owner):
    create(client, owner, billing_interval="monthly", owner_scope="venture")
    create(client, owner, billing_interval="yearly", owner_scope="venture")
    create(client, owner, billing_interval="yearly", owner_scope="personal")
    stats = client.get(
        f"{SUBSCRIPTIONS}/stats",
        headers=owner,
        params={"group_by": "billing_interval", "owner_scope": "venture"},
    )
    assert stats.json()["groups"] == {"monthly": 1, "yearly": 1}


def test_stats_refuse_a_field_that_is_not_a_filter(client, owner):
    for field in ("name", "amount", "category", "auto_renew", "next_billing_date"):
        stats = client.get(
            f"{SUBSCRIPTIONS}/stats", headers=owner, params={"group_by": field}
        )
        assert stats.status_code == 422, field


def test_cancelling_a_subscription_bumps_its_version(client, owner):
    subscription = create(client, owner).json()
    path = f"{SUBSCRIPTIONS}/{subscription['id']}"
    cancelled = client.patch(
        path,
        headers=owner,
        json={"status": "cancelled", "cancelled_on": "2026-10-15", "auto_renew": False},
    )
    assert cancelled.status_code == 200, cancelled.text
    assert cancelled.json()["status"] == "cancelled"
    assert cancelled.json()["auto_renew"] is False
    assert cancelled.json()["version"] == 2


def test_a_patch_that_changes_nothing_keeps_the_version(client, owner):
    subscription = create(client, owner, amount=99, status="trial").json()
    path = f"{SUBSCRIPTIONS}/{subscription['id']}"
    same = client.patch(path, headers=owner, json={"amount": 99, "status": "trial"})
    assert same.status_code == 200, same.text
    assert same.json()["version"] == 1


def test_a_stale_expected_version_is_refused(client, owner):
    subscription = create(client, owner).json()
    path = f"{SUBSCRIPTIONS}/{subscription['id']}"
    client.patch(path, headers=owner, json={"status": "paused"})
    stale = client.patch(
        path, headers=owner, json={"status": "cancelled", "expected_version": 1}
    )
    assert stale.status_code == 409, stale.text
    assert client.get(path, headers=owner).json()["status"] == "paused"


def test_deleting_a_subscription_removes_it(client, owner):
    subscription = create(client, owner).json()
    path = f"{SUBSCRIPTIONS}/{subscription['id']}"
    assert client.delete(path, headers=owner).status_code == 204
    assert client.get(path, headers=owner).status_code == 404


def test_a_subscription_with_reviews_cannot_be_deleted(client, owner):
    sid = create(client, owner).json()["id"]
    record = review(client, owner, sid).json()
    refused = client.delete(f"{SUBSCRIPTIONS}/{sid}", headers=owner)
    assert refused.status_code == 409, refused.text
    assert refused.json()["detail"] == {"error": "has_dependents"}
    assert client.delete(f"{REVIEWS}/{record['id']}", headers=owner).status_code == 204
    assert client.delete(f"{SUBSCRIPTIONS}/{sid}", headers=owner).status_code == 204


def test_a_subscription_with_aliases_cannot_be_deleted(client, owner):
    sid = create(client, owner).json()["id"]
    record = alias(client, owner, subscription_id=sid).json()
    refused = client.delete(f"{SUBSCRIPTIONS}/{sid}", headers=owner)
    assert refused.status_code == 409, refused.text
    assert client.delete(f"{ALIASES}/{record['id']}", headers=owner).status_code == 204
    assert client.delete(f"{SUBSCRIPTIONS}/{sid}", headers=owner).status_code == 204


def test_a_subscription_that_does_not_exist_is_404(client, owner):
    assert client.get(f"{SUBSCRIPTIONS}/999999999", headers=owner).status_code == 404
    patched = client.patch(
        f"{SUBSCRIPTIONS}/999999999", headers=owner, json={"status": "paused"}
    )
    assert patched.status_code == 404
    assert client.delete(f"{SUBSCRIPTIONS}/999999999", headers=owner).status_code == 404


# --- subscription review --------------------------------------------------------------------


def test_reviews_filter_by_subscription_and_verdict(client, owner):
    first = create(client, owner).json()["id"]
    second = create(client, owner).json()["id"]
    wanted = review(client, owner, first, verdict="worth_it").json()
    review(client, owner, first, review_date="2026-10-02", verdict="not_worth_it")
    review(client, owner, second, verdict="worth_it")
    listed = client.get(
        REVIEWS,
        headers=owner,
        params={"subscription_id": first, "verdict": "worth_it"},
    )
    assert ids(listed) == [wanted["id"]]


def test_reviews_are_ordered_by_review_date_then_id(client, owner):
    sid = create(client, owner).json()["id"]
    late = review(client, owner, sid, review_date="2026-12-01").json()
    early = review(client, owner, sid, review_date="2026-08-01").json()
    middle = review(client, owner, sid, review_date="2026-10-01").json()
    assert ids(client.get(REVIEWS, headers=owner)) == [
        early["id"],
        middle["id"],
        late["id"],
    ]
    newest_first = client.get(REVIEWS, headers=owner, params={"order": "desc"})
    assert ids(newest_first) == [late["id"], middle["id"], early["id"]]


def test_review_stats_count_per_verdict_and_per_subscription(client, owner):
    first = create(client, owner).json()["id"]
    second = create(client, owner).json()["id"]
    review(client, owner, first, verdict="worth_it")
    review(client, owner, first, review_date="2026-10-02", verdict="worth_it")
    review(client, owner, second, verdict="conditional")
    by_verdict = client.get(
        f"{REVIEWS}/stats", headers=owner, params={"group_by": "verdict"}
    )
    assert by_verdict.json()["groups"] == {"worth_it": 2, "conditional": 1}
    by_subscription = client.get(
        f"{REVIEWS}/stats", headers=owner, params={"group_by": "subscription_id"}
    )
    assert by_subscription.json()["groups"] == {str(first): 2, str(second): 1}


def test_review_stats_refuse_a_field_that_is_not_a_filter(client, owner):
    for field in ("value_score", "review_date", "period_start"):
        stats = client.get(
            f"{REVIEWS}/stats", headers=owner, params={"group_by": field}
        )
        assert stats.status_code == 422, field


def test_scoring_a_review_bumps_its_version(client, owner):
    sid = create(client, owner).json()["id"]
    record = review(client, owner, sid).json()
    path = f"{REVIEWS}/{record['id']}"
    scored = client.patch(
        path, headers=owner, json={"value_score": 85, "verdict": "worth_it"}
    )
    assert scored.status_code == 200, scored.text
    assert scored.json()["value_score"] == 85
    assert scored.json()["version"] == 2
    same = client.patch(path, headers=owner, json={"value_score": 85})
    assert same.json()["version"] == 2


def test_deleting_a_review_removes_it(client, owner):
    sid = create(client, owner).json()["id"]
    record = review(client, owner, sid).json()
    path = f"{REVIEWS}/{record['id']}"
    assert client.delete(path, headers=owner).status_code == 204
    assert client.get(path, headers=owner).status_code == 404
    assert ids(client.get(REVIEWS, headers=owner)) == []


# --- subscription alias ---------------------------------------------------------------------


def test_aliases_filter_by_subscription_and_mapping_status(client, owner):
    sid = create(client, owner).json()["id"]
    resolved = alias(
        client, owner, subscription_id=sid, mapping_status="resolved"
    ).json()
    alias(client, owner)
    listed = client.get(ALIASES, headers=owner, params={"subscription_id": sid})
    assert ids(listed) == [resolved["id"]]
    open_ones = client.get(
        ALIASES, headers=owner, params={"mapping_status": "unresolved"}
    )
    assert resolved["id"] not in ids(open_ones)
    assert len(ids(open_ones)) == 1


def test_alias_stats_count_per_mapping_status(client, owner):
    alias(client, owner)
    alias(client, owner)
    alias(client, owner, mapping_status="resolved")
    stats = client.get(
        f"{ALIASES}/stats", headers=owner, params={"group_by": "mapping_status"}
    )
    assert stats.json()["groups"] == {"unresolved": 2, "resolved": 1}


def test_alias_stats_refuse_a_field_that_is_not_a_filter(client, owner):
    for field in ("alias", "source", "evidence"):
        stats = client.get(
            f"{ALIASES}/stats", headers=owner, params={"group_by": field}
        )
        assert stats.status_code == 422, field


def test_deleting_an_alias_removes_it(client, owner):
    record = alias(client, owner).json()
    path = f"{ALIASES}/{record['id']}"
    assert client.delete(path, headers=owner).status_code == 204
    assert client.get(path, headers=owner).status_code == 404
