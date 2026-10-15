"""Owner-visible persistence for actual charges and honest AI usage evidence."""

import uuid

import pytest


@pytest.fixture
def subscription(client, owner):
    response = client.post("/v1/subscriptions", headers=owner, json={
        "name": "Custom AI plan", "provider": f"Independent provider {uuid.uuid4().hex}",
        "category": "ai", "amount": 20, "currency": "USD",
    })
    assert response.status_code == 201, response.text
    return response.json()


def test_arbitrary_ai_provider_and_category_filter(client, owner, subscription):
    response = client.get("/v1/subscriptions", headers=owner, params={"category": "ai"})
    assert response.status_code == 200, response.text
    assert subscription["id"] in [item["id"] for item in response.json()["items"]]


@pytest.mark.parametrize("outcome", ["completed", "partial", "discarded"])
def test_usage_preserves_unknown_measurements_and_outcome(client, owner, subscription, outcome):
    response = client.post("/v1/subscription-usages", headers=owner, json={
        "subscription_id": subscription["id"], "used_on": "2026-10-15",
        "title": "Investigate a customer issue", "outcome": outcome,
        "baseline_minutes": None, "actual_minutes": None,
    })
    assert response.status_code == 201, response.text
    usage = response.json()
    assert usage["baseline_minutes"] is None and usage["actual_minutes"] is None
    assert usage["rework_minutes"] == 0 and usage["outcome"] == outcome
    assert usage["source"] == "manual"
    assert client.get(f"/v1/subscription-usages/{usage['id']}", headers=owner).json() == usage


def test_usage_create_is_idempotent_and_preserves_negative_time_evidence(client, owner, subscription):
    payload = {
        "subscription_id": subscription["id"], "used_on": "2026-10-15", "title": "Fix generated report",
        "baseline_minutes": 30, "actual_minutes": 40, "rework_minutes": 15,
        "task_ref": "task:report-42", "evidence": "Manual review required corrections",
        "source": "manual", "source_ref": "report-42",
    }
    headers = {**owner, "Idempotency-Key": f"usage-{uuid.uuid4().hex}"}
    first = client.post("/v1/subscription-usages", headers=headers, json=payload)
    second = client.post("/v1/subscription-usages", headers=headers, json=payload)
    assert first.status_code == second.status_code == 201, first.text
    assert first.json() == second.json()
    assert first.json()["baseline_minutes"] == 30
    assert first.json()["actual_minutes"] == 40 and first.json()["rework_minutes"] == 15


def charge_payload(subscription_id):
    return {"subscription_id": subscription_id, "paid_on": "2026-10-15",
            "period_start": "2026-10-01", "period_end": "2026-10-31", "amount": 24,
            "currency": "USD", "evidence": "receipt:local-fixture-42"}


def test_charge_persists_paid_amount_currency_and_service_period(client, owner, subscription):
    payload = charge_payload(subscription["id"])
    headers = {**owner, "Idempotency-Key": f"charge-{uuid.uuid4().hex}"}
    response = client.post("/v1/subscription-charges", headers=headers, json=payload)
    assert response.status_code == 201, response.text
    charge = response.json()
    assert {name: charge[name] for name in payload} == payload
    assert charge["source"] == "manual"
    assert client.post("/v1/subscription-charges", headers=headers, json=payload).json() == charge


def test_charge_rejects_reversed_service_period_on_create_and_update(client, owner, subscription):
    payload = charge_payload(subscription["id"])
    response = client.post("/v1/subscription-charges", headers=owner,
                           json={**payload, "period_start": "2026-11-01"})
    assert response.status_code == 422, response.text
    valid = client.post("/v1/subscription-charges", headers=owner, json=payload)
    assert valid.status_code == 201, valid.text
    charge = valid.json()
    path = f"/v1/subscription-charges/{charge['id']}"
    assert client.patch(path, headers=owner, json={"period_end": "2026-09-30"}).status_code == 422
    assert client.get(path, headers=owner).json() == charge


def test_review_completeness_requires_explicit_owner_statement(client, owner, subscription):
    response = client.post("/v1/subscription-reviews", headers=owner, json={
        "subscription_id": subscription["id"], "review_date": "2026-10-15",
        "period_start": "2026-10-01", "period_end": "2026-10-15",
    })
    assert response.status_code == 201, response.text
    review = response.json()
    assert review["usage_complete"] is False
    assert review["hourly_value"] is None and review["value_currency"] is None
    response = client.patch(f"/v1/subscription-reviews/{review['id']}", headers=owner, json={
        "usage_complete": True, "hourly_value": 15, "value_currency": "USD",
    })
    assert response.status_code == 200, response.text
    assert response.json()["usage_complete"] is True
    assert response.json()["hourly_value"] == 15 and response.json()["value_currency"] == "USD"
