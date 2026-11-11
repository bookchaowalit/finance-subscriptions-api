-- SPEC-005 (2026-11-11): relay the outbox to Kafka finance.events.v1 (ADR-0008).
-- The relay marks rows in its own columns; published_at stays the platform-graph publisher's.
-- Append-only: do not edit once applied.

ALTER TABLE subscriptions.outbox
    ADD COLUMN relayed_at     timestamptz,
    ADD COLUMN relay_attempts integer NOT NULL DEFAULT 0,
    ADD COLUMN relay_next_at  timestamptz,
    ADD COLUMN relay_error    text;
CREATE INDEX outbox_unrelayed ON subscriptions.outbox (occurred_at, id) WHERE relayed_at IS NULL;

-- The API relays its own workspace's rows inline (RLS-bound); the hourly job uses the publisher role,
-- which already reads every workspace's outbox (policy publisher_sweep) and nothing else.
GRANT UPDATE (relayed_at, relay_attempts, relay_next_at, relay_error) ON subscriptions.outbox TO subscriptions_api;
GRANT UPDATE (relayed_at, relay_attempts, relay_next_at, relay_error) ON subscriptions.outbox TO subscriptions_publisher;
