-- SPEC-006 (2026-11-11): cross-service references between the finance cells (subscriptions).
-- Append-only: do not edit once applied.

-- The relay sends a record's events in insert order (the row lock makes it the commit order).
ALTER TABLE subscriptions.outbox ADD COLUMN seq bigint GENERATED ALWAYS AS IDENTITY;
DROP INDEX subscriptions.outbox_unrelayed;
CREATE INDEX outbox_unrelayed ON subscriptions.outbox (seq) WHERE relayed_at IS NULL;

-- SPEC-006: cross-service references, fed by finance.events.v1.
CREATE TABLE subscriptions.ref_targets (
    workspace_id   uuid NOT NULL,
    target         text NOT NULL,
    record_id      bigint NOT NULL,
    status         text,
    deleted        boolean NOT NULL DEFAULT false,
    record_version bigint NOT NULL DEFAULT 0,
    event_seq      bigint NOT NULL DEFAULT 0,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, target, record_id)
);
CREATE TABLE subscriptions.ref_sources (
    workspace_id    uuid NOT NULL,
    source          text NOT NULL,
    source_id       bigint NOT NULL,
    field           text NOT NULL,
    target_resource text NOT NULL,
    target_id       bigint,
    deleted         boolean NOT NULL DEFAULT false,
    record_version  bigint NOT NULL DEFAULT 0,
    event_seq       bigint NOT NULL DEFAULT 0,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, source, source_id, field)
);
CREATE INDEX ref_sources_live ON subscriptions.ref_sources (workspace_id, target_resource, target_id) WHERE NOT deleted;
CREATE TABLE subscriptions.ref_offsets (
    topic       text NOT NULL,
    partition   integer NOT NULL,
    next_offset bigint NOT NULL,
    PRIMARY KEY (topic, partition)
);
ALTER TABLE subscriptions.ref_targets ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscriptions.ref_targets FORCE ROW LEVEL SECURITY;
CREATE POLICY workspace_isolation ON subscriptions.ref_targets USING (workspace_id = subscriptions.current_workspace());
CREATE POLICY publisher_sync ON subscriptions.ref_targets TO subscriptions_publisher USING (true) WITH CHECK (true);
GRANT SELECT ON subscriptions.ref_targets TO subscriptions_api;
GRANT SELECT, INSERT, UPDATE ON subscriptions.ref_targets TO subscriptions_publisher;
ALTER TABLE subscriptions.ref_sources ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscriptions.ref_sources FORCE ROW LEVEL SECURITY;
CREATE POLICY workspace_isolation ON subscriptions.ref_sources USING (workspace_id = subscriptions.current_workspace());
CREATE POLICY publisher_sync ON subscriptions.ref_sources TO subscriptions_publisher USING (true) WITH CHECK (true);
GRANT SELECT ON subscriptions.ref_sources TO subscriptions_api;
GRANT SELECT, INSERT, UPDATE ON subscriptions.ref_sources TO subscriptions_publisher;
GRANT SELECT, INSERT, UPDATE ON subscriptions.ref_offsets TO subscriptions_publisher;

-- Rows written before SPEC-005 announce themselves once, so other cells learn their ids.
ALTER TABLE subscriptions.outbox NO FORCE ROW LEVEL SECURITY;
ALTER TABLE subscriptions.subscriptions NO FORCE ROW LEVEL SECURITY;
INSERT INTO subscriptions.outbox (workspace_id, type, subject, classification, payload, correlation_id) SELECT workspace_id, 'subscriptions.subscription.snapshot', 'empire:subscriptions:subscription:' || id, 'confidential', jsonb_build_object('scope', scope, 'status', status, 'billing_interval', billing_interval, 'currency', currency, 'started_on', started_on, 'next_billing_date', next_billing_date, 'cancelled_on', cancelled_on, 'auto_renew', auto_renew, 'account_id', account_id, 'project_id', project_id, 'goal_id', goal_id, 'version', version, 'deleted', false), 'spec-006-snapshot' FROM subscriptions.subscriptions ORDER BY id;
ALTER TABLE subscriptions.subscriptions FORCE ROW LEVEL SECURITY;
ALTER TABLE subscriptions.subscription_reviews NO FORCE ROW LEVEL SECURITY;
INSERT INTO subscriptions.outbox (workspace_id, type, subject, classification, payload, correlation_id) SELECT workspace_id, 'subscriptions.subscription-review.snapshot', 'empire:subscriptions:subscription-review:' || id, 'confidential', jsonb_build_object('subscription_id', subscription_id, 'review_date', review_date, 'period_start', period_start, 'period_end', period_end, 'verdict', verdict, 'next_review_date', next_review_date, 'version', version, 'deleted', false), 'spec-006-snapshot' FROM subscriptions.subscription_reviews ORDER BY id;
ALTER TABLE subscriptions.subscription_reviews FORCE ROW LEVEL SECURITY;
ALTER TABLE subscriptions.subscription_aliases NO FORCE ROW LEVEL SECURITY;
INSERT INTO subscriptions.outbox (workspace_id, type, subject, classification, payload, correlation_id) SELECT workspace_id, 'subscriptions.subscription-alias.snapshot', 'empire:subscriptions:subscription-alias:' || id, 'confidential', jsonb_build_object('subscription_id', subscription_id, 'mapping_status', mapping_status, 'version', version, 'deleted', false), 'spec-006-snapshot' FROM subscriptions.subscription_aliases ORDER BY id;
ALTER TABLE subscriptions.subscription_aliases FORCE ROW LEVEL SECURITY;
ALTER TABLE subscriptions.outbox FORCE ROW LEVEL SECURITY;
