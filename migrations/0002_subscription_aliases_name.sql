-- Fix the generated plural: subscriptions.subscription_aliass -> subscriptions.subscription_aliases (route /v1/subscription-aliases).
-- Grants, row-level security and policies follow the table; the sequence and index names keep the old spelling.
ALTER TABLE subscriptions.subscription_aliass RENAME TO subscription_aliases;
