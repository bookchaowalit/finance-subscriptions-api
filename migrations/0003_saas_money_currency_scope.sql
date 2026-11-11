-- SPEC-003 (2026-11-11): exact money, ISO 4217 currency, workspace-defined scope (subscriptions).
-- Amounts move from double precision to numeric; currency codes must be three upper-case letters;
-- owner_scope (the owner's own life areas) becomes scope, a free label each workspace defines.
-- Append-only: do not edit once applied.

-- subscription
ALTER TABLE subscriptions.subscriptions NO FORCE ROW LEVEL SECURITY;
UPDATE subscriptions.subscriptions SET currency = upper(btrim(currency));
ALTER TABLE subscriptions.subscriptions FORCE ROW LEVEL SECURITY;
ALTER TABLE subscriptions.subscriptions RENAME COLUMN owner_scope TO scope;
DO $$ DECLARE c text; BEGIN FOR c IN SELECT conname FROM pg_constraint WHERE conrelid = 'subscriptions.subscriptions'::regclass AND contype = 'c' AND conkey = ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid = 'subscriptions.subscriptions'::regclass AND attname = 'scope')] LOOP EXECUTE format('ALTER TABLE subscriptions.subscriptions DROP CONSTRAINT %I', c); END LOOP; END $$;
ALTER TABLE subscriptions.subscriptions ALTER COLUMN scope SET DEFAULT '';
ALTER TABLE subscriptions.subscriptions ADD CONSTRAINT subscriptions_scope_check CHECK (length(scope) <= 64);
DO $$ DECLARE i text; BEGIN SELECT indexname INTO i FROM pg_indexes WHERE schemaname = 'subscriptions' AND tablename = 'subscriptions' AND indexdef LIKE '%(workspace_id, scope)'; IF i IS NOT NULL THEN EXECUTE format('ALTER INDEX subscriptions.%I RENAME TO subscriptions_scope', i); END IF; END $$;
DO $$ DECLARE c text; BEGIN FOR c IN SELECT conname FROM pg_constraint WHERE conrelid = 'subscriptions.subscriptions'::regclass AND contype = 'c' AND conkey = ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid = 'subscriptions.subscriptions'::regclass AND attname = 'amount')] LOOP EXECUTE format('ALTER TABLE subscriptions.subscriptions DROP CONSTRAINT %I', c); END LOOP; END $$;
ALTER TABLE subscriptions.subscriptions ALTER COLUMN amount TYPE numeric(19,4) USING amount::numeric(19,4);
ALTER TABLE subscriptions.subscriptions ADD CONSTRAINT subscriptions_amount_check CHECK (amount >= 0);
DO $$ DECLARE c text; BEGIN FOR c IN SELECT conname FROM pg_constraint WHERE conrelid = 'subscriptions.subscriptions'::regclass AND contype = 'c' AND conkey = ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid = 'subscriptions.subscriptions'::regclass AND attname = 'currency')] LOOP EXECUTE format('ALTER TABLE subscriptions.subscriptions DROP CONSTRAINT %I', c); END LOOP; END $$;
ALTER TABLE subscriptions.subscriptions ADD CONSTRAINT subscriptions_currency_check CHECK (currency ~ '^[A-Z]{3}$');

-- subscription-review
ALTER TABLE subscriptions.subscription_reviews NO FORCE ROW LEVEL SECURITY;
UPDATE subscriptions.subscription_reviews SET revenue_currency = upper(btrim(revenue_currency));
ALTER TABLE subscriptions.subscription_reviews FORCE ROW LEVEL SECURITY;
DO $$ DECLARE c text; BEGIN FOR c IN SELECT conname FROM pg_constraint WHERE conrelid = 'subscriptions.subscription_reviews'::regclass AND contype = 'c' AND conkey = ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid = 'subscriptions.subscription_reviews'::regclass AND attname = 'revenue_attributed')] LOOP EXECUTE format('ALTER TABLE subscriptions.subscription_reviews DROP CONSTRAINT %I', c); END LOOP; END $$;
ALTER TABLE subscriptions.subscription_reviews ALTER COLUMN revenue_attributed TYPE numeric(19,4) USING revenue_attributed::numeric(19,4);
ALTER TABLE subscriptions.subscription_reviews ADD CONSTRAINT subscription_reviews_revenue_attributed_check CHECK (revenue_attributed IS NULL OR (revenue_attributed >= 0));
DO $$ DECLARE c text; BEGIN FOR c IN SELECT conname FROM pg_constraint WHERE conrelid = 'subscriptions.subscription_reviews'::regclass AND contype = 'c' AND conkey = ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid = 'subscriptions.subscription_reviews'::regclass AND attname = 'revenue_currency')] LOOP EXECUTE format('ALTER TABLE subscriptions.subscription_reviews DROP CONSTRAINT %I', c); END LOOP; END $$;
ALTER TABLE subscriptions.subscription_reviews ADD CONSTRAINT subscription_reviews_revenue_currency_check CHECK (revenue_currency IS NULL OR (revenue_currency ~ '^[A-Z]{3}$'));
