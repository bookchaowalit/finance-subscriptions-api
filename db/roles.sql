-- Roles for the subscriptions database. Locally a superuser runs this; on Neon
-- `subscriptions-db provision` creates the same roles through the migrator.
--
-- subscriptions_migrator  owns the schema and runs migrations only.
-- subscriptions_api       the API at runtime: no superuser, no BYPASSRLS, owns nothing.
-- subscriptions_publisher sweeps and marks the outbox across workspaces; nothing else.

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'subscriptions_migrator') THEN
        CREATE ROLE subscriptions_migrator LOGIN NOSUPERUSER NOCREATEROLE NOBYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'subscriptions_api') THEN
        CREATE ROLE subscriptions_api LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOINHERIT;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'subscriptions_publisher') THEN
        CREATE ROLE subscriptions_publisher LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOINHERIT;
    END IF;
END
$$;
