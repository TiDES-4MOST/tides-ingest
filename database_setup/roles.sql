-- roles.sql: least-privilege roles for a TiDES database. Run ONCE per database, as the admin
-- (superuser) role, AFTER setupDB.sh and migrate.sh, and again after any migration that adds tables:
--
--     psql -h <db host> -U <admin> -d <database> -v ON_ERROR_STOP=1 -f roles.sql
--
-- It is idempotent. It creates two NOLOGIN group roles and grants them exactly what they need:
--
--   tides_ingest    the Prefect flows (tides-ingest, tides-ostd-sync): read/insert/update the
--                   pipeline tables, append to the provenance tables, temporary staging tables,
--                   the name sequence. No DELETE, no TRUNCATE, no DDL.
--   tides_readonly  dashboards, the TOM, people: SELECT on everything (also on tables created later).
--
-- Login users are separate, so passwords never appear in this file. As the admin, in psql:
--
--     CREATE ROLE spo_4most_ingest LOGIN IN ROLE tides_ingest;
--     \password spo_4most_ingest
--     ALTER ROLE spo_4most_ingest SET statement_timeout = '15min';
--     ALTER ROLE spo_4most_ingest SET idle_in_transaction_session_timeout = '10min';
--
--     CREATE ROLE tides_reader LOGIN IN ROLE tides_readonly CONNECTION LIMIT 10;
--     \password tides_reader
--     ALTER ROLE tides_reader SET default_transaction_read_only = on;
--     ALTER ROLE tides_reader SET statement_timeout = '5min';
--
-- Then put spo_4most_ingest (not the admin) in TIDES_DB_USER/TIDES_DB_PASS of the production .env.
-- Keep the admin role for setupDB.sh, migrate.sh, this file, backups and emergencies.
-- (Role settings must be set on the LOGIN role: they are not inherited from group roles.)
-- Roles for the future web apps (manual-submission inbox, selection-function editor) come with
-- their migration; see the extensions integration guide.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tides_ingest') THEN
        CREATE ROLE tides_ingest NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tides_readonly') THEN
        CREATE ROLE tides_readonly NOLOGIN;
    END IF;
    -- TEMPORARY: the ingest stages each survey's alerts in a temporary table (sql_tasks/createStage.sql)
    EXECUTE format('GRANT CONNECT, TEMPORARY ON DATABASE %I TO tides_ingest', current_database());
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO tides_readonly', current_database());
END $$;

GRANT USAGE ON SCHEMA public TO tides_ingest, tides_readonly;

-- The ingest flow and sync_ostd
GRANT SELECT, INSERT, UPDATE ON tides_master TO tides_ingest;
GRANT SELECT, INSERT ON surveys, pipeline_selections, survey_nights TO tides_ingest;
GRANT SELECT, INSERT ON tides_events, tides_4most_log TO tides_ingest;          -- append-only
GRANT SELECT ON survey_ids, pipelines, schema_migrations TO tides_ingest;
GRANT SELECT ON tides_host, tides_host_catalog TO tides_ingest;
GRANT USAGE ON SEQUENCE tides_master_tides_id_seq, tides_events_event_id_seq, tides_4most_log_log_id_seq
    TO tides_ingest;
-- The naming trigger calls nextval() and, at the year rollover, setval() on tides_seq
GRANT USAGE, UPDATE ON SEQUENCE tides_seq TO tides_ingest;

-- Read-only, now and for tables the admin creates later (e.g. by future migrations)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO tides_readonly;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO tides_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO tides_readonly;
-- pg_dump (the tides_backup login) also reads every sequence: without this, the nightly backup fails
-- after a migration that adds a sequence, until this file is run again.
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON SEQUENCES TO tides_readonly;
