-- Drop tables in reverse order of dependency to be safe, 
-- though CASCADE handles constraints.
DROP TABLE IF EXISTS pipeline_selections CASCADE;
DROP TABLE IF EXISTS pipelines CASCADE;
DROP TABLE IF EXISTS surveys CASCADE;
DROP TABLE IF EXISTS survey_ids CASCADE;
DROP TABLE IF EXISTS tides_ztf CASCADE;
DROP TABLE IF EXISTS tides_master CASCADE;
-- Note: CASCADE on DROP TABLE removes the table AND any 
-- dependent objects (like foreign keys in other tables).
-- It does NOT delete the *data* in other tables, 
-- nor does it delete the *other tables* themselves, 
-- it just breaks the link.
-- To delete everything, we explicitly DROP each table.
-- Reset sequences
-- tides_id is a BIGSERIAL, so it has an implicit sequence named tides_master_tides_id_seq
-- We check if it exists before trying to alter it to avoid errors if the table didn't exist
ALTER SEQUENCE IF EXISTS tides_master_tides_id_seq RESTART WITH 1;
-- tides_seq is explicitly created for the naming function
ALTER SEQUENCE IF EXISTS tides_seq RESTART WITH 1;