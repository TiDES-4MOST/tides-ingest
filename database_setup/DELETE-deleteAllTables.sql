-- Drop tables in reverse order of dependency to be safe, 
-- though CASCADE handles constraints.
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