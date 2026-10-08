-- Survey-native ids for each TiDES object, keyed on the tides_id the match already found
-- (no second spatial join). One name per (object, survey) - the newest staged one.
INSERT INTO surveys (tides_id, transient_name, source_survey_id)
SELECT DISTINCT ON (tides_id, survey_id) tides_id, object_id, survey_id
FROM tides_stage
WHERE tides_id IS NOT NULL
ORDER BY tides_id, survey_id, jdmax DESC, stage_row
ON CONFLICT (tides_id, source_survey_id) DO NOTHING;
