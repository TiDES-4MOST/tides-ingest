-- First selection of each object by each (survey, pipeline). RETURNING gives only the rows
-- that are new, which the controller records as 'selected' events.
INSERT INTO pipeline_selections (tides_id, source_survey_id, pipeline_id)
SELECT DISTINCT tides_id, survey_id, pipeline_id
FROM tides_stage
WHERE tides_id IS NOT NULL
ON CONFLICT (tides_id, source_survey_id, pipeline_id) DO NOTHING
RETURNING tides_id, source_survey_id AS survey_id, pipeline_id;
