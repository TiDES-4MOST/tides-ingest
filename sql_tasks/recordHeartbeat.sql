-- The survey clock: remember every observing night on which this survey sent us alerts.
-- Called inside the survey's transaction, while tides_stage still exists.
INSERT INTO survey_nights (survey_id, night)
SELECT DISTINCT survey_id, floor(jdmax - 0.5)::int
FROM tides_stage
ON CONFLICT DO NOTHING;
