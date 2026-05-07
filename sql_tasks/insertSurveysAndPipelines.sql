-- 1. Insert into the `surveys` mapping table
INSERT INTO surveys (tides_id, transient_name, source_survey_id)
SELECT tm.tides_id, ts.object_id, ts.survey_id
FROM tides_stage ts
JOIN tides_master tm ON q3c_radial_query(ts.ra, ts.dec, tm.ra, tm.dec, 0.000277778)
ON CONFLICT (tides_id, source_survey_id) DO NOTHING;

-- 2. Insert into the new `pipeline_selections` junction table
INSERT INTO pipeline_selections (tides_id, source_survey_id, pipeline_id)
SELECT tm.tides_id, ts.survey_id, ts.pipeline_id
FROM tides_stage ts
JOIN tides_master tm ON q3c_radial_query(ts.ra, ts.dec, tm.ra, tm.dec, 0.000277778)
ON CONFLICT (tides_id, source_survey_id, pipeline_id) DO NOTHING;
