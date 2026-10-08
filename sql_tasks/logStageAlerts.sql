-- Provenance: one event per staged alert, with the survey's own id for the object.
-- Targets given to us by a person or external source carry submitted_by and are recorded
-- as 'manual_submission' instead of 'alert'.
INSERT INTO tides_events (tides_id, event_type, survey_id, survey_object_id, pipeline_id,
                          submitted_by, details, git_sha, flow_run_id)
SELECT tides_id,
       CASE WHEN CAST(:submitted_by AS text) IS NULL THEN 'alert' ELSE 'manual_submission' END,
       survey_id, object_id, pipeline_id, CAST(:submitted_by AS text),
       jsonb_build_object('jdmin', jdmin, 'jdmax', jdmax, 'latest_mag', latest_mag),
       :git_sha, :flow_run_id
FROM tides_stage
WHERE tides_id IS NOT NULL;
