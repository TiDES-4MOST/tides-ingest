-- Create tides_master rows for staged alerts that matched nothing.
-- Two NEW alerts within 1" of each other in the same batch must become ONE object, so we
-- insert only "leaders": unmatched rows with no other unmatched row (with a smaller
-- stage_row) within 1". The controller then re-runs matchStageToMaster.sql, which attaches
-- the leaders to their new rows and their neighbours to the nearest leader. Any row still
-- unmatched (a neighbour of a neighbour) becomes a leader in the next pass.
INSERT INTO tides_master (ra, dec, jdmin, jdmax, jd_obs_trigger, latest_mags, latest_mjd,
                          n_sources, last_survey_id, active, sync_pending, created, updated)
SELECT ts.ra, ts.dec, ts.jdmin, ts.jdmax, ts.jdmax, ts.latest_mag, ts.latest_filter,
       ts.n_sources, ts.survey_id, TRUE, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
FROM tides_stage ts
WHERE ts.tides_id IS NULL
  AND NOT EXISTS (
      SELECT 1 FROM tides_stage other
      WHERE other.tides_id IS NULL
        AND other.stage_row < ts.stage_row
        AND q3c_join(ts.ra, ts.dec, other.ra, other.dec, :radius_deg))
ORDER BY ts.stage_row
RETURNING tides_id, name, ra, dec, last_survey_id AS survey_id;
