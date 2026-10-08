-- Deactivate active objects that are
--   (a) stale: the survey that made the last detection has observed on more than :days
--       further nights without detecting the object again (survey_nights), or, for rows with
--       no last_survey_id, more than :days calendar days have passed (wall clock), or
--   (b) faint: every recorded band magnitude is fainter than :faint_mag (unchanged rule).
UPDATE tides_master tm
SET active = FALSE,
    sync_pending = TRUE
FROM (
    SELECT t.tides_id,
           CASE WHEN age.n > :days THEN :stale_reason ELSE 'faint_alert' END AS reason,
           age.n AS nights_since_detection
    FROM tides_master t
    CROSS JOIN LATERAL (
        SELECT CASE WHEN t.last_survey_id IS NULL THEN :now_mjd - t.jdmax
                    ELSE (SELECT count(*) FROM survey_nights n
                          WHERE n.survey_id = t.last_survey_id
                            AND n.night > floor(t.jdmax - 0.5)) END AS n
    ) age
    WHERE t.active
      AND (   age.n > :days
           OR (    t.latest_mags != '{}'::jsonb
               AND NOT EXISTS (SELECT 1 FROM jsonb_each_text(t.latest_mags) m
                               WHERE m.value::numeric <= :faint_mag)))
) d
WHERE tm.tides_id = d.tides_id
RETURNING tm.tides_id, tm.name, tm.pk_4most, d.reason, d.nights_since_detection AS days_since_detection;
