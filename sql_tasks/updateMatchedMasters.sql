-- One UPDATE per matched master (several staged alerts can match the same master).
--   * jdmin/jdmax only ever widen (LEAST/GREATEST).
--   * Per band, a magnitude/MJD/count is only taken if its MJD is NEWER than what we have
--     (tides_merge_bands), so replaying an old alert never overwrites newer data.
--   * Only a genuinely new detection (newest staged jdmax > stored jdmax) may reactivate the
--     object, move `updated`, or change last_survey_id. A replay changes nothing.
--   * If several alerts match one master, the band data comes from the newest of them.
-- `old` is the same row read before the update, so RETURNING can report the previous state
-- (PostgreSQL 17 has no RETURNING old.*).
WITH per_master AS (
    SELECT tides_id, MIN(jdmin) AS jdmin, MAX(jdmax) AS jdmax
    FROM tides_stage
    WHERE tides_id IS NOT NULL
    GROUP BY tides_id
),
newest AS (
    SELECT DISTINCT ON (tides_id) tides_id, survey_id, object_id, latest_mag, latest_filter, n_sources
    FROM tides_stage
    WHERE tides_id IS NOT NULL
    ORDER BY tides_id, jdmax DESC, stage_row
)
UPDATE tides_master tm
SET jdmin        = LEAST(tm.jdmin, p.jdmin),
    jdmax        = GREATEST(tm.jdmax, p.jdmax),
    latest_mags  = tides_merge_bands(tm.latest_mags, tm.latest_mjd, n.latest_mag,    n.latest_filter),
    n_sources    = tides_merge_bands(tm.n_sources,   tm.latest_mjd, n.n_sources,     n.latest_filter),
    latest_mjd   = tides_merge_bands(tm.latest_mjd,  tm.latest_mjd, n.latest_filter, n.latest_filter),
    active       = CASE WHEN p.jdmax > tm.jdmax THEN TRUE ELSE tm.active END,
    sync_pending = CASE WHEN p.jdmax > tm.jdmax AND NOT tm.active THEN TRUE ELSE tm.sync_pending END,
    last_survey_id = CASE WHEN p.jdmax > tm.jdmax THEN n.survey_id ELSE tm.last_survey_id END,
    updated      = CASE WHEN p.jdmax > tm.jdmax THEN CURRENT_TIMESTAMP ELSE tm.updated END
FROM per_master p
JOIN newest n ON n.tides_id = p.tides_id
JOIN tides_master old ON old.tides_id = p.tides_id
WHERE tm.tides_id = p.tides_id
RETURNING tm.tides_id, tm.name, tm.pk_4most, tm.active,
          old.active AS old_active,
          (p.jdmax > old.jdmax) IS TRUE AS new_detection,
          n.survey_id, n.object_id AS survey_object_id;
