-- 001: 4MOST outbox state, the survey clock, and the replay-safe band merge.
-- Every statement is idempotent: running this file twice is harmless.

-- 4MOST outbox state on each transient (see tides_flows/fourmost_sync.py)
ALTER TABLE tides_master
    ADD COLUMN IF NOT EXISTS sync_state TEXT
        CHECK (sync_state IN ('ambiguous', 'failed', 'conflict', 'not_submittable')),
    ADD COLUMN IF NOT EXISTS sync_attempts INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS sync_last_error TEXT,
    ADD COLUMN IF NOT EXISTS mag_4most DOUBLE PRECISION,          -- the MAG 4MOST currently has
    ADD COLUMN IF NOT EXISTS last_survey_id INT REFERENCES survey_ids(survey_id);  -- made the latest detection
-- (last_survey_id is NULL for rows created before this migration: deactivation then uses the wall clock.)

-- The survey clock: one row per (survey, observing night) on which that survey sent us at
-- least one TiDES alert (see sql_tasks/recordHeartbeat.sql). night = floor(MJD - 0.5), the
-- same convention as Rubin's dayObs. Maintenance, bad weather or our own outage add no rows,
-- so they never age an object (see sql_tasks/deactivateUnobserved.sql).
CREATE TABLE IF NOT EXISTS survey_nights (
    survey_id INT NOT NULL REFERENCES survey_ids(survey_id),
    night     INT NOT NULL,
    PRIMARY KEY (survey_id, night)
);

-- Per-band merge: take a band's new value only if that band's new MJD is NEWER than the stored
-- one. Used for latest_mags, latest_mjd and n_sources, so an old (replayed) alert can never
-- overwrite newer data.
--   old_vals/new_vals: {band: value}     old_mjd/new_mjd: {band: MJD}
CREATE OR REPLACE FUNCTION tides_merge_bands(old_vals jsonb, old_mjd jsonb, new_vals jsonb, new_mjd jsonb)
RETURNS jsonb LANGUAGE sql IMMUTABLE AS $$
    SELECT COALESCE(old_vals, '{}'::jsonb) || COALESCE(
        (SELECT jsonb_object_agg(n.band, new_vals -> n.band)
         FROM jsonb_each_text(COALESCE(new_mjd, '{}'::jsonb)) AS n(band, mjd)
         WHERE new_vals ? n.band
           AND n.mjd::float8 > COALESCE((old_mjd ->> n.band)::float8, '-infinity'::float8)),
        '{}'::jsonb)
$$;
