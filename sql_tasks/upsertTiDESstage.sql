WITH old_data AS (
    -- 1. Identify which staging rows already exist in the master table
    -- using a 1 arcsecond (0.000277778 degrees) positional cross-match.
    SELECT tm.tides_id,
        tm.active AS old_status
    FROM tides_master tm,
        tides_stage ts
    WHERE q3c_radial_query(ts.ra, ts.dec, tm.ra, tm.dec, 0.000277778)
),
updated_rows AS (
    -- 2. Update existing records in the master table.
    UPDATE tides_master tm
    SET jdmax = GREATEST(tm.jdmax, ts.jdmax), -- Ensure we never roll back the max julian date
        active = True,
        -- Merging the new magnitude finding into our JSONB dictionary.
        -- We construct a new key-value pair using the incoming filter (e.g. 'g') and magnitude (e.g. 21.0), 
        -- and concatenate it (||) with the existing latest_mags JSON object. 
        -- This overwrites the old magnitude for that specific filter while leaving other filters intact.
        latest_mags = COALESCE(tm.latest_mags, '{}'::jsonb) || jsonb_build_object(ts.latest_filter, ts.latest_mag),
        updated = CURRENT_TIMESTAMP
    FROM tides_stage ts
    WHERE q3c_radial_query(ts.ra, ts.dec, tm.ra, tm.dec, 0.000277778)
    RETURNING tm.*
),
inserted_rows AS (
    -- 3. Insert fresh, unmatched transients.
    INSERT INTO tides_master (
            ra,
            dec,
            jdmin,
            jdmax,
            jd_obs_trigger,
            latest_mags,
            active,
            created,
            updated
        )
    SELECT ts.ra,
        ts.dec,
        ts.jdmin,
        ts.jdmax,
        ts.jdmax,
        -- Initialize the JSONB column with the very first detection's filter and magnitude 
        jsonb_build_object(ts.latest_filter, ts.latest_mag),
        True,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    FROM tides_stage ts
    WHERE NOT EXISTS (
            SELECT 1
            FROM tides_master tm
            WHERE q3c_radial_query(ts.ra, ts.dec, tm.ra, tm.dec, 0.000277778)
        )
    RETURNING *
),
joinedOldNew AS (
    -- 4. Re-attach the old_status from the `old_data` CTE so the controller can detect 
    -- transitions from inactive (False) to active (True) and update the 4MOST API.
    SELECT updated_rows.*,
        old_data.old_status
    FROM updated_rows,
        old_data
    WHERE updated_rows.tides_id = old_data.tides_id
)
-- 5. Return all updated and completely new rows to the Python controller interface
SELECT *
FROM joinedOldNew
UNION ALL
SELECT *,
    NULL as old_status
FROM inserted_rows;