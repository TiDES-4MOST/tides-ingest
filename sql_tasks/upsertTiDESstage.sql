WITH old_data AS (
    SELECT tm.tides_id,
        tm.active AS old_status
    FROM tides_master tm,
        tides_stage ts
    WHERE q3c_radial_query(ts.ra, ts.dec, tm.ra, tm.dec, 0.000277778)
),
updated_rows AS (
    UPDATE tides_master tm
    SET jdmax = GREATEST(tm.jdmax, ts.jdmax),
        active = True,
        latest_mags = COALESCE(tm.latest_mags, '{}'::jsonb) || jsonb_build_object(ts.latest_filter, ts.latest_mag),
        updated = CURRENT_TIMESTAMP
    FROM tides_stage ts
    WHERE q3c_radial_query(ts.ra, ts.dec, tm.ra, tm.dec, 0.000277778)
    RETURNING tm.*
),
inserted_rows AS (
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
    SELECT updated_rows.*,
        old_data.old_status
    FROM updated_rows,
        old_data
    WHERE updated_rows.tides_id = old_data.tides_id
)
SELECT *
FROM joinedOldNew
UNION ALL
SELECT *,
    NULL as old_status
FROM inserted_rows;