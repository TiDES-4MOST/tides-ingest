CREATE TEMP TABLE to_deactivate AS
SELECT *
FROM tides_master
WHERE active = True
    AND (
        updated < now() - interval '5days'
        OR (
            -- Ensure we actually have magnitudes before checking them
            latest_mags != '{}'::jsonb
            AND
            -- Check if ALL of the recorded magnitudes are fainter than 22.5
            -- jsonb_each_text expands our { "g": 23.0, "i": 21.0 } object into rows.
            -- Using "NOT EXISTS ( ... WHERE value <= 22.5)" means:
            -- "Are there NO filters brighter than or equal to 22.5?"
            -- If this resolves to TRUE, the object is genuinely too faint across ALL latest filters, so we deactivate it.
            NOT EXISTS (
                SELECT 1
                FROM jsonb_each_text(latest_mags)
                WHERE value::numeric <= 22.5
            )
        )
    );
--- Update the master table to deactivate the objects
UPDATE tides_master
SET active = False
FROM to_deactivate
WHERE to_deactivate.tides_id = tides_master.tides_id
RETURNING tides_master.tides_id,
    tides_master.pk_4most,
    tides_master.active;