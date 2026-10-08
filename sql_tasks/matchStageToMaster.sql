-- Attach every not-yet-matched staged alert to its NEAREST tides_master row within 1".
-- q3c_join(stage..., master..., r): master is the SECOND pair, so the q3c index on
-- tides_master (q3c_ang2ipix(ra, dec)) is used. (The old q3c_radial_query argument order
-- could not use it and scanned the whole table.)
-- Ties are broken by tides_id, so the result is always the same.
UPDATE tides_stage ts
SET tides_id = nearest.tides_id
FROM (
    SELECT DISTINCT ON (s.stage_row) s.stage_row, tm.tides_id
    FROM tides_stage s
    JOIN tides_master tm ON q3c_join(s.ra, s.dec, tm.ra, tm.dec, :radius_deg)
    WHERE s.tides_id IS NULL
    ORDER BY s.stage_row, q3c_dist(s.ra, s.dec, tm.ra, tm.dec), tm.tides_id
) nearest
WHERE ts.stage_row = nearest.stage_row;
