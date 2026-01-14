CREATE TEMP TABLE to_deactivate AS
SELECT *
FROM tides_master
WHERE active = True
    AND updated < now() - interval '0.5days'
    OR (
        rlatest > 22.5
        and glatest > 22.5
    );
--- Update the master table to deactivate the objects
UPDATE tides_master
SET active = False
FROM to_deactivate
WHERE to_deactivate.tides_id = tides_master.tides_id
RETURNING tm.tides_id,
    tm.active;