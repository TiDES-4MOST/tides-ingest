CREATE TEMP TABLE to_deactivate AS
SELECT *
FROM tides_master
WHERE active = True
    AND updated < now() - interval '5days'
    OR (
        rlatest > 17
        and glatest > 17
    );
--- Update the master table to deactivate the objects
UPDATE tides_master
SET active = False
FROM to_deactivate
WHERE to_deactivate.tides_id = tides_master.tides_id
RETURNING tides_master.tides_id,
    tides_master.pk_4most,
    tides_master.active;