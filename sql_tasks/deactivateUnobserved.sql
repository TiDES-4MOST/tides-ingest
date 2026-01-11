CREATE TEMP TABLE to_deactivate AS
SELECT *
FROM tides_master
WHERE active = True
    AND updated < now() - interval '5days'
    OR (
        rmag > 22.5
        and gmag > 22.5
    );
--- Update the master table to deactivate the objects
UPDATE tides_master
SET active = False
FROM to_deactivate
WHERE to_deactivate.tides_id = tides_master.tides_id;