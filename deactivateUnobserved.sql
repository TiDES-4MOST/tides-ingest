CREATE TEMP TABLE to_deactivate AS SELECT * from tides_master where active=True and updated < now() - interval '4days' or (rmag>20.5 and gmag>20.5);

UPDATE tides_master SET active=False FROM to_deactivate WHERE to_deactivate.tides_id=tides_master.tides_id;  

