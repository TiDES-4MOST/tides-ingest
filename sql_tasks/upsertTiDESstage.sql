MERGE INTO tides_master tm 
USING tides_stage ts
ON ts.name=tm.name
WHEN MATCHED THEN
UPDATE set
jdmax = ts.jdmax,
magrmin = ts.magrmin,
maggmin = ts.maggmin,
rmag = ts.rmag,
gmag = ts.gmag,
jdgmax = ts.jdgmax,
jdrmax = ts.jdrmax,
ncandgp = ts.ncandgp,
sherlock_class = ts.classification,
active = True,
updated = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
INSERT (name, ra, dec, jdmin, jdmax, magrmin, maggmin, rmag, gmag, jdgmax, jdrmax, ncandgp, sherlock_class, active, created, updated )
VALUES (ts.name, ts.ramean, ts.decmean, ts.jdmin, ts.jdmax, ts.magrmin, ts.maggmin, ts.rmag, ts.gmag,
 ts.jdgmax, ts.jdrmax, ts.ncandgp, ts.classification, True, CURRENT_TIMESTAMP,CURRENT_TIMESTAMP );   