MERGE INTO tides_master tm USING tides_stage ts ON q3c_radial_query(ts.ra, ts.dec, tm.ra, tm.dec, 0.000277778)
WHEN MATCHED THEN
UPDATE
set jdmax = ts.jdmax,
    active = True,
    glatest = ts.gmag,
    rlatest = ts.rmag,
    updated = CURRENT_TIMESTAMP
    WHEN NOT MATCHED THEN
INSERT (
        ra,
        dec,
        jdmin,
        jdmax,
        jd_obs_trigger,
        glatest,
        rlatest,
        active,
        created,
        updated
    )
VALUES (
        ts.ra,
        ts.dec,
        ts.jdmin,
        ts.jdmax,
        ts.jdmax,
        ts.gmag,
        ts.rmag,
        True,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    );