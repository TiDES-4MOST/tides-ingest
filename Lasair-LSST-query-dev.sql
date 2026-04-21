SELECT objects.diaObjectId,
    objects.ra,
    objects.decl,
    objects.lastDiaSourceMjdTai,
    objects.firstDiaSourceMjdTai,
    objects.latest_psfFlux objects.g_psfFlux,
    objects_ext.g_psfFluxSigma objects.g_latestMJD,
    objects.r_psfFlux,
    objects_ext.r_psfFluxSigma objects.r_latestMJD,
    objects.i_psfFlux,
    objects_ext.i_psfFluxSigma objects.i_latestMJD,
    objects.z_psfFlux,
    objects_ext.z_psfFluxSigma objects.z_latestMJD,
    objects.nPosDiaSources,
    objects.nPosDiaSourcesNights,
    objects.ngSources,
    objects.nrSources,
    objects.niSources,
    objects.nzSources,
    objects.nPosDiaSourcesNights sherlock_classifications.classification as sherlock_classifications
FROM
WHERE -- We want an object detected on 2 seperate night
    objects.nPosDiaSourcesNights > 2 -- AND we want at least 3 filters with more than 1 detection
    AND (IFNULL(ngSources > 1, 0)) + (IFNULL(nrSources > 1, 0)) + (IFNULL(niSources > 1, 0)) + (IFNULL(nzSources > 1, 0)) >= 3 --- AND we want the object to be brighter than 22.5 in any filter
    --- with zp=31.4, Flux is therefore 3630.78
    AND (
        objects.g_psfFlux > 3630.78
        OR objects.r_psfFlux > 3630.78
        OR objects.i_psfFlux > 3630.78
        OR objects.z_psfFlux > 3630.78
    ) --- We'll use Sherlock FOR NOW to filter out stars and AGN etc 
    AND sherlock_classifications.classification in ('SN', 'NT', 'ORPHAN', 'UNCLEAR')