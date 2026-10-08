"""
fourmost_payload.py

WHAT we send to 4MOST for a transient, checked against the 4MOST target catalogue
specification (Target_catalog_format_specification):

  - DEC must be <= +40 and RA in [0, 359.99999999]           -> otherwise not submitted
  - MAG must be in [3, 50] and in the band named by MAG_TYPE
  - MAG_TYPE must come from 4MOST's case-sensitive whitelist  -> MAG_TYPE_FOR_BAND below
  - CADENCE 1048576 (= 2**20, SINGLE_VISIT): transients are observed in one visit
  - DATE_EARLIEST / DATE_LATEST are JD in [2459945.5, 2462501.5], or 0.
    TiDES stores MJD everywhere; convert with mjd_to_jd() here and nowhere else.

Magnitude policy (requested by 4MOST):
  - the first POST carries the magnitude of the LATEST real detection, with its own band;
  - afterwards we PATCH mag + mag_type only when the object gets brighter than a milestone
    (18, 17, 16, 15) that 4MOST has not been told about yet. Fainter: no update. Brighter
    than 15: nothing more to report.

Only pure functions here (no database, no network), so all of it can be unit tested.
"""

import json
import math

# TiDES band key (as stored in tides_master.latest_mags) -> 4MOST MAG_TYPE.
# Bands not listed here (u, y, ...) are never sent.
MAG_TYPE_FOR_BAND = {
    'g_lsst': 'LSST_g_AB',
    'r_lsst': 'LSST_r_AB',
    'i_lsst': 'LSST_i_AB',
    'z_lsst': 'LSST_z_AB',
    # 4MOST has no ZTF system. Decision (Chris): label ZTF g/r as SDSS g/r for 4MOST only;
    # the TiDES database keeps the true system (g_ztf / r_ztf).
    'g_ztf': 'SDSS_g_AB',
    'r_ztf': 'SDSS_r_AB',
}
MAG_MIN, MAG_MAX = 3.0, 50.0
RA_MAX = 359.99999999
DEC_MIN, DEC_MAX = -89.99999999, 40.0
MJD_TO_JD = 2400000.5
JD_MIN, JD_MAX = 2459945.5, 2462501.5
MAG_MILESTONES = (18.0, 17.0, 16.0, 15.0)

# Fields that are the same for every TiDES transient (unchanged from the current controller).
TRANSIENT_DEFAULTS = {
    "pmra": 0.0,
    "pmdec": 0.0,
    "epoch": 2000,
    "resolution": 1,
    "subsurvey": "tides-sn",
    "cadence": 1048576,
    "template": 'SN_spec_specid56_snt1_phase5_redshift0.169.fits',
    "ruleset": 'tides_snMay2024',
    "extent_flag": 0,
    "extent_parameter": 0,
    "extent_index": 0,
    "t_exp_d": 38.0,
    "t_exp_g": 38.0,
    "t_exp_b": 4300.,
    "t_exp_s": 4300.,
    "classification": "TRA",
}


def is_number(x):
    """True for a finite int/float (False for None, NaN, inf, strings)."""
    try:
        return x is not None and not isinstance(x, bool) and math.isfinite(float(x))
    except (TypeError, ValueError):
        return False


def mjd_to_jd(mjd):
    """MJD -> JD for 4MOST date fields. None means 'no constraint', which 4MOST writes as 0."""
    if mjd is None:
        return 0.0
    jd = float(mjd) + MJD_TO_JD
    if not JD_MIN <= jd <= JD_MAX:
        raise ValueError(f"JD {jd} (from MJD {mjd}) is outside 4MOST's allowed range {JD_MIN}-{JD_MAX}")
    return jd


def _as_dict(value):
    """jsonb columns arrive as dict (psycopg2) or str; NULL arrives as None/NaN."""
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value:
        return json.loads(value)
    return {}


def latest_detection(latest_mags, latest_mjd):
    """
    The most recent real detection in a band 4MOST understands, as (band, mag, mjd).
    Returns None if there is none. Ties on MJD go to the brighter magnitude.
    """
    mags, mjds = _as_dict(latest_mags), _as_dict(latest_mjd)
    best = None
    for band, mag in mags.items():
        mjd = mjds.get(band)
        if band not in MAG_TYPE_FOR_BAND or not (is_number(mag) and is_number(mjd)):
            continue
        mag, mjd = float(mag), float(mjd)
        if not (MAG_MIN <= mag <= MAG_MAX) or mjd <= 0:
            continue                      # e.g. the old 56.4 placeholder
        if best is None or (mjd, -mag) > (best[2], -best[1]):
            best = (band, mag, mjd)
    return best


def build_transient_payload(row, survey_id):
    """
    The POST payload for one tides_master row (dict-like).
    Returns (payload, None) or (None, reason_it_cannot_be_submitted).
    """
    ra, dec = row.get('ra'), row.get('dec')
    if not (is_number(ra) and is_number(dec)):
        return None, 'ra/dec missing or not finite'
    ra = min(float(ra) % 360.0, RA_MAX)
    dec = float(dec)
    if dec > DEC_MAX:
        return None, f'dec {dec:.5f} is north of +40 (4MOST limit)'
    if dec < DEC_MIN:
        return None, f'dec {dec:.8f} is below the 4MOST limit'
    detection = latest_detection(row.get('latest_mags'), row.get('latest_mjd'))
    if detection is None:
        return None, 'no valid detection magnitude (4MOST needs MAG in [3, 50] with a known band)'
    band, mag, _ = detection

    payload = dict(TRANSIENT_DEFAULTS)
    payload.update({
        "uploadedfor_survey_id": int(survey_id),
        "name": str(row['name']),
        "ra": ra,
        "dec": dec,
        "mag": round(mag, 3),
        "mag_type": MAG_TYPE_FOR_BAND[band],
        "is_active": True,
    })
    return payload, None


def milestone_mag_update(row):
    """
    The PATCH {mag, mag_type} owed to 4MOST, or None.

    Owed when the latest detection is at or brighter than a milestone (18/17/16/15) that is
    brighter than the magnitude 4MOST already has (row['mag_4most']; unknown counts as faint).
    """
    detection = latest_detection(row.get('latest_mags'), row.get('latest_mjd'))
    if detection is None:
        return None
    band, mag, _ = detection
    sent = float(row['mag_4most']) if is_number(row.get('mag_4most')) else 99.0
    if any(mag <= milestone < sent for milestone in MAG_MILESTONES):
        return {"mag": round(mag, 3), "mag_type": MAG_TYPE_FOR_BAND[band]}
    return None
