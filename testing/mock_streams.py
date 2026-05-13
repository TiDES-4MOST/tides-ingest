import pandas as pd
import json

def generate_mock_lsst_targets():
    """
    Simulates a payload returned by the LSST kafka stream.
    """
    data = [
        {
            'object_id': 1000001,
            'survey_id': 1,
            'pipeline_id': 1,
            'ra': 150.0000,
            'dec': 2.0000,
            'jdmin': 61000.0,
            'jdmax': 61050.0,
            'latest_filter': json.dumps({"g_lsst": 61050.0, "r_lsst": 61048.0, "i_lsst": 61040.0}),
            'latest_mag': json.dumps({"g_lsst": 21.5, "r_lsst": 21.0, "i_lsst": 20.8}),
            'n_sources': json.dumps({"g_lsst": 5, "r_lsst": 8, "i_lsst": 3})
        },
        # A transient that only has a single detection (extreme edge case)
        {
            'object_id': 1000002,
            'survey_id': 1,
            'pipeline_id': 1,
            'ra': 160.0000,
            'dec': -5.0000,
            'jdmin': 61055.0,
            'jdmax': 61055.0,
            'latest_filter': json.dumps({"z_lsst": 61055.0}),
            'latest_mag': json.dumps({"z_lsst": 22.1}),
            'n_sources': json.dumps({"z_lsst": 1})
        }
    ]
    return pd.DataFrame(data)

def generate_mock_ztf_targets():
    """
    Simulates a payload returned by the ZTF kafka stream.
    Includes edge cases designed to spatially match with LSST mock data.
    """
    data = [
        # This ZTF object is 0.5 arcsec away from LSST 1000001
        # 0.5 arcsec = ~0.000138 degrees
        {
            'object_id': "ZTF24aaaaaa",
            'survey_id': 2,
            'pipeline_id': 1,
            'ra': 150.0001,
            'dec': 2.0001,
            'jdmin': 61010.0,
            'jdmax': 61060.0, # Newer than LSST jdmax (61050), should update the master table
            'latest_filter': json.dumps({"g_ztf": 61060.0, "r_ztf": 61058.0}),
            'latest_mag': json.dumps({"g_ztf": 21.2, "r_ztf": 20.9}),
            'n_sources': json.dumps({"g_ztf": 12, "r_ztf": 10})
        },
        # A brand new ZTF object not seen by LSST
        {
            'object_id': "ZTF24bbbbbb",
            'survey_id': 2,
            'pipeline_id': 1,
            'ra': 170.0000,
            'dec': 10.0000,
            'jdmin': 61005.0,
            'jdmax': 61045.0,
            'latest_filter': json.dumps({"r_ztf": 61045.0}),
            'latest_mag': json.dumps({"r_ztf": 19.5}),
            'n_sources': json.dumps({"r_ztf": 2})
        }
    ]
    return pd.DataFrame(data)
