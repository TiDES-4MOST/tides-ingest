"""
opr4_ztf.py

This module contains the logic for connecting to the Lasair API and importing transients from a stream.
It is designed to be imported by opr4_controller.py.

Usage:
    import opr4_ztf
    targets = opr4_ztf.get_targets()
"""

import pandas as pd
import json
import math
import os
import numpy as np
from prefect.artifacts import create_markdown_artifact
from prefect import flow, task
from datetime import datetime
import stream_utils

ZTF_KAFKA = 'kafka.lsst.ac.uk:9092'
JD_TO_MJD = 2400000.5   # Lasair ZTF times are JD; TiDES stores MJD everywhere

@task
def ingest_report(recentUniqueObjects):
    """
    Generates a markdown report for the ZTF transients.
    
    Args:
        recentUniqueObjects (pd.DataFrame): The DataFrame containing the recent ZTF objects.
    """
    
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    

    markdown_report = f"# TiDES ZTF Stream Report\n"
    markdown_report += f"**Date:** {date_str}\n\n"

    def generate_section(title, data):
        # Check if data is a list (and empty) or None
        if isinstance(data, list):
            if not data:
                return ""
            try:
                df = pd.DataFrame(data)
            except:
                return ""
        else:
            df = data

        # Check if it's a DataFrame and not empty
        if isinstance(df, pd.DataFrame) and not df.empty:
            section = f"## {title}\n"
            
            try:
                section += df.to_markdown(index=False, tablefmt="pipe")
            except ImportError:
                section += df.to_csv(sep="|", index=False)
            
            return section + "\n\n"
        
        return ""

    markdown_report += generate_section("ZTF Transients", recentUniqueObjects)

    # If report is empty (besides header), mention it
    if len(markdown_report.split('\n')) <= 4:
         markdown_report += "_No new transients in this batch._"

    # Create the artifact
    artifact_key = f"tides-ztf-stream-report"

    create_markdown_artifact(
        key=artifact_key,
        markdown=markdown_report,
        description=f"TiDES ZTF Stream Report - {date_str}"
    )

    return markdown_report

def connect_lasair(topic, group_id):
    """
    Opens a Kafka consumer on the Lasair ZTF topic (auto-commit OFF, see stream_utils.py).
    """
    if not all([topic, group_id]):
        print("Warning: Lasair ZTF topic/group id not fully set in .env")
    return stream_utils.open_consumer(ZTF_KAFKA, topic, group_id)

def get_latest_batch(consumer, max_messages=20000):
    """
    Reads the Lasair Kafka stream until it is idle (or max_messages).

    Returns:
        pd.DataFrame: one row per objectId (its most recent alert in this batch).
    """
    records, n_skipped = stream_utils.read_messages(consumer, id_key='objectId', max_messages=max_messages)
    recentObjects = pd.DataFrame.from_records(records)   # built once, not concatenated per message
    if len(recentObjects) != 0:
        recentUniqueObjects = recentObjects.sort_values("latest_jd", ascending=False).drop_duplicates(subset=["objectId"], keep="first")
    else:
        recentUniqueObjects = recentObjects

    ingest_report(recentUniqueObjects)

    return recentUniqueObjects


def process_transients(raw_data, pipeline_id):
    """
    Filters and formats the raw transient data to meet the standard
    tides-ingest contract.
    """
    if raw_data is None or raw_data.empty:
        return pd.DataFrame(columns=stream_utils.CONTRACT_COLUMNS)   # typed empty frame, never None

    out_df = pd.DataFrame()
    out_df['object_id'] = raw_data['objectId'].astype(str)
    out_df['survey_id'] = 2  # integer id for ZTF
    out_df['pipeline_id'] = pipeline_id # Setting dynamic pipeline ID from args
    out_df['ra'] = raw_data.get('ra', pd.Series(dtype='float'))
    
    if 'decl' in raw_data.columns:
        out_df['dec'] = raw_data['decl']
    else:
        out_df['dec'] = raw_data.get('dec', pd.Series(dtype='float'))
        
    # JD -> MJD here, on the way in. Mixing JD (2.46e6) and MJD (6e4) in jdmax would break
    # GREATEST(jdmax) and the deactivation clock for every object ZTF ever touches.
    out_df['jdmin'] = raw_data.get('first_jd', pd.Series(dtype='float')) - JD_TO_MJD
    out_df['jdmax'] = raw_data.get('latest_jd', pd.Series(dtype='float')) - JD_TO_MJD

    def is_number(x):
        try:
            return x is not None and math.isfinite(float(x))
        except (TypeError, ValueError):
            return False

    def band_dicts(row):
        """
        One band per alert (the latest detection). The DB keeps the true system: g_ztf / r_ztf.
        If there is no usable magnitude the dicts stay empty (no NaN, no placeholder).
        """
        if 'latestFilter' in row and 'latestMag' in row:
            band, mag = f"{row['latestFilter']}_ztf", row['latestMag']
        else:
            band, mag = ('g_ztf', row.get('gmag')) if is_number(row.get('gmag')) else ('r_ztf', row.get('rmag'))
        mjd = float(row['latest_jd']) - JD_TO_MJD if is_number(row.get('latest_jd')) else None
        if band in ('g_ztf', 'r_ztf') and is_number(mag) and mjd is not None:
            return {band: round(float(mag), 4)}, {band: mjd}, {band: 1}   # n_sources=1 is a placeholder (P1)
        return {}, {}, {}

    dicts = raw_data.apply(band_dicts, axis=1)
    out_df['latest_mag'] = [json.dumps(d[0], allow_nan=False) for d in dicts]
    out_df['latest_filter'] = [json.dumps(d[1], allow_nan=False) for d in dicts]
    out_df['n_sources'] = [json.dumps(d[2], allow_nan=False) for d in dicts]

    print(f"Processed {len(out_df)} transients from ZTF.")
    return out_df

def get_targets(pipeline_id, topic, group_id, max_messages=20000):
    """
    The main entry point for the controller.
    
    Connects to the stream, gets data, and processes it into a list of targets 
    ready for the controller.

    Returns:
        (pd.DataFrame, consumer): see tides_lsst.get_targets (same contract).
    """
    # 1. Connect to Lasair
    consumer = connect_lasair(topic, group_id)
    try:
        # 2. Get the latest batch of data
        latest_transients = get_latest_batch(consumer, max_messages=max_messages)

        # 3. Process and filter the data
        targets = process_transients(latest_transients, pipeline_id)
    except Exception:
        stream_utils.close_consumer(consumer)
        raise

    return targets, consumer

if __name__ == "__main__":
    from dotenv import load_dotenv
    # def load_credentials():
    #     """
    #     Loads 4MOST API credentials from a YAML file.
        
    #     This mirrors the 'connect4MOST_API' and 'loadTiDESdbSettings' logic from tidesCom.py.
    #     """
    #     global USERNAME, PASSWORD, SCHEMA, ACCESS_TOKEN
        
    #     # Load credentials from .env
    #     load_dotenv()
        
    #     USERNAME = os.getenv('FOURMOST_USERNAME')
    #     PASSWORD = os.getenv('FOURMOST_PASSWORD')
    #     SCHEMA = os.getenv('FOURMOST_SCHEMA')
    #     ACCESS_TOKEN = os.getenv('FOURMOST_ACCESS_TOKEN')
        
    #     # Check if critical credentials are loaded
    #     if not all([USERNAME, PASSWORD]):
    #         print("Warning: 4MOST credentials not found in .env")
        
    #     print("Loading credentials from environment variables...")
    #     return None
    # load_credentials()
    targets = get_targets()
    print(targets)
