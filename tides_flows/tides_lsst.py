"""
tides_lsst.py

This module contains the logic for connecting to the Lasair API and importing transients from a stream.
It is designed to be imported by opr4_controller.py.

Usage:
    import tides_lsst
    targets = tides_lsst.get_targets()
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

LSST_KAFKA = 'lasair-lsst-kafka.lsst.ac.uk:9092'
LSST_BANDS = ('g', 'r', 'i', 'z')   # u and y are deliberately ignored

@task
def ingest_report(recentUniqueObjects):
    """
    Generates a markdown report for the LSST transients.
    
    Args:
        recentUniqueObjects (pd.DataFrame): The DataFrame containing the recent LSST objects.
    """
    
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    

    markdown_report = f"# TiDES LSST Stream Report\n"
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

    markdown_report += generate_section("LSST Transients", recentUniqueObjects)

    # If report is empty (besides header), mention it
    if len(markdown_report.split('\n')) <= 4:
         markdown_report += "_No new transients in this batch._"

    # Create the artifact
    artifact_key = f"tides-lsst-stream-report"

    create_markdown_artifact(
        key=artifact_key,
        markdown=markdown_report,
        description=f"TiDES LSST Stream Report - {date_str}"
    )

    return markdown_report

def connect_lasair(topic, group_id):
    """
    Opens a Kafka consumer on the Lasair LSST topic (auto-commit OFF, see stream_utils.py).

    Returns:
        confluent_kafka.Consumer
    """
    if not all([topic, group_id]):
        print("Warning: Lasair LSST topic/group id not fully set in .env")
    return stream_utils.open_consumer(LSST_KAFKA, topic, group_id)

def get_latest_batch(consumer, max_messages=20000):
    """
    Reads the Lasair Kafka stream until it is idle (or max_messages).

    Args:
        consumer: the Kafka consumer from connect_lasair().

    Returns:
        pd.DataFrame: one row per diaObjectId (its most recent alert in this batch).
    """
    records, n_skipped = stream_utils.read_messages(consumer, id_key='diaObjectId', max_messages=max_messages)
    recentObjects = pd.DataFrame.from_records(records)   # built once, not concatenated per message
    if len(recentObjects) != 0:
        recentUniqueObjects = recentObjects.sort_values("lastDiaSourceMjdTai", ascending=False).drop_duplicates(subset=["diaObjectId"], keep="first")
    else:
        recentUniqueObjects = recentObjects

    ingest_report(recentUniqueObjects)

    return recentUniqueObjects


def process_transients(raw_data, pipeline_id):
    """
    Filters and formats the raw transient data to meet the standard
    tides-ingest contract.

    Here are the columns from the LSST stream:
        objects.diaObjectId,
        objects.ra,
        objects.decl,
        objects.lastDiaSourceMjdTai,
        objects.firstDiaSourceMjdTai,
        objects.latest_psfFlux,
        objects.g_psfFlux,
        objects_ext.g_psfFluxSigma,
        objects.g_latestMJD,
        objects.r_psfFlux,
        objects_ext.r_psfFluxSigma,
        objects.r_latestMJD,
        objects.i_psfFlux,
        objects_ext.i_psfFluxSigma,
        objects.i_latestMJD,
        objects.z_psfFlux,
        objects_ext.z_psfFluxSigma,
        objects.z_latestMJD,
        objects.nPosDiaSources,
        objects.nPosDiaSourcesNights,
        objects.ngSources,
        objects.nrSources,
        objects.niSources,
        objects.nzSources,
        objects.nPosDiaSourcesNights,
        sherlock_classifications.classification as sherlock_classifications
    """
    if raw_data is None or raw_data.empty:
        return pd.DataFrame(columns=stream_utils.CONTRACT_COLUMNS)   # typed empty frame, never None

    # Now we need to make sure the data frame columns are reformatted to match the tides-ingest contract
    # The columns we need are:
    # object_id
    # survey_id
    # ra
    # dec
    # jdmin
    # jdmax
    # latest_filter - this is a JSON array
    # latest_mag - this is a JSON array
    # n_sources - this is a JSON array

    out_df = pd.DataFrame()
    # str() of an int64 column: diaObjectIds are > 2**53, so they must never pass through float
    out_df['object_id'] = raw_data['diaObjectId'].astype('int64').astype(str)
    out_df['survey_id'] = 1  # integer id for LSST
    out_df['pipeline_id'] = pipeline_id # Setting dynamic pipeline ID from args
    out_df['ra'] = raw_data.get('ra', pd.Series(dtype='float'))
    
    if 'decl' in raw_data.columns:
        out_df['dec'] = raw_data['decl']
    else:
        out_df['dec'] = raw_data.get('dec', pd.Series(dtype='float'))
        
    out_df['jdmin'] = raw_data.get('firstDiaSourceMjdTai', pd.Series(dtype='float'))   # MJD (TAI)
    out_df['jdmax'] = raw_data.get('lastDiaSourceMjdTai', pd.Series(dtype='float'))    # MJD (TAI)

    def is_number(x):
        try:
            return x is not None and math.isfinite(float(x))
        except (TypeError, ValueError):
            return False

    def band_dicts(row):
        """
        Per-band dicts for one alert. A band only gets a magnitude/MJD when it has a real,
        positive flux AND a date. (Before: missing flux became mag 56.4 and missing date 0.0,
        which looked like data, and 56.4 is outside 4MOST's allowed MAG range.)
        """
        mags, mjds, counts = {}, {}, {}
        for b in LSST_BANDS:
            key = f'{b}_lsst'
            n = row.get(f'n{b}Sources')
            counts[key] = int(n) if is_number(n) else 0
            flux, mjd = row.get(f'{b}_psfFlux'), row.get(f'{b}_latestMJD')
            if is_number(flux) and float(flux) > 0 and is_number(mjd) and float(mjd) > 0:
                mags[key] = round(-2.5 * math.log10(float(flux)) + 31.4, 4)   # nJy -> AB mag
                mjds[key] = float(mjd)
        return mags, mjds, counts

    dicts = raw_data.apply(band_dicts, axis=1)
    # allow_nan=False: a NaN would make PostgreSQL reject the jsonb and roll back the whole survey
    out_df['latest_mag'] = [json.dumps(d[0], allow_nan=False) for d in dicts]
    out_df['latest_filter'] = [json.dumps(d[1], allow_nan=False) for d in dicts]   # band -> MJD of that mag
    out_df['n_sources'] = [json.dumps(d[2], allow_nan=False) for d in dicts]

    print(f"Processed {len(out_df)} transients from LSST.")
    return out_df

def get_targets(pipeline_id, topic, group_id, max_messages=20000):
    """
    The main entry point for the controller.
    
    Connects to the stream, gets data, and processes it into a list of targets 
    ready for the controller.

    Returns:
        (pd.DataFrame, consumer): the targets, and the still-open Kafka consumer.
        The CALLER must call stream_utils.commit_offsets(consumer) after its database commit,
        and stream_utils.close_consumer(consumer) in all cases.
    """
    # 1. Connect to Lasair
    consumer = connect_lasair(topic, group_id)
    try:
        # 2. Get the latest batch of data
        latest_transients = get_latest_batch(consumer, max_messages=max_messages)

        # 3. Process and filter the data
        targets = process_transients(latest_transients, pipeline_id)
    except Exception:
        stream_utils.close_consumer(consumer)   # nothing committed: the next run re-reads these alerts
        raise

    return targets, consumer

if __name__ == "__main__":
    from dotenv import load_dotenv
    def load_credentials():
        """
        Loads 4MOST API credentials from a YAML file.
        
        This mirrors the 'connect4MOST_API' and 'loadTiDESdbSettings' logic from tidesCom.py.
        """
        global USERNAME, PASSWORD, SCHEMA, ACCESS_TOKEN
        
        # Load credentials from .env
        load_dotenv()
        
        USERNAME = os.getenv('FOURMOST_USERNAME')
        PASSWORD = os.getenv('FOURMOST_PASSWORD')
        SCHEMA = os.getenv('FOURMOST_SCHEMA')
        ACCESS_TOKEN = os.getenv('FOURMOST_ACCESS_TOKEN')
        
        # Check if critical credentials are loaded
        if not all([USERNAME, PASSWORD]):
            print("Warning: 4MOST credentials not found in .env")
        
        print("Loading credentials from environment variables...")
        return None
    load_credentials()
    targets = get_targets()
    print(targets)
