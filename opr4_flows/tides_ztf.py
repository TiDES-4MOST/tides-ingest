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
import os
import lasair  # Uncomment when lasair package is available
import numpy as np
from prefect.artifacts import create_markdown_artifact
from prefect import flow, task
from datetime import datetime

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
    Establishes a connection to the Lasair API.

    Returns:
        lasair_consumer object: The consumer object to poll for messages.
    """
    token = os.getenv('LASAIR_ZTF_TOKEN')
    
    # Check if credentials are set
    if not all([topic, group_id, token]):
        print("Warning: Lasair ZTF credentials not fully set in .env")

    # TODO: Initialize Lasair consumer
    consumer = lasair.lasair_consumer('kafka.lsst.ac.uk:9092', group_id, topic)
    
    print(f"Connecting to Lasair topic {topic} with group {group_id}...")
    return consumer

def get_latest_batch(consumer):
    """
    Polls the Lasair Kafka stream for new transient events.

    Args:
        consumer: The Lasair consumer object.

    Returns:
        pd.DataFrame: A DataFrame containing the recent objects from the stream.
    """
    recentObjects = pd.DataFrame()
  
    while True:
        msg = consumer.poll(timeout=5) #The kafka poll will wait 5 seconds to hear back. If nothing is delivered the pipeline will end and only objects 
        if msg is None:
            print('no more transients')
            break
        
        if msg.error():
            print(str(msg.error()))
            break
        jmsg = json.loads(msg.value())
        #print('jmsg: ',jmsg)
        mostRecentComm = pd.DataFrame(jmsg, columns=jmsg.keys(), index=[0])
        recentObjects = pd.concat([recentObjects,mostRecentComm], ignore_index=True)
    #print('Length Recent Objects: ', len(recentObjects))
    if len(recentObjects)!=0:
        recentUniqueObjects = recentObjects.sort_values("jdmax", ascending = False).drop_duplicates(subset=["objectId"], inplace=False, keep="first")
    else: recentUniqueObjects = recentObjects
    #print('Recent ZTF Object: ',recentObjects)

    ingest_report(recentUniqueObjects)

    return recentUniqueObjects
    

def process_transients(raw_data, pipeline_id):
    """
    Filters and formats the raw transient data to meet the standard
    tides-ingest contract.
    """
    if raw_data is None or raw_data.empty:
        return pd.DataFrame(columns=['object_id', 'survey_id', 'ra', 'dec', 'jdmin', 'jdmax', 'latest_filter', 'latest_mag'])

    out_df = pd.DataFrame()
    out_df['object_id'] = raw_data.get('objectId', raw_data.get('id', pd.Series(dtype='str')))
    out_df['survey_id'] = 2  # integer id for ZTF
    out_df['pipeline_id'] = pipeline_id # Setting dynamic pipeline ID from args
    out_df['ra'] = raw_data.get('ra', pd.Series(dtype='float'))
    
    if 'decl' in raw_data.columns:
        out_df['dec'] = raw_data['decl']
    else:
        out_df['dec'] = raw_data.get('dec', pd.Series(dtype='float'))
        
    out_df['jdmin'] = raw_data.get('jdmin', pd.Series(dtype='float'))
    out_df['jdmax'] = raw_data.get('jdmax', pd.Series(dtype='float'))

    # Determine filter key
    if 'latestFilter' in raw_data.columns and 'latestMag' in raw_data.columns:
         filter_key = raw_data['latestFilter'].astype(str) + '_ztf'
         raw_mag = raw_data['latestMag'].astype(float)
    elif 'gmag' in raw_data.columns and 'rmag' in raw_data.columns:
         raw_mag = raw_data['gmag'].fillna(raw_data['rmag'])
         filter_key = np.where(raw_data['gmag'].notnull(), 'g_ztf', 'r_ztf')
    else:
         filter_key = pd.Series('unknown_ztf', index=raw_data.index)
         raw_mag = pd.Series(29.99, index=raw_data.index)

    # Need json string representations of dicts for postgres
    import json
    out_df['latest_filter'] = raw_data.apply(lambda row: json.dumps({
        filter_key.loc[row.name]: row.get('jdmax', 0)
    }), axis=1)
    
    out_df['latest_mag'] = raw_data.apply(lambda row: json.dumps({
        filter_key.loc[row.name]: raw_mag.loc[row.name]
    }), axis=1)

    out_df['n_sources'] = raw_data.apply(lambda row: json.dumps({
        filter_key.loc[row.name]: 1
    }), axis=1)

    print(f"Processed {len(out_df)} transients from ZTF.")
    return out_df

def get_targets(pipeline_id, topic, group_id):
    """
    The main entry point for the controller.
    
    Connects to the stream, gets data, and processes it into a list of targets 
    ready for the controller.

    Returns:
        list: A list of dict-like objects representing the targets and their properties.
    """
    # 1. Connect to Lasair
    consumer = connect_lasair(topic, group_id)
    
    # 2. Get the latest batch of data
    latest_transients = get_latest_batch(consumer)
    
    # 3. Process and filter the data
    targets = process_transients(latest_transients, pipeline_id)

    
    return targets

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
