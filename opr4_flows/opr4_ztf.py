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

def connect_lasair():
    """
    Establishes a connection to the Lasair API.

    Returns:
        lasair_consumer object: The consumer object to poll for messages.
    """
    # Load configuration settings from environment variables
    topic = os.getenv('LASAIR_ZTF_TOPIC')
    #group_id = os.getenv('LASAIR_ZTF_GROUP_ID') # TODO: Uncomment for production
    group_id = 'opr4'+str(np.random.randint(0, 1000))
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
        mostRecentComm = pd.DataFrame(jmsg, columns=jmsg.keys(), index=[0])
        recentObjects = pd.concat([recentObjects,mostRecentComm], ignore_index=True)
    #print('Length Recent Objects: ', len(recentObjects))
    if len(recentObjects)!=0:
        recentUniqueObjects = recentObjects.sort_values("jdmax", ascending = False).drop_duplicates(subset=["objectId"], inplace=False, keep="first")
    else: recentUniqueObjects = recentObjects
    #print(recentObjects)
    return recentUniqueObjects
    

def process_transients(raw_data):
    """
    Filters and formats the raw transient data.
    
    This is where you would apply selection criteria (e.g., magnitude limits, 
    detection counts) similar to lightcurveSatify in tidesCom.py.

    Args:
        raw_data (pd.DataFrame): Raw data from the stream.

    Returns:
        list: A list of filtered and processed targets.
    """
    # TODO: Apply selection criteria
    # for index, row in raw_data.iterrows():
    #     if check_criteria(row):
    #         pass
            
    print("Processing transients...")
    return raw_data # Placeholder

def get_targets():
    """
    The main entry point for the controller.
    
    Connects to the stream, gets data, and processes it into a list of targets 
    ready for the controller.

    Returns:
        list: A list of dictionary objects representing the targets and their properties.
    """
    # 1. Connect to Lasair
    consumer = connect_lasair()
    
    # 2. Get the latest batch of data
    latest_transients = get_latest_batch(consumer)
    
    # 3. Process and filter the data
    targets = process_transients(latest_transients)
    
    return targets

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
