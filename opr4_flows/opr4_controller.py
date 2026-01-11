"""
opr4_controller.py

This file is the specific implementation of the "Controller" process for the 4MOST operational rehearsal.
It uses Prefect to orchestrate the workflow and manages 4MOST credentials.

Usage:
    prefect deployment run 'run-opr4-workflow/main'
    # OR python opr4_controller.py
"""

from prefect import flow, task
# from prefect_dask import DaskTaskRunner # Optional: for parallel execution
import yaml
import os
from dotenv import load_dotenv
import opr4_ztf # Import the data source module

# Global config placeholders
# 4MOST API Credentials
USERNAME = None
PASSWORD = None
SCHEMA = None
ACCESS_TOKEN = None

@task
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

@task
def sqlalchemy_credentials_flow():
    dbUsername = os.getenv('TIDES_DB_USER')
    dbPassword = os.getenv('TIDES_DB_PASS')
    dbDatabase = os.getenv('TIDES_DB_DATABASE')
    sqlalchemy_credentials = DatabaseCredentials(
        driver=AsyncDriver.POSTGRESQL_ASYNCPG,
        username=dbUsername,
        password=dbPassword,
        database=dbDatabase,
        host="localhost",
        port=5432,
    )
    print(sqlalchemy_credentials.get_engine())
    return sqlalchemy_credentials.get_engine()

@task
def sqlalchmey_engine():
  dbUsername = os.getenv('TIDES_DB_USER')
  dbPassword = os.getenv('TIDES_DB_PASS')
  dbDatabase = os.getenv('TIDES_DB_DATABASE')
  url = 'postgresql+psycopg2://'+str(dbUsername)+':'+str(dbPassword)+'@localhost:5432/'+str(dbDatabase)
  engine = sqlalchemy.create_engine(url,future=True)
  return engine

@task
def fetch_targets():
    """
    Calls the opr4_ztf module to get the latest list of targets.
    """
    print("Fetching targets from opr4_ztf...")
    targets = opr4_ztf.get_targets()
    return targets

@task
def submit_to_4most(targets):
    """
    Iterates through the targets and submits them to the 4MOST facility.
    
    Args:
        targets: List of targets to submit.
    """
    # TODO: Initialize 4MOST API connection using loaded credentials
    
    # TODO: Loop through targets and format for 4MOST submission
    # for target in targets:
    #     upload_params = format_for_upload(target)
    #     result = st.create_transient(data=upload_params)
    
    print(f"Submitting {len(targets)} targets to 4MOST...")
    pass

@flow(name="OPR4 Workflow")
def run_opr4_workflow():
    """
    The main Prefect flow for the OPR4 process.
    """
    # 1. Load configuration and credentials
    load_credentials()
    
    # 2. Fetch targets from the ZTF stream (via opr4_ztf)
    targets = fetch_targets()
    
    engine = sqlalchmey_engine() ## Create the connection to the TiDES DB

    createTransientStage(targets, engine) ## Create a temporary table for the recent detections
    #Starting the session with the local TiDES Database
    with engine.connect() as conn, conn.begin() :
        upsertToMaster(conn)
        deactivateUnobservedTransients(conn)
        toUpdate = prepare4MOSTUpdate(conn)
        print('New Transients',len(toUpdate[toUpdate['pk_4most'].isnull()]))
        print('Updating Transients',len(toUpdate[~toUpdate['pk_4most'].isnull()]))

if __name__ == "__main__":
    run_opr4_workflow()
