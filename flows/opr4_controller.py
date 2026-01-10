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
    
    # TODO: Load credentials from a secure file (e.g., 4mostAPIDetails.yaml)
    # settings = yaml.load(open('./4mostAPIDetails.yaml'), Loader=yaml.SafeLoader)
    # USERNAME = settings['connect']['username']
    # ...
    
    print("Loading credentials...")
    return None

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
    
    # 3. Submit viable targets to 4MOST
    if targets:
        submit_to_4most(targets)
    else:
        print("No targets found to submit.")

if __name__ == "__main__":
    run_opr4_workflow()
