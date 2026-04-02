# User Guide for `submit_transients.py`

This document goes through the steps to using the `submit_transients.py` tool. This tool allows you to submit and update transient targets to the 4FS Web Interface.

## 1. Setup & Authentication

Before performing any actions, you must import the module and set your authentication credentials. You can use either a username/password combination or an API access token.

```python
import submit_transients as st

# Option 1: Using Username & Password
st.USERNAME = 'your_username'
st.PASSWORD = 'your_password'

# Option 2: Using an Access Token (Recommended)
st.ACCESS_TOKEN = 'your_access_token'
```

## 2. Creating a Transient

To submit a new transient, you use the `create_transient` function. You need to provide a dictionary (payload) containing the transient's details.

### Step-by-Step:
1. Define your payload dictionary.
2. Call `st.create_transient(data=payload)`.

```python
# Define the payload. Not all columns are compulsory.
payload = {
    "uploadedfor_survey_id": 15, #This is TiDES-SN subsurvey ID
    "name": "LSSTTestChris", #This is the name that TiDES will use for internal tracking in our own database.
    "ra": 198.03, 
    "dec": -27.51,
    "pmra": 0.0, #Proper motion in RA
    "pmdec": 0.0, #Proper motion in DEC
    "epoch": 2000, #Epoch of coordinates e.g. J2000 (or 2016 if Gaia. What is LSST?)
    "resolution": 1, #Resolution of fibre LR=1, HR=2
    "subsurvey": "tides-sn", #Subsurvey ID (tides-sn, tides-host, tides-rm, source)
    "cadence": 1048576, #See 4FS instructions, tides-sn shoudl be 1048576
    "template": "SF_col00pt780_mass09pt88_sfr02pt00_z00pt225.fits", #These are any of the templates that Chris made for the simulations. If we didn't have control of the exposure times, this would be the template that we would use to determine the exposure times to meet our SSC.
    "ruleset": "tides_snMay2024", #Rulesets that Chris made for the simulations. ( e.g. tides_hostsMay2024, tides_snMay2024, tides_rmMay2024, sourceMay2024)
    # "redshift_estimate": None,
    # "redshift_error": None,
    "extent_flag": 0, #These extent_parameters describe the shape of an extended source, e.g. galaxy
    "extent_parameter": 0,
    "extent_index": 0,
    "mag": 21.8, #This is the magnitude of the transient
    # "mag_err": None,
    "mag_type": "LSST_r_AB", #This is the band of the magnitude columns e.g. LSST_r_AB
    # "reddening": None,
    # "date_earliest": None,
    # "date_latest": None,
    "t_exp_d": 38.0, #Exposure time for dark conditions (minutes)
    "t_exp_g": 38.0, #Exposure time for grey conditions (minutes)
    "t_exp_b": 38.0, #Exposure time for bright conditions (minutes)
    "t_exp_s": 38.0, #Exposure time for super bright conditions (minutes)
    # "template_redshift": None,
    # "cal_mag_blue": None,
    # "cal_mag_green": None,
    # "cal_mag_red": None,
    # "cal_mag_err_blue": None,
    # "cal_mag_err_blue": None,
    # "cal_mag_err_green": None,
    # "cal_mag_id_green": None,
    # "cal_mag_id_red": None,
    # "cal_mag_id_red": None,
    "classification": "TRA", #This is the classification of the target. Up. to 3 characters and 3 classifications. e.g "GAL, AGN", "GAL", "TRA", "LEN"
    # "completeness": None,
    # "parallax": None,
    "is_active": True, #This is the flag that determines if 4MOST will select the target.
}

# Submit the transient
st.create_transient(data=payload)
```

## 3. Creating & Submitting JSON Files

You can also define your payload in a separate JSON file and load it into your script. This helps keep your Python code clean and allows you to manage templates easily.

### The Default Payload Structure
Below is the standard structure for a payload. You can save this as `transient_payload.json`. I haven't bothered to write out all the optional fields.

```json
{
    "uploadedfor_survey_id": 15,
    "name": "TEMPLATE_NAME",
    "ra": 0.0,
    "dec": 0.0,
    "pmra": 0.0,
    "pmdec": 0.0,
    "epoch": 2021,
    "resolution": 1,
    "subsurvey": "SUBSURVEY",
    "cadence": 0,
    "template": "TEMPLATE_NAME",
    "ruleset": "RULESET_NAME",
    "extent_flag": 0,
    "extent_parameter": 0,
    "extent_index": 0,
    "mag": 20,
    "mag_type": "MAG_TYPE",
    "t_exp_d": 20.0,
    "t_exp_g": 15.0,
    "t_exp_b": 7.0,
    "t_exp_s": 4.0,
    "classification": "TRA",
    "is_active": true
}
```

### Loading and Submitting the JSON File

```python
import json
import submit_transients as st

# Setup credentials
st.ACCESS_TOKEN = 'your_access_token'

# Load the JSON data
with open('transient_payload.json', 'r') as f:
    payload = json.load(f)

# Modify fields if necessary (e.g., give it a unique name)
payload['name'] = "MyNewTransient"

# Submit
st.create_transient(data=payload)
```

## 4. Updating a Transient (Deactivating)

The `update_transient` function allows you to modify existing transients. The most common use case is deactivating a transient that should no longer be observed.

**Important:** You must know the `id` of the transient you want to update. This `id` is returned when you first run `create_transient`, or can be found by listing transients.

### Deactivating a Target (`is_active` = False)

To stop a target from being considered for observation, change its `is_active` flag to `False`.

```python
# The ID of the transient you want to update
transient_id = 12345 

# The data to update (only need to include the fields changing)
update_data = {
    'is_active': False
}

# Perform the update
st.update_transient(pk=transient_id, data=update_data)
```
