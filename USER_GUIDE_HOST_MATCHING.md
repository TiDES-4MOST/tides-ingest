# TiDES Host Galaxy Matching & Follow-up Queue User Guide

This guide describes how to run and deploy the standalone Host Galaxy Matching and 4MOST Follow-up Queue pipeline for the TiDES project.

---

## 1. Overview
The host matching pipeline identifies host galaxies for discovered transients. It is orchestrated via **Prefect** and queries the **Lasair Sherlock position API** for active transients. It also dynamically manages the host follow-up queue, registering qualifying host galaxies with the 4MOST API under the `"tides-host"` subsurvey.

### Key Features:
- **Deduplication**: Galaxies are cataloged exactly once in `tides_host_catalog` (with an auto-incrementing, indexed serial `host_id`). Multiple transients associated with the same physical galaxy reference this single entry.
- **Sherlock Ranking**: The pipeline filters matching candidates where `catalogue_object_type = 'galaxy'` and saves up to the top 3 matches (assigning ranks 1, 2, and 3 based on Sherlock proximity/association likelihood).
- **Self-Healing / Re-runnable**: Instead of using fragile timestamps, the pipeline queries active transients that *do not currently have association matches* for the specified pipeline ID. Any transient that failed to match due to temporary network issues or API downtime will automatically be retried on the next run.
- **Dynamic 4MOST Follow-up Queue**: The pipeline automatically evaluates which host galaxies should be scheduled for spectroscopic follow-up on 4MOST.
  - **Deactivation Protection**: A host galaxy is queued *only* when the associated transient has faded and is no longer active (`tides_master.active = False`) to prevent bright transient light from contaminating the galaxy spectrum or wasting observing resources.
  - **Target Selection Criteria**: The inactive transient must satisfy at least one of:
    1. **Unobserved**: The transient hasn't been observed in the last 50 days (MJD difference $> 50$ days).
    2. **Faint LSST**: The transient has been observed recently, but *all* its recorded LSST filter detections are fainter than 24 mag (value $> 24.0$).
  - **4MOST API Sync**: Qualifiers are automatically registered with the 4MOST API (subsurvey `"tides-host"`, ruleset `"tides_hostsMay2024"`, classification `"GAL"`). If a queued host target later becomes ineligible (e.g. the transient becomes active again), it is updated on 4MOST as `is_active = False` to release telescope resources.

---

## 2. Database Schema
The database architecture is structured as follows:

### `tides_host_catalog`
Stores unique physical host galaxies:
- `host_id` (`BIGSERIAL PRIMARY KEY`): Auto-incrementing unique host identifier.
- `host_name` (`VARCHAR UNIQUE NOT NULL`): The main catalog ID returned by Sherlock.
- `ra`, `dec` (`double precision`): Sky coordinates of the galaxy.
- `mag` (`JSONB`): Magnitude metadata containing filter, value, and error.

### `tides_host`
Maps transients to host galaxy candidates and tracks their 4MOST queue states:
- `association_id` (`BIGSERIAL PRIMARY KEY`).
- `tides_id` (`BIGINT REFERENCES tides_master`): Link to the matching transient.
- `host_id` (`BIGINT REFERENCES tides_host_catalog`): Link to the host galaxy.
- `rank` (`INT`): Sherlock proximity rank (1 to 3).
- `selection_fn` (`INT REFERENCES pipelines`): Link to the pipeline run registration.
- `metadata` (`JSONB`): Detailed metadata including classification, separation, photoZ, redshift, and catalog source.
- `pk_4most` (`BIGINT`): The returned 4MOST target ID.
- `sync_pending` (`BOOLEAN`): Set to `True` when a state change (activation/deactivation) needs to be synced to 4MOST.
- `active` (`BOOLEAN`): Current follow-up queue state.

---

## 3. Configuration
Ensure the `.env` file in the root of the project contains the following environment variables:

```env
# Database Configuration
TIDES_DB_USER='username'
TIDES_DB_PASS='password'
TIDES_DB_DATABASE='dopr4_tides'
TIDES_DB_PORT='5432'
TIDES_DB_HOST='127.0.0.1'

# Lasair Token (used to authenticate Sherlock queries)
LASAIR_LSST_TOKEN='your_lsst_token_here'
LASAIR_ZTF_TOKEN='your_ztf_token_here'

# 4MOST API Credentials (for host submission)
FOURMOST_USERNAME='username'
FOURMOST_PASSWORD='password'
FOURMOST_SCHEMA="https://4most.mpe.mpg.de/QFSwi/targetCat/transients/"
FOURMOST_ACCESS_TOKEN='token'
```

---

## 4. Running the Pipeline

### Local Execution (Standard)
To run the matching and queue update locally with default settings (pipeline `'sherlock'`, version `'v1.0'`):
```bash
python host_tools/tides_host_matching/tides_match_gal_local.py
```

### Local Execution (Custom Pipeline / Version)
To parameterize the lookup under a different pipeline name or version (e.g. for testing a new model version):
```bash
python host_tools/tides_host_matching/tides_match_gal_local.py --pipeline-name my-custom-pipeline --pipeline-version v2.0
```

---

## 5. Prefect Deployment
To build and deploy the host matching pipeline as a standalone Prefect deployment:

1. **Build the Deployment**:
   ```bash
   prefect deployment build -n tides-galaxy-matching -p default host_tools/tides_host_matching/tides_match_gal_local.py:match_galaxies_flow
   ```
2. **Apply the Deployment**:
   ```bash
   prefect deployment apply match_galaxies_flow-deployment.yaml
   ```
3. **Schedule / Run**:
   Schedule execution inside the Prefect UI or configure a cron interval in the deployment YAML.

---

## 6. Testing & Verification
An automated integration test is available in `testing/test_host_matching.py`. This script:
1. Wipes the test database (`dopr4_tides_test`).
2. Inserts active transients and runs matching to associate them with host galaxies.
3. Transitions the transient active states to trigger the follow-up scenarios:
   - **Scenario 1**: Active transient (should not queue the host).
   - **Scenario 2**: Inactive transient unobserved for $>50$ days (should queue and register the host).
   - **Scenario 3**: Inactive transient with all LSST detections $>24$ mag (should queue and register the host).
   - **Scenario 4**: Inactive transient with a bright LSST detection $\le 24$ mag (should not queue the host).
4. Executes the evaluation and 4MOST sync tasks.
5. Verifies database mappings and ensures all transients deduplicate to a single catalog host entry.

To run the test:
```bash
python testing/test_host_matching.py
```
