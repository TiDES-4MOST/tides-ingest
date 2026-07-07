# TiDES Ingest Testing Framework

This directory contains the tools necessary to safely simulate and test the ingestion pipeline deterministically, without relying on live Kafka data streams.

## Overview
Because live transient streams (like LSST and ZTF) rarely emit edge cases on demand, we use **Dependency Injection** via a `test_mode` parameter in the main controller. When enabled, the pipeline bypasses Kafka and reads directly from hardcoded pandas DataFrames in `mock_streams.py`.

## Adding New Test Cases
To test a new edge case (e.g., missing filter data, extreme dates, spatial proximity):
1. Open `testing/mock_streams.py`.
2. Add a new dictionary object to the `data` list in either `generate_mock_lsst_targets()` or `generate_mock_ztf_targets()`.
3. Ensure you follow the strict database schema (e.g., wrapping magnitude dictionaries in `json.dumps()`).

**Example Spatial Cross-Match Test:**
To test the 1-arcsecond spatial cross-match, we created an LSST object at `RA=150.0, Dec=2.0` in the mock data. Then, we created a ZTF object at `RA=150.0001, Dec=2.0001`. The SQL pipeline automatically detects they are the same physical transient and merges their JSONB magnitude data in the `tides_master` table.

## Standard Testing Workflow

### 1. Wipe the Database (Nuclear Reset)
Before running a test, ensure your test database (`dopr4_tides`) is completely clean so old data doesn't artificially trigger cross-matches or unique constraint errors. 
From the project root, run:
```bash
python database_setup/databaseHardReset.py
# Or use the bash equivalent: ./database_setup/databaseHardReset.sh
```
*(Note: It will aggressively warn you and ask you to type `RESET` to confirm).*

### 2. Enable Test Mode
At the bottom of `opr4_flows/tides_controller.py`, ensure the controller is called with `test_mode=True`:
```python
if __name__ == "__main__":
    run_target_workflow(connect_db=True, test_mode=True)
```

### 3. Run the Pipeline
Run the controller exactly as you normally would:
```bash
python opr4_flows/tides_controller.py
```
You will see `=== RUNNING IN TEST MODE ===` in the console.

### 4. Verify the Results
Inspect your database using a GUI (like pgAdmin or DBeaver) or a database CLI to verify the edge cases resolved correctly:
- **`tides_master` Table**: Ensure the correct number of unique transients were created (e.g., matching transients shouldn't create duplicate rows).
- **`surveys` Junction Table**: Check to ensure multi-survey objects (like our ZTF/LSST spatial match) are correctly mapped. You should see two different `source_survey_id` entries linked to the single `tides_id`.
- **JSONB Columns**: Check the `latest_mags` column to ensure records successfully `COALESCE` and merge incoming dictionary keys without overwriting other survey filter bands.

---

## 4MOST Downtime & Fallback Testing

To verify the resilience of the pipeline when the 4MOST API is down, you can run the automated integration test:

```bash
python testing/test_4most_downtime.py
```

### Manual Downtime Verification Workflow
If you want to manually test the fallback behavior of the orchestrator, follow these steps:

1. **Point 4MOST to a Bogus Host**:
   In your python environment or test runner, mock the schema URL variable on the `submit_transients` module before running the workflow:
   ```python
   import submit_transients as st
   st.URL_SCHEMA = "https://invalid-host-4most-down.mpe.mpg.de/"
   ```
2. **Execute the Pipeline**:
   Run the controller normally (or in `test_mode`). The pipeline will complete successfully without raising exceptions.
3. **Verify Local Database State**:
   Inspect the `tides_master` table:
   - Transients are fully created/updated.
   - The `sync_pending` column is `True`.
   - The `pk_4most` column is `null`.
4. **Restore Connection & Sync**:
   Restore the correct `st.URL_SCHEMA` to point to the active 4MOST API, and run the sync task:
   ```python
   with engine.connect() as conn:
       tides_controller.sync_pending_to_4most(conn)
       conn.commit()
   ```
5. **Verify Final State**:
   Check the `tides_master` table again:
   - The `sync_pending` column has cleared to `False`.
   - The `pk_4most` column is updated with the returned 4MOST IDs.

