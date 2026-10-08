# Testing tides-ingest

These tests run the real pipeline code against a **local test database**, with mock alert streams and an in-memory fake 4MOST. Nothing here talks to the real 4MOST.

For the full picture (what each step of the pipeline does and why), see [../USER_GUIDE.md](../USER_GUIDE.md), especially §7 and §13.

## Before you start

1. **A test database.**
   - Local (`localhost`), with `test` or `mock` in its name (e.g. `tides_dev_test`), and the q3c extension.
   - The tests **truncate** it, so never point them at anything you care about.
   - `test_4most_downtime.py` creates the schema and migrations itself on an empty database. Otherwise load them with `database_setup/setupDB.sh` + `migrate.sh`.
2. In `.env`:
   ```
   TIDES_TEST_DB_DATABASE=tides_dev_test
   TIDES_ENV=development
   ```
3. Run everything **from the repository root**.

> ⚠️ Do **not** use `database_setup/databaseHardReset.*` to prepare a test. It resets whatever `TIDES_DB_DATABASE` points at, which is usually your dev database (see USER_GUIDE.md §14).

## The tests

| File | Run | What it checks |
|---|---|---|
| `test_4most_downtime.py` | `python testing/test_4most_downtime.py` | Mock ingest (3 objects, all pending). Then 4MOST **down**: nothing marked synced. Then a POST whose response is lost: rows `ambiguous`. Then recovery: rows found by name and adopted, **no duplicates**. Also asserts every request went to the fake. |
| `test_host_matching.py` | `python testing/test_host_matching.py` | Host association, catalogue dedup and the four queue scenarios. Needs **live Lasair Sherlock** and a working token/endpoint pair (USER_GUIDE.md §9). 4MOST is faked. |
| mock pipeline | `TIDES_DB_DATABASE=tides_dev_test python -c "import sys; sys.path.insert(0,'tides_flows'); import tides_controller as t; t.run_target_workflow(test_mode=True)"` | Matching (an LSST and a ZTF alert 0.5″ apart merge into one object), events, survey clock, deactivation. `test_mode` refuses databases without `test`/`scratch`/`mock` in the name, and never writes to 4MOST. |

None of these are pytest-collectable yet: they are scripts that print `PASSED` or `FAILURE` and exit non-zero on failure.

## Building blocks

- **`mock_streams.py`**: `generate_mock_lsst_targets()` / `generate_mock_ztf_targets()` return DataFrames in the plugin contract (USER_GUIDE.md §7.4). To add a case, add a row:
  - **MJD** times in the **past** (the validator drops JD values and future dates);
  - band keys like `r_lsst` / `g_ztf`;
  - `json.dumps(...)` for the three JSON columns, with no NaN.

  An object is deactivated if its survey has more than `DEACTIVATE_AFTER_DAYS` observing nights (in the mock data) after its last detection. Keep test dates close together unless that's what you are testing.
- **`fake_4most.py`**: `fake = fake_4most.install()` routes every `submit_transients` request to an in-memory fake.
  - `fake.fail_mode = None | 'down' | 'lose_response'`.
  - `fake.targets` holds what was "created"; `fake.calls` holds every request.
  - Use it for any test that involves 4MOST.

## Never

- Point a test at the real 4MOST. The old version of these tests created mock targets in the production queue.
- Run tests against a remote database, or one whose name doesn't contain `test`/`mock`.
