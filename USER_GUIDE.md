# tides-ingest: User and Maintainer Guide

This is the single reference for running, operating and changing the TiDES ingest pipeline. It is written so that someone who has never seen the code can take it over.

> **Verified against:** branch `delta_opr4` at `8e4c1d5`, plus the October 2026 review patch (Kafka offsets, 4MOST outbox, upsert rewrite, survey clock, milestone magnitudes, provenance tables). Last checked 2026-10-08.
> **If you change behaviour, update this guide in the same commit.** A guide that disagrees with the code is worse than none.

**How to read it**

| You want to… | Read |
|---|---|
| Understand what this is | §1 Purpose, §2 Concepts, §3 Architecture |
| Get it running on a new machine | §5 Quick start |
| Run it in production | §6 Configuration, §11 Operations, §12 Runbooks |
| Fix something that went wrong | §16 Troubleshooting, §12 Runbooks |
| Change the code | §7 Ingest in detail, §13 Testing, §15 Known limitations |
| Answer "why was this object observed?" | §10 Provenance |

**Conventions**
- Times are **MJD** unless stated. The only exceptions are the 4MOST date fields, which are JD (§8.3).
- Magnitudes are AB. Positions are decimal degrees (J2000/ICRS).
- Paths are relative to the repository root.
- **Run every command from the repository root** unless a section says otherwise. The code opens `./sql_tasks/*.sql` with relative paths.

---

## Contents

1. [What this is and why it exists](#1-what-this-is-and-why-it-exists)
2. [Key concepts](#2-key-concepts)
3. [Architecture](#3-architecture)
4. [Repository tour](#4-repository-tour)
5. [Quick start](#5-quick-start)
6. [Configuration reference](#6-configuration-reference)
7. [The ingest flow in detail](#7-the-ingest-flow-in-detail)
8. [4MOST integration](#8-4most-integration)
9. [Host-galaxy flow](#9-host-galaxy-flow)
10. [Provenance and the selection function](#10-provenance-and-the-selection-function)
11. [Operations](#11-operations)
12. [Runbooks](#12-runbooks)
13. [Testing](#13-testing)
14. [Safety rules](#14-safety-rules)
15. [Known limitations and open work](#15-known-limitations-and-open-work)
16. [Troubleshooting](#16-troubleshooting)
17. [Glossary](#17-glossary)
18. [History and references](#18-history-and-references)

---

## 1. What this is and why it exists

**TiDES** (the Time-Domain Extragalactic Survey) is the 4MOST survey that takes spectra of extragalactic transients, mostly supernovae, while they are bright. It later takes spectra of their **host galaxies**, once the transient has faded.

**4MOST** is a multi-object fibre spectrograph on ESO's VISTA telescope. Its scheduler, **OpSys**, picks targets from the **OSTD** (Operational Survey Target Database). Surveys feed the OSTD through the 4MOST web API.

4MOST does not discover transients. Imaging surveys do: **LSST** (the Rubin Observatory) now, and ZTF and others later. Their alert streams pass through the **Lasair** broker. Lasair runs the **TiDES selection function** (an SQL filter written by us) on every alert, and publishes the alerts that pass to a Kafka topic.

This repository is the bridge between the two:

```
 LSST/ZTF alerts ──▶ Lasair (runs the TiDES SQL filter) ──Kafka──┐
                                                                  ▼
                                    ┌──────────── tides-ingest (this repo) ─────────────┐
                                    │ 1. read the alerts                                │
                                    │ 2. merge detections of the same object (1″)       │
                                    │    into one TiDES object: TiDES26abc               │
                                    │ 3. keep state + full history in PostgreSQL        │
                                    │ 4. decide which objects are still "live"          │
                                    │ 5. keep the 4MOST queue in step (create /         │
                                    │    switch off / brightness milestones)            │
                                    │ 6. find host galaxies; queue them after the fade  │
                                    └──────────────────────────┬────────────────────────┘
                                                               ▼
                                      4MOST transients API ──▶ OSTD ──▶ OpSys ──▶ fibres
```

Why it is built the way it is:
- **One object, many surveys.** The same supernova can be alerted by LSST and ZTF under different names. TiDES gives it one name and remembers every survey that saw it.
- **4MOST is a shared, external system.** We must never send it garbage, duplicates or test data. Every change we make there is logged.
- **The selection function must be reconstructable at the end of the survey.** For every target we must be able to say which selection pipeline and version picked it, when, from which alert, what 4MOST was told, and by which version of this code. Section 10 shows how.

---

## 2. Key concepts

| Term | Meaning here |
|---|---|
| **TiDES object** | One astrophysical transient. One row in `tides_master`, with key `tides_id` and name `TiDESyy` + letters (e.g. `TiDES26abc`, assigned by a database trigger). |
| **Survey** | An imaging survey that sends us alerts. Ids live in `survey_ids`: 1 LSST, 2 ZTF, 3 LS4, 4 4MOST, 5 ATLAS, 6 GOTO, 7 HSC, 8 BlackGEM, 9 TNS. |
| **Native id** | The survey's own name for the object (LSST `diaObjectId`, ZTF `objectId`). Stored in `surveys` and in `tides_events`. |
| **Selection pipeline** | A named, versioned selection function, e.g. `TiDES-LSST-default` `v1.0`. One row in `pipelines`, maintained **by hand** (§10.2). |
| **Alert** | One Kafka message from Lasair: the latest state of one survey object that passed our filter. |
| **Consumer group / offset** | Kafka remembers, per *group id*, how far that group has read. We commit our position only after the database has safely stored the alerts (§7.3). |
| **Active** | `tides_master.active`: the object should currently be on 4MOST's queue. |
| **Outbox** | `tides_master.sync_pending = TRUE` means 4MOST still has to be told about this row. The 4MOST sync step works through the outbox (§7.10). |
| **Survey clock** | The list of **observing nights** on which a survey actually sent us alerts (`survey_nights`). An object is deactivated after more than `DEACTIVATE_AFTER_DAYS` such nights without a new detection. Maintenance and bad weather don't count (§7.9). |
| **Milestones** | 4MOST asked to be told when a target becomes brighter than 18, 17, 16 or 15 mag, because bright targets cause flux leakage on the CCD. Only brightening is reported (§8.4). |
| **4MOST target** | The object as registered in 4MOST. Our copy of its id is `tides_master.pk_4most`. |
| **Subsurvey** | 4MOST grouping inside a survey: TiDES uses `tides-sn` (transients) and `tides-host` (hosts). **Survey id 15 is shared** with another programme whose subsurvey is `source`; never touch those targets. |
| **OSTD ingestion** | When 4MOST copies a target into its operational database it gets `ostd_targ_id` and `date_ingested`. After that a target can only be switched on/off, not deleted. |

---

## 3. Architecture

### 3.1 Components

| Component | File | Runs as | Reads | Writes |
|---|---|---|---|---|
| **Ingest flow** | `tides_flows/tides_controller.py` → `run_target_workflow` | Prefect flow (deployment `opr4-deploy`) | Lasair Kafka; `tides_master` | all `tides_*` tables; 4MOST (POST/PATCH) |
| Survey plugins | `tides_flows/tides_lsst.py`, `tides_flows/tides_ztf.py` | called by the flow | Kafka | nothing (return a DataFrame) |
| Kafka helpers | `tides_flows/stream_utils.py` | library | Kafka | Kafka offsets (commit) |
| 4MOST outbox | `tides_flows/fourmost_sync.py` | called by the flow | `tides_master` | 4MOST; `tides_master`; `tides_4most_log`; `tides_events` |
| 4MOST payload rules | `tides_flows/fourmost_payload.py` | library (pure functions) | – | – |
| 4MOST API client | `tides_flows/submit_transients.py` | library + CLI | 4MOST | 4MOST |
| Provenance helpers | `tides_flows/provenance.py` | library | – | `tides_events`, `tides_4most_log` |
| **OSTD sync** | `db_4MOST_tasks/sync_ostd.py` → `sync_ostd_flow` | Prefect flow (not yet deployed) | 4MOST (GET) | `tides_master.ostd_*`, `date_ingested` |
| **Host matching** | `host_tools/tides_host_matching/tides_match_gal_local.py` → `match_galaxies_flow` (git submodule) | Prefect flow / script | `tides_master`; Lasair Sherlock API | `tides_host*`; 4MOST (currently blocked, §9) |
| SQL steps | `sql_tasks/*.sql` | executed by the flow | | |
| Schema | `database_setup/*.sql`, `database_setup/migrations/*.sql` | `setupDB.sh`, `migrate.sh` | | |
| Admin tools | `qmost_db_utils/*.py` | CLI, by hand | `tides_master`, 4MOST | 4MOST (currently blocked, §12.12) |

### 3.2 One ingest run, step by step

`run_target_workflow(connect_db=True, test_mode=False)` in `tides_flows/tides_controller.py`:

1. **Settings.** `load_dotenv()`, then `submit_transients.configure_from_env()` sets the 4MOST URL, credentials and **mode** (`off` / `dry_run` / `live`, §6.2).
2. **Run lock.** `pg_try_advisory_lock(TIDES_RUN_LOCK_KEY)`. If another ingest run or an OSTD sync holds it, this run does nothing and exits.
3. **For each enabled survey** (`SURVEYS_ENABLED`, default `lsst`), on its own:
   1. Read Kafka until idle (`stream_utils.read_messages`), keeping the latest alert per object.
   2. Convert to the plugin contract (§7.4).
   3. Validate (§7.5); bad rows are dropped and printed.
   4. In **one database transaction**:
      - stage the alerts;
      - match each to the nearest object within 1″, or create a new one;
      - update the matched objects;
      - record survey ids, first selections and events;
      - record tonight as an observing night for this survey.
   5. **Commit the Kafka offsets**, only after the database commit.
   6. If anything in this survey fails, roll back, don't commit its offsets (its alerts are re-read next run), note the failure, and **carry on** with the next survey.
4. **Deactivation** (always runs, even on an empty night): stale or faint objects become inactive and `sync_pending` (§7.9).
5. **4MOST sync** (always runs): reconcile, create, switch on/off, milestone magnitudes (§7.10).
6. **Report.** A Markdown artifact `tides-ingest-report` in Prefect.
7. If any step failed, the flow **raises at the end**, so Prefect marks the run *Failed*, after everything else has been done.

### 3.3 Who owns which state

| State | Lives in | Notes |
|---|---|---|
| What has been read from Kafka | Kafka, per consumer group | committed by us after the DB commit |
| Identity, positions, magnitudes, active flag | `tides_master` | the source of truth |
| "4MOST must be told" | `tides_master.sync_pending`, `sync_state` | the outbox |
| What 4MOST has | 4MOST; our copies are `pk_4most`, `mag_4most` | |
| Every request to 4MOST | `tides_4most_log` | append-only |
| Every thing that happened to an object | `tides_events` | append-only |
| When each survey observed | `survey_nights` | the survey clock |
| Selection function definitions | `pipelines` (by hand) + Lasair | §10.2 |

### 3.4 What happens when something fails

| Failure | Effect | Recovery |
|---|---|---|
| Kafka unreachable / no partition assignment within 60 s | that survey fails; others continue; flow marked Failed | automatic next run |
| A malformed Kafka message | skipped and printed (never blocks the stream) | none needed |
| An alert fails validation (NaN, JD instead of MJD, bad position, future date) | dropped and printed; **its offset is still committed** | it is lost; see §15 |
| Database error during a survey | that survey's transaction rolls back; offsets not committed | automatic: re-read next run |
| 4MOST down | rows stay pending; after 3 consecutive failures we stop calling 4MOST for this run | automatic next run. **The run is still green** (§15) |
| 4MOST rejects one item | that item → `sync_state='failed'`; the rest of its batch is retried one by one | a human fixes the row (§12.3) |
| 4MOST outcome unknown (timeout, 5xx, HTML page) | row → `sync_state='ambiguous'`; next run looks it up by name before any re-POST | automatic |
| Two runs at once | the second exits immediately (advisory lock + Prefect concurrency limit) | none needed |

---

## 4. Repository tour

| Path | What it is | When you touch it |
|---|---|---|
| `tides_flows/tides_controller.py` | the ingest flow; settings at the top (`MATCH_RADIUS_DEG`, `DEACTIVATE_AFTER_DAYS`, `FAINT_LIMIT_MAG`, `KAFKA_MAX_MESSAGES`, `TIDES_RUN_LOCK_KEY`); `validate_targets`; `SURVEY_IDS` and `fetch_survey` | adding a survey; changing run behaviour |
| `tides_flows/tides_lsst.py` | LSST plugin: Kafka → contract DataFrame | the Lasair LSST query changes |
| `tides_flows/tides_ztf.py` | ZTF plugin (**not live yet**) | before enabling ZTF (§15) |
| `tides_flows/stream_utils.py` | Kafka consumer, group ids, read loop, commit, `CONTRACT_COLUMNS` | rarely |
| `tides_flows/fourmost_sync.py` | 4MOST outbox (steps 1–5, §7.10) | changing how we talk to 4MOST |
| `tides_flows/fourmost_payload.py` | what we send: field values, spec checks, band → `MAG_TYPE`, milestones, MJD→JD | new band/survey; 4MOST changes a rule |
| `tides_flows/submit_transients.py` | 4MOST's own client (from Jake / 4MOST) with a **TiDES safety block** added (`WRITES_ALLOWED`, `configure_from_env`, 2xx success) | when 4MOST ships a new version: re-apply the TiDES block (§12.13) |
| `tides_flows/provenance.py` | writes `tides_events` / `tides_4most_log`; `git_sha()`, `flow_run_id()` | rarely |
| `tides_flows/tidesCom.py`, `tides_flows/ingest_report.py` | **legacy, unused** (tidesCom doesn't even import) | delete one day (§15) |
| `sql_tasks/*.sql` | one SQL step per file (§7) | changing matching/merging/deactivation |
| `database_setup/*.sql`, `setupDB.sh` | base schema for a new database | new database |
| `database_setup/migrations/NNN_*.sql`, `migrate.sh` | schema changes after the base, applied once each | any schema change: add a new numbered file |
| `database_setup/databaseHardReset.*`, `DELETE-deleteAllTables.sql` | **destructive** reset tools | only on a test database you own (§14) |
| `db_4MOST_tasks/sync_ostd.py` | OSTD id sync flow | |
| `host_tools/tides_host_matching/` | host matching (**separate git repo**, submodule) | §9 |
| `qmost_db_utils/` | admin CLIs to deactivate/delete 4MOST targets | rarely; currently blocked (§12.12) |
| `selection_functions/Lasair-LSST-query-dev.sql` | **copy** of the Lasair LSST filter (the live one is on Lasair) | keep in step with Lasair (§10.2) |
| `testing/` | mock streams, in-memory fake 4MOST, integration tests | §13 |
| `prefect.yaml` | Prefect deployment (build, pull, deployment `opr4-deploy`) | deploying, scheduling |
| `requirements.txt` | pinned dependencies | upgrades (§12.14) |
| `.env.example` | template for `.env` (never commit `.env`) | new setting |
| `implementation_plans/` | personal notes (git-ignored), incl. the 2026-10 review | |

---

## 5. Quick start

These steps set up a **development** machine. Nothing here can write to 4MOST: `TIDES_ENV=development` makes the client refuse the production URL.

**1. Clone, including the host-matching submodule.**
```bash
git clone --recurse-submodules https://github.com/TiDES-4MOST/tides-ingest.git
cd tides-ingest
# already cloned without it?   git submodule update --init
```

**2. Python 3.12 environment.** The production image and the original `opr4` conda env use 3.12.
```bash
conda create -n tides python=3.12 -y && conda activate tides
pip install -r requirements.txt
```

**3. PostgreSQL with the q3c extension.** Install PostgreSQL (this code was tested on 17) and [q3c](https://github.com/segasai/q3c). On macOS, Postgres.app includes q3c.

**4. `.env`.** Copy the template and fill in the values (§6.1):
```bash
cp .env.example .env
```
For development, keep `TIDES_ENV=development` and `FOURMOST_MODE=off` (or `dry_run`, to see the payloads).

**5. Create a development database and a test database.**
```bash
createdb tides_dev
createdb tides_dev_test
for DB in tides_dev tides_dev_test; do psql -d $DB -c "CREATE EXTENSION IF NOT EXISTS q3c;"; done
```
Then load the schema and the migrations into each. `setupDB.sh` and `migrate.sh` read `../.env` (relative to the current directory), so **run them from inside `database_setup/`**. They take the database name from `TIDES_DB_DATABASE` and ask for confirmation.
```bash
cd database_setup
./setupDB.sh          # base tables, trigger, seed rows (survey_ids, pipelines)
./migrate.sh          # migrations 001, 002, 003 … each applied exactly once
cd ..
```
To load the test database, temporarily point `TIDES_DB_DATABASE` in `.env` at `tides_dev_test` and run both again. The downtime test (step 8) can also create the schema itself on an empty test database.

**6. Read the stream without writing anything (dev mode).**
```bash
python -c "import sys; sys.path.insert(0,'tides_flows'); import tides_controller as t; t.run_target_workflow(connect_db=False)"
```
- Reads the Lasair LSST topic and prints how many targets it would ingest.
- Uses `LASAIR_LSST_GROUP_ID` if `.env` sets it, otherwise a random throwaway group.
- Writes nothing to the database and **never commits Kafka offsets**, so it is safe even with a real group id.
- Outside the observing season the topic may hold only a handful of example alerts.

**7. Run the mock pipeline against the test database.** `test_mode` refuses any database whose name doesn't contain `test`, `scratch` or `mock`, and never writes to 4MOST.
```bash
TIDES_DB_DATABASE=tides_dev_test python -c "import sys; sys.path.insert(0,'tides_flows'); import tides_controller as t; t.run_target_workflow(test_mode=True)"
psql -d tides_dev_test -c "SELECT tides_id, name, active, sync_pending, latest_mags FROM tides_master ORDER BY tides_id;"
```
Expect 3 objects: an LSST object and a ZTF object 0.5″ away merged into one, plus one LSST-only and one ZTF-only object. All three stay pending, because test mode never sends to 4MOST.

**8. Run the integration test.** It truncates the test database and uses an in-memory fake 4MOST:
```bash
# .env:  TIDES_TEST_DB_DATABASE=tides_dev_test
python testing/test_4most_downtime.py
# → INTEGRATION TEST PASSED: downtime, lost responses and recovery work, with no duplicates.
```

**9. Run the real flow against your dev database** (live Kafka, nothing sent to 4MOST with `FOURMOST_MODE=off`).

> ⚠️ This **commits Kafka offsets** to `LASAIR_LSST_GROUP_ID` if it is set. **Never use the production group id on a development machine.** Production would then skip every alert your dev run consumed. Leave it unset in development (a random group is used), or use a clearly separate name such as `tides-lsst-dev-<yourname>`.

```bash
python tides_flows/tides_controller.py
```

---

## 6. Configuration reference

### 6.1 Environment variables

The flows call `load_dotenv()`, which reads `.env` from the repository root. Variables already set in the environment win over `.env`. **Never commit `.env`.** It is git-ignored and docker-ignored. In production the worker mounts it into the container read-only (§11.2).

| Variable | Read by | Default | Development | Production | Meaning |
|---|---|---|---|---|---|
| `TIDES_ENV` | `submit_transients.check_writes_allowed`, `stream_utils.group_id_from_env`, tests | `development` | `development` | **`production`** | Writes to the production 4MOST host are refused unless this is `production`. In production a fixed Kafka group id is also required. |
| `TIDES_DB_HOST`, `TIDES_DB_PORT`, `TIDES_DB_USER`, `TIDES_DB_PASS`, `TIDES_DB_DATABASE` | all flows | – | your dev DB | production DB | PostgreSQL connection. |
| `TIDES_TEST_DB_DATABASE` | `testing/*.py` | – | e.g. `tides_dev_test` | not set | Database the tests may **truncate**. Must be local, and its name must contain `test` or `mock`. |
| `FOURMOST_MODE` | `submit_transients.configure_from_env` | `off` | `off` or `dry_run` | **`live`** | `off`: no 4MOST calls. `dry_run`: build payloads and log them to `tides_4most_log`, send nothing. `live`: send. |
| `FOURMOST_SCHEMA` | `configure_from_env`, `qmost_db_utils` | production URL | production URL is fine (writes are refused) | `https://4most.mpe.mpg.de/QFSwi/targetCat/transients/` | 4MOST transients API URL. |
| `FOURMOST_USERNAME`, `FOURMOST_PASSWORD`, `FOURMOST_ACCESS_TOKEN` | `configure_from_env`, host flow, utils | – | | | 4MOST credentials (basic auth and/or token). |
| `FOURMOST_SURVEY_ID` | `configure_from_env` | `15` | | `15` | `uploadedfor_survey_id` (TiDES = 15, **shared** with subsurvey `source`). |
| `SURVEYS_ENABLED` | controller | `lsst` | `lsst` | `lsst` (add `ztf` only after §15 item Z) | Comma list of surveys to ingest. |
| `LASAIR_LSST_TOPIC` | controller | `lasair_16TiDES_Frohmaier_et_al_2025` | | the TiDES LSST topic | Kafka topic published by our Lasair filter. |
| `LASAIR_LSST_GROUP_ID` | controller via `stream_utils` | random `tides-lsst-dev-xxxxxxxx` | **unset** (random, see §7.3), or a dev-only name | **fixed**, e.g. `tides-lsst-prod-1` | Kafka consumer group. Offsets are committed to it after each successful run. **The production value must never appear in a development `.env`.** |
| `LASAIR_ZTF_TOPIC`, `LASAIR_ZTF_GROUP_ID` | controller | – / random | | when ZTF goes live | As above, for ZTF. |
| `LSST_PIPELINE_NAME`, `LSST_PIPELINE_VERSION` | controller | `TiDES-LSST-default`, `v1.0` | | the current selection version | Which `pipelines` row the LSST alerts are recorded under (§10.2). The run fails if the row doesn't exist. |
| `ZTF_PIPELINE_NAME`, `ZTF_PIPELINE_VERSION` | controller | `TiDES-ZTF-default`, `v1.0` | | | Same, for ZTF. |
| `DEACTIVATE_AFTER_DAYS` | controller | `10` | | 7–10 (TiDES decision) | **Observing nights** (not calendar days) without a new detection before an object is deactivated (§7.9). |
| `KAFKA_MAX_MESSAGES` | controller | `20000` | | | Most alerts read per survey per run. The rest are read next run. |
| `GIT_SHA` | `provenance.git_sha` | `git rev-parse HEAD` if available, else `unknown` | | set at deploy (§11.2) | Code version recorded on every event and 4MOST request. The image has no `.git`. |
| `LASAIR_LSST_TOKEN`, `LASAIR_ZTF_TOKEN` | host flow only | – | | | Lasair **API** tokens for Sherlock. The Kafka stream needs no token. See §9 for the endpoint pairing problem. |
| `TIDES_ENV_FILE`, `PREFECT_SECRET_API` | `prefect.yaml` (at deploy time) | – | | absolute path of the worker's `.env`; Prefect API URL | Used only by `prefect deploy`. |

Constants that are deliberately **not** environment variables (change them in code, then record the change, §10.2):
- `MATCH_RADIUS_DEG = 0.000277778` (1″) and `FAINT_LIMIT_MAG = 22.5`, in `tides_controller.py`;
- `MATCH_RADIUS_ARCSEC = 1.0`, `BATCH_SIZE = 50` and `TIMEOUT_SECONDS = 60`, in `fourmost_sync.py`;
- everything in `fourmost_payload.py` (template, ruleset, exposure times, milestones, band → MAG_TYPE).

### 6.2 What can talk to 4MOST: the mode matrix

All writes go through `submit_transients.create/update/delete_transient`. These call `check_writes_allowed(url)` first.

| `FOURMOST_MODE` | `TIDES_ENV` | URL | Result |
|---|---|---|---|
| `off` | any | any | No 4MOST calls at all. The outbox just accumulates. |
| `dry_run` | any | any | Payloads built and logged to `tides_4most_log` with `outcome='dry_run'`. Nothing sent. |
| `live` | not `production` | production host (`4most.mpe.mpg.de`) | **Refused at the start of the run** (`RuntimeError`). |
| `live` | `production` | production host | Sends. |
| `live` | any | another host (a test server or a fake) | Sends. |
| (any) with `test_mode=True` | | | `live` is downgraded to `dry_run`. Test mode never writes. |

Code that never calls `configure_from_env()` has writes switched **off** (`WRITES_ALLOWED = False`). Today that means the host flow and `qmost_db_utils` (§9, §12.12). The `submit_transients.py` CLI switches writes on when a human types `--create/--update/--delete`. Even then, the production host still needs `TIDES_ENV=production`.

---

## 7. The ingest flow in detail

### 7.1 Entry point

`tides_controller.run_target_workflow(connect_db=True, test_mode=False)` is a Prefect flow named **"delta OPR4 Workflow"**, with `log_prints=True` and `timeout_seconds=3600`.

| Call | Effect |
|---|---|
| `connect_db=True, test_mode=False` | Normal run (what the deployment does). |
| `connect_db=False` | Dev mode: read Kafka and print the counts. No DB writes, no offset commits, no 4MOST. |
| `test_mode=True` | Use `testing/mock_streams.py` instead of Kafka, for both LSST and ZTF. Requires a DB whose name contains `test`/`scratch`/`mock`. Never writes to 4MOST. |

### 7.2 Run lock

The flow holds a **session-level PostgreSQL advisory lock** on key `TIDES_RUN_LOCK_KEY = 0x7469444553` for its whole duration. `sync_ostd_flow` uses the same key. A run that can't get the lock prints a message and returns quietly. The Prefect deployment also has `concurrency_limit: 1` with `CANCEL_NEW`. Together these make overlapping runs impossible. Overlapping runs used to create duplicate objects and duplicate 4MOST targets.

### 7.3 Reading Kafka (`stream_utils.py`)

- **Consumer:** `confluent_kafka.Consumer` with `auto.offset.reset=earliest` and **`enable.auto.commit=False`**. It is built directly, not through `lasair.lasair_consumer`, because the Lasair wrapper can't switch auto-commit off. Brokers:
  - LSST: `lasair-lsst-kafka.lsst.ac.uk:9092` (`tides_lsst.LSST_KAFKA`);
  - ZTF: `kafka.lsst.ac.uk:9092` (`tides_ztf.ZTF_KAFKA`).
- **Group id** (`group_id_from_env`):
  - **Production:** set `LASAIR_LSST_GROUP_ID` to a fixed value. Each run continues where the last one committed. If `TIDES_ENV=production` and the variable is unset, the run fails rather than guessing.
  - **Development:** leave it unset. Each run gets a new random group, starts from the oldest retained message, and re-reads the whole topic. That's deliberate: you can debug repeatedly without waiting for new alerts. It's harmless because re-reading changes nothing (§7.7).
  - **Changing** the fixed group id makes the next run re-read everything Lasair still retains. That's safe for the same reason.
- **Read loop** (`read_messages`):
  1. Waits up to 60 s for Kafka to assign partitions. Joining a group takes about 3 s; before assignment an empty poll means nothing.
  2. Then reads until the topic has been idle for 10 s, or `KAFKA_MAX_MESSAGES` messages have been read.
  - A message that isn't valid JSON, or lacks the object id field, is printed and skipped.
  - A Kafka error raises, and the survey fails.
- **Per object**, only the most recent alert in the batch is kept: by `lastDiaSourceMjdTai` for LSST, `latest_jd` for ZTF.
- **Commit** (`commit_offsets`) happens **only after** that survey's database transaction has committed. If the run dies in between, the next run re-reads those alerts. The consumer is always closed.

### 7.4 Survey plugins and the contract

A plugin's `get_targets(pipeline_id, topic, group_id, max_messages)` returns `(DataFrame, consumer)`. It must **never** return `None`. An empty batch is an empty DataFrame with the contract columns (`stream_utils.CONTRACT_COLUMNS`):

| Column | Type | Meaning / rules |
|---|---|---|
| `object_id` | str | Native survey id. LSST `diaObjectId`s exceed 2⁵³, so convert them with `astype('int64').astype(str)` and **never** via float. |
| `survey_id` | int | `survey_ids.survey_id` (LSST 1, ZTF 2, …). |
| `pipeline_id` | int | `pipelines.pipeline_id` of the selection pipeline/version. |
| `ra`, `dec` | float | Degrees. |
| `jdmin`, `jdmax` | float | **MJD** of the first and latest detection. (The names are historical: they hold MJD, not JD.) |
| `latest_filter` | JSON str | `{band_key: MJD of that band's latest detection}`. |
| `latest_mag` | JSON str | `{band_key: AB mag}`. Only real detections: **no placeholders, no NaN** (`json.dumps(..., allow_nan=False)`). |
| `n_sources` | JSON str | `{band_key: number of detections}`. |

**Band keys** are `<band>_<survey>`: `g_lsst`, `r_lsst`, `i_lsst`, `z_lsst`, `g_ztf`, `r_ztf`, … The database keeps the true photometric system. The 4MOST label is chosen only when the payload is built (§8.3).

**LSST** (`tides_lsst.process_transients`). Input columns are those selected by `selection_functions/Lasair-LSST-query-dev.sql`.
- `ra`, `decl` → `ra`, `dec`.
- `firstDiaSourceMjdTai` / `lastDiaSourceMjdTai` → `jdmin` / `jdmax` (MJD, TAI).
- Bands **g, r, i, z** only (`LSST_BANDS`; u and y are deliberately ignored). A band gets a magnitude only if `<b>_psfFlux > 0` and `<b>_latestMJD` is set. Then `mag = −2.5·log10(flux_nJy) + 31.4`.
- `n<b>Sources` → `n_sources`.

**ZTF** (`tides_ztf.process_transients`, not live). Expects Lasair columns `objectId, ra, decl, first_jd, latest_jd`, plus either `latestFilter`/`latestMag` or `gmag`/`rmag`.
- JD → MJD via `− 2400000.5`.
- One band per alert.
- `n_sources` is a placeholder `1` (§15).

### 7.5 Validation (`validate_targets`)

Before staging, each row is checked. Rows that fail are **dropped and printed** (`Dropping alert … : reason`):
- `object_id` missing;
- `ra`, `dec`, `jdmax`, `survey_id` or `pipeline_id` missing or not finite;
- `ra` outside [0, 360), or `dec` outside [−90, 90];
- `jdmax` not in (40000, now + 1 day]. This catches JD-instead-of-MJD and future dates;
- one of the three JSON columns not valid JSON, not a dict, or containing a non-finite value.

The batch's Kafka offsets are still committed, so a dropped alert is gone (only the printed log line remains). See §15.

### 7.6 Staging and matching

All inside the survey's transaction (`ingest_survey_targets` → `upsertToMaster`):

1. **`createStage.sql`** creates the TEMP table `tides_stage` with explicit column types. It has `ON COMMIT DROP`, so it disappears with the transaction. **`indexStage.sql`** adds a q3c index and runs `ANALYZE`.
2. **`matchStageToMaster.sql`** attaches each staged alert to the **nearest** `tides_master` row within **1″**.
   - It uses `q3c_join(stage, master, r)`, an argument order that uses the `tides_master` q3c index: 5.7 ms vs 22.7 s at 200k objects.
   - Ties go to the lower `tides_id`, so the result is deterministic.
3. **`insertNewMasters.sql`** handles alerts that matched nothing.
   - It inserts only the "leaders": unmatched rows with no earlier unmatched row within 1″.
   - The controller then re-runs the match. It loops until every alert is attached, which normally takes one pass.
   - This is what stops two new alerts 0.5″ apart from becoming two TiDES objects.
   - New rows get `active = TRUE`, `sync_pending = TRUE` and `last_survey_id`, and the trigger names them.

### 7.7 Updating matched objects (`updateMatchedMasters.sql`)

There is **one UPDATE per object**, even if several alerts matched it:
- `jdmin = LEAST(...)`, `jdmax = GREATEST(...)`: the time span only widens.
- `latest_mags`, `latest_mjd` and `n_sources` are merged **per band**, by the SQL function `tides_merge_bands`. A band's value is replaced only if the incoming MJD for that band is **newer**.
- Only a **genuinely new detection** (incoming `jdmax` > stored `jdmax`) may:
  - set `active = TRUE` (and `sync_pending = TRUE` if it was inactive): the object is reactivated;
  - move `updated`;
  - change `last_survey_id`.
- So **replaying old alerts changes nothing**. That is why re-reading a topic (dev groups, offset resets, retries after a crash) is safe.

### 7.8 Survey ids, selections and events

- **`insertSurveys.sql`** keeps one native id per (object, survey), the first one seen (`ON CONFLICT DO NOTHING`). Every native id ever seen is also in `tides_events` (`alert` rows, `survey_object_id`).
- **`insertPipelineSelections.sql`** records the first time each (object, survey, pipeline) selected the object. Newly inserted rows also produce `selected` events.
- **`logStageAlerts.sql`** writes one `alert` event per staged alert, with the native id, pipeline and magnitudes. It writes `manual_submission` instead when `submitted_by` is given (§10.3).
- The Python side logs `created` for new objects and `activated` (reason `redetected`) for revived ones.

### 7.9 Survey clock and deactivation

- **`recordHeartbeat.sql`** inserts `(survey_id, floor(jdmax − 0.5))` for every staged alert into `survey_nights`. That's the observing night, in Rubin's `dayObs` convention. A night exists in the table only if the survey sent us at least one TiDES alert that night.
- **`deactivateUnobserved.sql`** (one statement, run after all surveys) deactivates an active object when **either**:
  1. **Stale:** the survey that made its latest detection (`last_survey_id`) has observed on more than `DEACTIVATE_AFTER_DAYS` nights after the object's last detection night. Reason: `stale_<N>d`. The `d` is historical; it means nights.
     - Rows created before the migration have no `last_survey_id`. For those, wall-clock days since `jdmax` are used instead.
  2. **Faint:** every recorded band magnitude is fainter than 22.5. Reason: `faint_alert`.
- Deactivated rows get `sync_pending = TRUE`, so 4MOST is told, and a `deactivated` event with the reason.

Why it is done this way:
- Maintenance, bad weather and our own outages add no nights, so they never age an object. A 12-day shutdown causes zero flips.
- A transient that is genuinely not detected while the survey keeps observing is switched off after N nights.
- **No model of how transients fade is used**, and there is **no maximum lifetime**. Both were deliberate TiDES decisions to keep the selection function simple and unbiased.
- **Fade → rebrighten → fade works.** A new detection reactivates the object (4MOST is told `is_active=true`), and N quiet observing nights deactivate it again.
- Under the current Lasair LSST filter every alert has at least one band ≤ 22.5, so the faint rule effectively never fires for LSST-only objects. Fading is detected by absence of alerts.

### 7.10 4MOST sync (`fourmost_sync.sync_pending_to_4most`)

After deactivation, every run works through the outbox in five steps. All of them use `FourmostClient`, which logs every request to `tides_4most_log`.

1. **Reconcile `ambiguous` rows** (live mode only). For each, `GET ?name=<name>&subsurvey=tides-sn`:
   - not found → clear the state, so step 3 POSTs again;
   - exactly one found, within 1″ → **adopt** its id;
   - several found, or one at the wrong position → `conflict`, for a human.
2. **Clear inactive, never-registered rows.** Nothing to tell 4MOST, so `sync_pending = FALSE`.
3. **POST new active targets**, in batches of 50.
   - Each payload is built by `fourmost_payload.build_transient_payload`. Rows that can't be submitted (dec > +40, no valid magnitude) become `not_submittable`, and are re-checked every run.
   - **Write-ahead:** rows are marked `ambiguous` and committed *before* the POST. A crash or timeout therefore can never lead to a blind re-POST.
   - If 4MOST rejects a batch (4xx), its items are retried **one by one**, so one bad row can't block 49 good ones.
4. **PATCH `is_active`** for rows whose active flag changed. Success requires HTTP 2xx, a JSON body, and the body **echoing** the requested value.
5. **Milestone magnitudes:** PATCH `mag` + `mag_type` when an active target has brightened past 18/17/16/15 since what 4MOST has (`mag_4most`) (§8.4).

After 3 consecutive network or server failures the client stops for this run (circuit breaker), and everything stays pending.

**Outcome of each request:**

| Outcome | When | Row becomes |
|---|---|---|
| `ok` | 2xx with a JSON body | synced (`sync_pending=FALSE`, `sync_state=NULL`) |
| `rejected` | 400/404/409/422, or a payload that can't be encoded | `failed` (needs a human, §12.3) |
| `ambiguous` | POST with timeout, connection error, 5xx, or 2xx with a non-JSON body (4MOST has returned HTML error pages with 200) | `ambiguous` (automatic lookup next run) |
| `retry` | PATCH/GET failures, 401/403/429 | unchanged; tried next run |
| `dry_run` | `FOURMOST_MODE=dry_run` | unchanged (payload logged) |

**`sync_state` values:**

| Value | Meaning | Who acts |
|---|---|---|
| `NULL` | normal | – |
| `ambiguous` | a POST's outcome is unknown | automatic (step 1) |
| `failed` | 4MOST rejected the row, or answered something unexpected | human: read `sync_last_error`, fix, set `sync_state = NULL` |
| `conflict` | name lookup found several targets, or one at another position | human: look in 4MOST |
| `not_submittable` | our payload builder refused it (reason in `sync_last_error`) | automatic re-check every run; usually permanent (e.g. dec > +40) |

`sync_attempts` counts problems since the last success. `sync_last_error` holds the latest message.

### 7.11 Report and failure signalling

- `ingest_report` publishes the Prefect Markdown artifact `tides-ingest-report`: new, updated and deactivated targets. The plugins also publish `tides-lsst-stream-report` / `tides-ztf-stream-report` with the raw batch.
- Any failed survey, failed deactivation or exception in the 4MOST step is collected. The flow **raises at the end**, after doing everything it could, so Prefect shows **Failed**.
- **4MOST being down is not (yet) a failure:** the breaker stops quietly and the run is green (§15).

### 7.12 Naming

The trigger `tides_name_auto_generator` (`database_setup/createMasterTable.sql`) names every new row `TiDES` + two-digit year + letters:
- `aaa…zzz`, then `aaaa…zzzz`;
- that's 474,552 names per year, from the sequence `tides_seq`.

When the year of the last-inserted row differs from the current year, the sequence restarts at `aaa`. The year comes from the database server's `current_date`.

The sequence is `NO CYCLE` (migration 003): if the cap is ever reached, inserts fail with a clear error instead of reusing names. Sequence values used by a rolled-back run are skipped, so names can have gaps. That is harmless.

---

## 8. 4MOST integration

### 8.1 The API and the client

- **Endpoint:** `https://4most.mpe.mpg.de/QFSwi/targetCat/transients/`, a Django REST API.
- **Auth:** HTTP basic (`FOURMOST_USERNAME`/`FOURMOST_PASSWORD`) and/or `Authorization: Token …` (`FOURMOST_ACCESS_TOKEN`).
- **Client:** `tides_flows/submit_transients.py`, written by 4MOST (Jake).
  - Functions: `get_list`, `get_options`, `create_transient` (POST; dict or list), `update_transient` (PATCH), `delete_transient` (DELETE; only before OSTD ingestion).
  - Pass `return_mode="response"` to get the raw `requests.Response`. The TiDES code always does, and judges success by status code and body.
- **The TiDES safety block** (marked `### TiDES safety settings`, near the top):
  - `WRITES_ALLOWED` (default `False`);
  - `PRODUCTION_HOST`;
  - `check_writes_allowed()`, called inside create/update/delete;
  - `configure_from_env()`;
  - plus the change of the success test from `(200, 204)` to any 2xx.
  - **When 4MOST sends a new version of this file, re-apply this block** (§12.13).
- **CLI** (run from the repo root), mainly for inspection:
  ```bash
  python tides_flows/submit_transients.py --username U --password P --options        # field definitions
  python tides_flows/submit_transients.py --username U --password P --list --limit 5
  python tides_flows/submit_transients.py --username U --password P --list --filter "name=TiDES26abc&subsurvey=tides-sn"
  ```
  > ⚠️ `--update <id>` **without `--data` overwrites the target with a dummy payload**, and `--runtest` creates two dummy targets. Write actions on the production host also require `TIDES_ENV=production`.

### 8.2 What we send for a transient (`fourmost_payload.build_transient_payload`)

| Field | Value | Notes |
|---|---|---|
| `uploadedfor_survey_id` | `15` (`FOURMOST_SURVEY_ID`) | shared with subsurvey `source` |
| `name` | `TiDES26abc` | not required to be unique by 4MOST for transients, which is why the reconciliation is ours |
| `ra`, `dec` | degrees | dec ≤ +40 or the row is `not_submittable` |
| `pmra`, `pmdec`, `epoch` | 0, 0, 2000 | |
| `resolution` | 1 | low resolution |
| `subsurvey` | `tides-sn` | |
| `cadence` | 1048576 (= 2²⁰, SINGLE_VISIT) | transients must get all their signal in one visit |
| `template` | `SN_spec_specid56_snt1_phase5_redshift0.169.fits` | |
| `ruleset` | `tides_snMay2024` | |
| `extent_flag`, `extent_parameter`, `extent_index` | 0, 0, 0 | point source |
| `mag`, `mag_type` | latest real detection and its band (§8.4) | |
| `t_exp_d`, `t_exp_g`, `t_exp_b`, `t_exp_s` | 38, 38, 4300, 4300 | carried over from the original controller; meaning to be confirmed with 4MOST (§15) |
| `classification` | `TRA` | |
| `is_active` | `true` | |

`date_earliest`/`date_latest` are not sent (4MOST stores 0). If you ever send them, they are **JD**: convert with `fourmost_payload.mjd_to_jd()`, which also checks 4MOST's allowed range (2459945.5–2462501.5).

### 8.3 Rules from the 4MOST target catalogue specification

Source: `https://4most.mpe.mpg.de/static/manual/Target_catalog_format_specification.htm`.

- `DEC` ∈ [−89.99999999, **+40**], `RA` ∈ [0, 359.99999999].
- `MAG` ∈ [3, 50]. The API itself accepts 0–50. The magnitude must be in the band named by `MAG_TYPE`.
- `MAG_TYPE` comes from a **case-sensitive whitelist** (Gaia, LSST, DECam, VISTA, SDSS, Bessel, Johnson, Cousins). It has **no ZTF**.
  - Our mapping is `fourmost_payload.MAG_TYPE_FOR_BAND`: `g/r/i/z_lsst` → `LSST_g/r/i/z_AB`, and `g/r_ztf` → `SDSS_g/r_AB` (a TiDES decision).
  - Bands not in the map are never sent.
  - The API does not check this list, so a wrong label would be accepted silently. That's why it's enforced on our side.
- `CADENCE`: 2²⁰ = SINGLE_VISIT (transients). Hosts must use **0**, because galaxy spectra can be stacked across nights.
- `EXTENT_FLAG`: 0 = point source (MAG is the total magnitude); 1 = flat (MAG is a **surface brightness**, mag/arcsec²); 2 = Sérsic (with radius and index).
- `DATE_EARLIEST`/`DATE_LATEST`: JD, or 0.

### 8.4 Magnitude policy and milestones

- **First POST:** the magnitude of the **most recent real detection**, in a band 4MOST understands, labelled with that band (`latest_detection`). Ties on date go to the brighter band.
- **Afterwards:** a PATCH of `mag` + `mag_type` **only when** the latest detection is at or brighter than a milestone (18, 17, 16, 15) that is brighter than what 4MOST already has (`mag_4most`). Fading is never reported, and nothing is sent below 15.
  - Reason (4MOST's request): very bright targets cause flux leakage and crosstalk on the CCD and can contaminate other spectra.
  - Consequence: a target that peaked at 16 and faded keeps `mag=16` in 4MOST (open question for 4MOST, §15).
- Whether a milestone is due is recomputed every run from the data. Nothing needs to be flagged. A rejected milestone PATCH is retried next run and never blocks `is_active` updates.

### 8.5 OSTD ingestion and `sync_ostd`

4MOST periodically ingests targets into the OSTD. `db_4MOST_tasks/sync_ostd.py` (`sync_ostd_flow(force_full_sync=False)`):
1. Reads `MAX(date_ingested)` from `tides_master`. On the first run it starts from `2026-08-01`.
2. Fetches `?date_ingested__gt=…&subsurvey=tides-sn`, following pagination.
3. Writes `ostd_targ_id`, `ostd_u_obj_id` and `date_ingested` by `pk_4most`.

It only **reads** from 4MOST. It takes the same run lock as the ingest flow, and deliberately does **not** touch `updated`. It is not yet in `prefect.yaml` (§11.2).

### 8.6 Rules of the road with 4MOST

- **Never** POST/PATCH/DELETE to production from a laptop, a test, or a notebook. Use `FOURMOST_MODE=off/dry_run` or the in-memory fake (`testing/fake_4most.py`).
- **Never** act on targets whose subsurvey isn't `tides-sn` or `tides-host`. Survey 15 is shared.
- Targets already ingested into the OSTD can't be deleted, only switched off (`is_active=false`).
- Survey id 5 (`S_test`) exists on 4MOST for testing. Ask 4MOST before using it.

---

## 9. Host-galaxy flow

> **Status (2026-10):** the host flow was not changed by the October 2026 patch, and has known problems (below). Its 4MOST submission is **currently blocked** by the write guard: the flow never calls `configure_from_env()`, so `WRITES_ALLOWED` stays `False`. Hosts are matched and queued in the database, but nothing is sent to 4MOST. Fix the items in §15 (H1–H8) before switching submission on.

**Where:** `host_tools/tides_host_matching/tides_match_gal_local.py`. This is a **git submodule**, i.e. a separate repository (`TiDES-4MOST/tides_host_matching`): commit changes there, then update the submodule pointer here.

**What it does** (`match_galaxies_flow(pipeline_name="sherlock", pipeline_version="v1.0")`, Prefect flow `tides-galaxy-matching`):

1. Finds or registers the `pipelines` row (`sherlock`, `v1.0`).
2. Loads **active** transients with no `tides_host` row for that pipeline.
3. For each, calls Lasair **Sherlock** (`sherlock_position`) and keeps up to 3 `galaxy` crossmatches. With the default `lite=True`, Sherlock returns only its best match, so in practice it is 1.
4. Stores each galaxy once in `tides_host_catalog` (deduplicated by catalogue **name**), and each association in `tides_host` (rank, metadata such as separation, `sm_axis_arcsec`, `classificationReliability`, photo-z).
5. Queue rule (`update_host_queue_status`): an association becomes active when its transient is **inactive** and either:
   - more than 50 days have passed since `jdmax` (MJD), or
   - all of its LSST magnitudes are fainter than 24.
6. Syncs to 4MOST: one target **per association** (subsurvey `tides-host`, ruleset `tides_hostsMay2024`, classification `GAL`). *Currently blocked.*
7. Records start/end times in `run_state.py` (`tides_run_state.json` inside the submodule). Nothing reads this back.

**Run by hand:**
```bash
python host_tools/tides_host_matching/tides_match_gal_local.py --pipeline-name sherlock --pipeline-version v1.0
```

**Lasair endpoint (important).**
- The flow calls `lasair.lasair_client(token)` with no endpoint, and the library default is the **ZTF** API. It prefers `LASAIR_LSST_TOKEN`. That pairing returns `Unauthorized` for every query, and the error is swallowed: no hosts, and a green run.
- The LSST token works with `endpoint="https://lasair.lsst.ac.uk/api"`; the ZTF token works with the default.
- One-line fix: in `get_lasair_client()`, pass the endpoint that matches the token (§15 H1).

---

## 10. Provenance and the selection function

The point of all the record-keeping is that at the end of the survey we can reconstruct the TiDES selection function, from the database alone:
- which objects were selected;
- by which pipeline and version, from which survey alert, and when;
- what happened to them (activated, deactivated and why);
- what 4MOST was told, and by which version of this code.

### 10.1 What is recorded where

| Question | Table | Written by |
|---|---|---|
| Which survey ids does object X have? | `surveys` (first id per survey); `tides_events` (`alert` rows, `survey_object_id`, all ids) | ingest |
| When did pipeline P/version V first select X, for which survey? | `pipeline_selections` (+ `selected` events) | ingest |
| Every alert that touched X | `tides_events` (`alert`, with `details` = jdmin/jdmax/mags) | ingest |
| When and why X was created / activated / deactivated | `tides_events` (`created`, `activated`/`redetected`, `deactivated`/`stale_Nd` or `faint_alert`) | ingest |
| What we sent 4MOST about X, and what it answered | `tides_4most_log` (`tides_ids`, method, payload, status, outcome, response) | 4MOST sync |
| 4MOST registration and magnitude updates | `tides_events` (`4most_created`, `4most_mag_update`) | 4MOST sync |
| Which code did it | `git_sha` on every `tides_events` / `tides_4most_log` row | |
| Which run did it | `flow_run_id` (Prefect) on the same rows | |
| Was it given to us by a person? | `tides_events.event_type='manual_submission'`, `submitted_by` | §10.3 |
| What the selection function was | `pipelines` (name, version, `notes`, `created_at`) + `selection_functions/*.sql` + Lasair | **humans**, §10.2 |
| Which nights the survey observed | `survey_nights` | ingest |

**Not recorded automatically:**
- Hand edits made with SQL. There are no triggers. If you change `active` or similar by hand, insert a `tides_events` row yourself (§12.6).
- Ingest-side constants (`DEACTIVATE_AFTER_DAYS`, the 1″ radius, `FAINT_LIMIT_MAG`, payload rules). Record changes in `pipelines.notes` and in the git history.

### 10.2 Changing a selection function (checklist)

The selection function lives on Lasair. A copy is kept in `selection_functions/`. The version label lives in `pipelines`. These are maintained **by people**; nothing enforces it, so follow the checklist every time:

1. Edit the filter on Lasair. Commit the same SQL to `selection_functions/` (e.g. `Lasair-LSST-query-v1.1.sql`).
2. Add a `pipelines` row with a **new version** and a short note of what changed and why:
   ```sql
   INSERT INTO pipelines (pipeline_name, version, notes)
   VALUES ('TiDES-LSST-default', 'v1.1', '2027-01-15: require >=3 nights (was 2). See selection_functions/Lasair-LSST-query-v1.1.sql');
   ```
3. Set `LSST_PIPELINE_VERSION=v1.1` in the production `.env`. It takes effect on the next run; no rebuild is needed (§11.2).
4. If the change creates a new Kafka topic, also update `LASAIR_LSST_TOPIC`, and choose a **new** `LASAIR_LSST_GROUP_ID` for it.

From then on, new selections are recorded under the new `pipeline_id`. Objects already selected keep their original first-selection rows. Re-selection by the new version adds a new `pipeline_selections` row, because the primary key includes `pipeline_id`.

### 10.3 Targets given to us by a person or external source

A `manual` pipeline row exists (migration 002). There is **no dedicated CLI yet**. The supported way is to push a contract DataFrame through the same ingest function the flow uses, with `submitted_by` set. The object then goes through the same matching, events and 4MOST outbox as any alert; the next scheduled run sends it to 4MOST.
```python
# run from the repo root, against the right database (.env)
import sys, json; sys.path.insert(0, 'tides_flows')
from dotenv import load_dotenv; load_dotenv()
import pandas as pd, sqlalchemy
import tides_controller as tc

engine = tc.sqlalchmey_engine.fn()
with engine.connect() as c:
    manual_id = c.execute(sqlalchemy.text(
        "SELECT pipeline_id FROM pipelines WHERE pipeline_name='manual' AND version='v1.0'")).scalar()

target = pd.DataFrame([{
    'object_id': 'TNS2026abc',              # the external name
    'survey_id': 9,                          # survey_ids: 9 = TNS (pick the right source)
    'pipeline_id': manual_id,
    'ra': 150.12345, 'dec': -12.34567,      # degrees
    'jdmin': 61320.2, 'jdmax': 61321.2,     # MJD, not in the future
    'latest_mag':    json.dumps({'r_lsst': 19.4}),   # band key must be in fourmost_payload.MAG_TYPE_FOR_BAND
    'latest_filter': json.dumps({'r_lsst': 61321.2}),
    'n_sources':     json.dumps({'r_lsst': 1}),
}])
new_rows, updated = tc.ingest_survey_targets.fn(engine, target, survey_id=9, submitted_by='C. Frohmaier: TNS 2026abc')
print(new_rows, updated)
```
Notes:
- The magnitude must sit under a band key that 4MOST understands (`MAG_TYPE_FOR_BAND`). Otherwise the row becomes `not_submittable`.
- If the position is within 1″ of an existing object, it is merged into that object instead.
- Try it on a test database first.

### 10.4 SQL cookbook

```sql
-- Everything that ever happened to one object
SELECT event_time, event_type, reason, survey_id, survey_object_id, pipeline_id, submitted_by, details, git_sha
FROM tides_events
WHERE tides_id = (SELECT tides_id FROM tides_master WHERE name = 'TiDES26abc')
ORDER BY event_time, event_id;

-- Everything we told 4MOST about it, and what 4MOST answered
SELECT request_time, method, status_code, outcome, payload, left(response_text, 200) AS response
FROM tides_4most_log
WHERE tides_ids @> ARRAY[(SELECT tides_id FROM tides_master WHERE name = 'TiDES26abc')]::bigint[]
ORDER BY request_time;

-- How many objects each selection pipeline/version selected, and when
SELECT p.pipeline_name, p.version, ps.source_survey_id, count(*) AS n,
       min(ps.selection_time) AS first, max(ps.selection_time) AS last
FROM pipeline_selections ps JOIN pipelines p USING (pipeline_id)
GROUP BY 1, 2, 3 ORDER BY 1, 2, 3;

-- The active set as it was at a given moment (from events; ignores hand edits)
WITH last_change AS (
    SELECT DISTINCT ON (tides_id) tides_id, event_type
    FROM tides_events
    WHERE event_type IN ('created', 'activated', 'deactivated')
      AND event_time <= '2027-03-01 12:00+00'
    ORDER BY tides_id, event_time DESC, event_id DESC)
SELECT tm.name FROM last_change lc JOIN tides_master tm USING (tides_id)
WHERE lc.event_type <> 'deactivated';

-- Objects seen by more than one survey
SELECT tides_id, array_agg(DISTINCT survey_id) AS surveys
FROM tides_events WHERE event_type = 'alert'
GROUP BY tides_id HAVING count(DISTINCT survey_id) > 1;

-- All native ids recorded for one object
SELECT DISTINCT survey_id, survey_object_id FROM tides_events
WHERE tides_id = 123 AND event_type IN ('alert', 'manual_submission');

-- Why was it deactivated, and after how many nights?
SELECT tm.name, e.event_time, e.reason
FROM tides_events e JOIN tides_master tm USING (tides_id)
WHERE e.event_type = 'deactivated' ORDER BY e.event_time DESC LIMIT 20;
```

---

## 11. Operations

### 11.1 Environments

| Environment | Database | `TIDES_ENV` | `FOURMOST_MODE` | Kafka group |
|---|---|---|---|---|
| Laptop / development | your dev DB | `development` | `off` or `dry_run` | random (unset) |
| Tests | a local DB with `test` in its name | `development` | (tests use the in-memory fake) | – |
| Shadow (before go-live, §11.5) | production DB | `production` | `dry_run` | the fixed production group |
| Production | production DB | `production` | `live` | fixed, e.g. `tides-lsst-prod-1` |

### 11.2 Deploying with Prefect

`prefect.yaml` defines the build, the pull step and one deployment:
- **Build:** a Docker image `opr4_ingest:latest` (`dockerfile: auto` installs `requirements.txt`). The image contains **no secrets**: `.env` is in `.dockerignore`.
- **Pull:** set the working directory to `/opt/prefect/tides-ingest` in the container.
- **Deployment `opr4-deploy`:** entrypoint `tides_flows/tides_controller.py:run_target_workflow`, work pool `opr4`, `concurrency_limit: 1` (`CANCEL_NEW`).
  - The worker host's `.env` is **mounted read-only** at `/opt/prefect/tides-ingest/.env`.
  - `GIT_SHA` is injected for provenance.

One-time setup on the worker host (needs Docker and `pip install prefect-docker`):
```bash
prefect work-pool create opr4 --type docker
prefect worker start --pool opr4            # keep running (systemd/tmux)
```

Deploy (from the repo root, on a clean checkout of the commit you want to run):
```bash
TIDES_ENV_FILE=/absolute/path/to/production/.env \
GIT_SHA=$(git rev-parse --short=12 HEAD) \
PREFECT_SECRET_API=https://<your-prefect-server>/api \
prefect deploy --name opr4-deploy --cron "0 * * * *" --timezone UTC
```
- Without `--cron` the deployment has no schedule. You can also add a `schedules:` block to the deployment in `prefect.yaml`.
- Changing settings in the mounted `.env` takes effect at the next run, with **no rebuild**. Code changes need a re-deploy, which rebuilds the image.

**Other flows are not yet in `prefect.yaml`.** Suggested entries (copy the `work_pool` block from `opr4-deploy`):
```yaml
- name: tides-ostd-sync
  entrypoint: "db_4MOST_tasks/sync_ostd.py:sync_ostd_flow"
  schedules: [{cron: "30 12 * * *", timezone: "UTC"}]     # daily, after OpSys ingestion
- name: tides-host-matching
  entrypoint: "host_tools/tides_host_matching/tides_match_gal_local.py:match_galaxies_flow"
  schedules: [{cron: "0 14 * * *", timezone: "UTC"}]      # daily, not weekly (see §15 H6)
```

### 11.3 Recommended schedule

| Flow | Cadence | Why |
|---|---|---|
| Ingest (`opr4-deploy`) | every 1 h (overnight is enough, but hourly all day is harmless) | keeps 4MOST current. Each run is short, and the lock prevents overlap. |
| OSTD sync | daily | 4MOST ingests in batches. It only reads. |
| Host matching | daily | the flow only matches *active* transients, and a weekly run misses short-lived ones. |

### 11.4 Monitoring and health checks

- **Prefect UI:** ingest runs should be *Completed*. *Failed* means a survey, the deactivation step or the 4MOST step raised (read the logs; `log_prints` is on). The `tides-ingest-report` artifact shows what changed.
- **SQL health checks** (run daily, or wire them into a dashboard):
```sql
-- Outbox: should be small, and 'failed'/'conflict' should be zero
SELECT coalesce(sync_state, 'normal') AS state, count(*)
FROM tides_master WHERE sync_pending OR sync_state IS NOT NULL GROUP BY 1;

-- Rows needing a human
SELECT tides_id, name, sync_state, sync_attempts, sync_last_error
FROM tides_master WHERE sync_state IN ('failed', 'conflict') ORDER BY tides_id;

-- 4MOST requests in the last 7 days, by outcome
SELECT date_trunc('day', request_time) AS day, method, outcome, count(*)
FROM tides_4most_log WHERE request_time > now() - interval '7 days'
GROUP BY 1, 2, 3 ORDER BY 1 DESC, 2, 3;

-- Survey clock: last observing night per survey (as a date)
SELECT survey_id, date '1858-11-17' + max(night) AS last_night, count(*) AS nights
FROM survey_nights GROUP BY 1;

-- Size of the active set
SELECT active, count(*) FROM tides_master GROUP BY 1;
```
- **Red flags:**
  - `ambiguous` rows that persist for more than a day (4MOST lookups are failing);
  - many `retry` outcomes (4MOST down; the run stays green, see §15);
  - the survey clock not advancing during the observing season (Lasair or Kafka problem);
  - `Dropping alert` lines in the logs (plugin and Lasair columns out of step).

### 11.5 Go-live checklist

1. 4MOST confirms the wipe. A GET of survey 15, subsurvey `tides-sn`, returns **0** targets.
2. Fresh production database:
   - `CREATE EXTENSION q3c`;
   - `setupDB.sh`;
   - `migrate.sh`;
   - check `SELECT * FROM schema_migrations` lists 001–003.
3. Production `.env`:
   - `TIDES_ENV=production`;
   - a fixed `LASAIR_LSST_GROUP_ID`;
   - `SURVEYS_ENABLED=lsst`;
   - `DEACTIVATE_AFTER_DAYS` decided;
   - `LSST_PIPELINE_VERSION` matches the Lasair filter.
4. **Shadow night:** deploy with `FOURMOST_MODE=dry_run` and let it run for a night. Inspect the would-be payloads:
   ```sql
   -- batch POSTs are logged as a JSON array, single requests as an object: unpack both
   SELECT p->>'name' AS name, p->>'mag' AS mag, p->>'mag_type' AS mag_type, p->>'dec' AS dec, l.request_time
   FROM tides_4most_log l,
        jsonb_array_elements(CASE WHEN jsonb_typeof(l.payload) = 'array' THEN l.payload
                                  ELSE jsonb_build_array(l.payload) END) AS p
   WHERE l.outcome = 'dry_run' ORDER BY l.request_time DESC LIMIT 50;
   ```
5. Switch to `FOURMOST_MODE=live` (edit `.env`; no re-deploy needed).
6. Host flow: leave 4MOST submission off until §15 H1–H8 are fixed.
7. Watch the health checks (§11.4) closely for the first week.

### 11.6 Backups

```bash
pg_dump -Fc -h $HOST -U $USER -d $DB -f tides_$(date +%F).dump        # back up
pg_restore -h $HOST -U $USER -d <empty_db> tides_YYYY-MM-DD.dump        # restore
```
Back up daily during the survey. `tides_events` and `tides_4most_log` are the provenance record and cannot be rebuilt.

---

## 12. Runbooks

### 12.1 4MOST is down or erroring
- Nothing to do immediately. Rows stay `sync_pending`, POSTs with an unknown outcome are `ambiguous`, and the next run reconciles them.
- Check what is happening:
  ```sql
  SELECT request_time, method, status_code, outcome, left(response_text, 200)
  FROM tides_4most_log ORDER BY log_id DESC LIMIT 20;
  ```
- If it lasts more than a day, tell 4MOST. When it recovers, watch the first run reconcile.

### 12.2 Objects stuck in `ambiguous`
- The lookup (`GET ?name=…&subsurvey=tides-sn`) is failing, or 4MOST is still down.
- If 4MOST is up, test the lookup by hand with the CLI (§8.1, `--filter`).
- A row that has become `conflict` needs §12.4.

### 12.3 Objects in `failed`
1. Read the reason:
   ```sql
   SELECT tides_id, name, sync_last_error FROM tides_master WHERE sync_state = 'failed';
   ```
   For the full response:
   ```sql
   SELECT * FROM tides_4most_log WHERE tides_ids @> ARRAY[<id>]::bigint[] ORDER BY log_id DESC LIMIT 3;
   ```
2. Fix the cause: bad data, a payload rule 4MOST changed (then fix `fourmost_payload.py`), or a credentials problem.
3. Release the row so the next run retries it:
   ```sql
   UPDATE tides_master SET sync_state = NULL WHERE tides_id = <id>;
   ```

### 12.4 Objects in `conflict`
- 4MOST has several targets with this name, or one at a different position.
- Look in 4MOST: `--list --filter "name=<name>&subsurvey=tides-sn"`. Decide which 4MOST id is ours, then:
  ```sql
  UPDATE tides_master SET pk_4most = <id>, sync_state = NULL WHERE tides_id = <tides_id>;
  ```
- Leave `sync_pending` TRUE if the active state still needs sending.
- Ask 4MOST to switch off (`is_active=false`) any extra target. **Never** touch targets of subsurvey `source`.

### 12.5 LSST maintenance or a long gap
- Nothing to do. No alerts means no new observing nights, so nothing is deactivated, and no flip-flop.
- During the gap: the clock (`survey_nights`) stops, and Kafka offsets stay where they were.
- When the survey returns: the first run reads the backlog (up to `KAFKA_MAX_MESSAGES` per run) and carries on.

### 12.6 Switching one object off (or on) by hand
```sql
BEGIN;
UPDATE tides_master SET active = FALSE, sync_pending = TRUE WHERE name = 'TiDES26abc';
INSERT INTO tides_events (tides_id, event_type, reason, submitted_by, details)
SELECT tides_id, 'deactivated', 'manual', 'your name', '{"why": "AGN, per classification"}'::jsonb
FROM tides_master WHERE name = 'TiDES26abc';
COMMIT;
```
- The next run PATCHes 4MOST.
- **A new detection will reactivate it.** There is no "blocked" flag yet (§15). If an object must stay off, raise it with the team.

### 12.7 Re-reading Kafka (replay)
- Set a **new** `LASAIR_LSST_GROUP_ID` (e.g. `tides-lsst-prod-2`). The next run reads everything Lasair still retains.
- This is safe: replays change nothing (§7.7).
- Lasair's retention is limited (days to weeks), so a replay can't recover very old alerts.

### 12.8 Adding a new survey (e.g. ATLAS)
1. **Survey id:** `survey_ids` already has LS4 3, ATLAS 5, GOTO 6, HSC 7, BlackGEM 8, TNS 9. Add a row for anything else.
2. **Pipeline.** ATLAS already has a `v1.0` row; for any other survey insert one:
   ```sql
   INSERT INTO pipelines (pipeline_name, version, notes)
   VALUES ('TiDES-ATLAS-default', 'v1.0', 'first ATLAS selection; filter in selection_functions/…')
   ON CONFLICT (pipeline_name, version) DO NOTHING;
   ```
3. **Plugin:** create `tides_flows/tides_atlas.py` by copying `tides_lsst.py`. Implement `get_targets(pipeline_id, topic, group_id, max_messages=20000) -> (DataFrame, consumer)`:
   - use `stream_utils.open_consumer` and `stream_utils.read_messages(consumer, id_key=...)`;
   - return the contract (§7.4): **MJD** times, band keys `<band>_atlas`, only real detections, `json.dumps(..., allow_nan=False)`;
   - never return `None`;
   - close the consumer on error.
4. **4MOST labels:** add the survey's bands to `fourmost_payload.MAG_TYPE_FOR_BAND`, mapped to the nearest whitelisted 4MOST system. Bands without a mapping are never used for the 4MOST magnitude.
5. **Controller:**
   - add `'atlas': 5` to `SURVEY_IDS`;
   - add a `fetch_atlas_targets` task, modelled on `fetch_lsst_targets`, with its own `ATLAS_PIPELINE_*`, `LASAIR_ATLAS_TOPIC` and `LASAIR_ATLAS_GROUP_ID` env vars;
   - add it to the `fetchers` dict in `fetch_survey`.
6. **Mock and test:** add mock rows to `testing/mock_streams.py`, and the survey to the `test_mode` list in `run_target_workflow`. Run §13.
7. **Enable:** `SURVEYS_ENABLED=lsst,atlas`. Record the change in `pipelines.notes`.

### 12.9 Changing `DEACTIVATE_AFTER_DAYS` or other selection-relevant settings
- Edit `.env`; it takes effect at the next run.
- These settings are part of the effective selection function, so write the change, with its date, into `pipelines.notes` of the current version, or create a new version (§10.2).

### 12.10 Credentials
- Rotate them in the worker's `.env`. They are read at the start of each run; no rebuild is needed.
- Never put credentials on a command line in shared shells: `--password` ends up in shell history.

### 12.11 Year rollover
- Automatic. The first object inserted in a new year (by the database server's date) gets `TiDESyyaaa`.
- Make sure only one ingest is running (the lock guarantees it).

### 12.12 Using `qmost_db_utils`
- `query_tides_db.py` and `cleanup_by_date.py` deactivate or delete targets on 4MOST.
- After the 2026-10 patch their **write actions refuse to run**: they don't switch on `WRITES_ALLOWED`. `--dry-run` still works.
- This is intentional until their dangerous defaults are fixed (§15 U1): `--where` defaults to `active = True`, there's no subsurvey filter on a **shared** survey, and there's no confirmation.

### 12.13 Updating `submit_transients.py` from 4MOST
1. Save the TiDES block: `WRITES_ALLOWED`, `PRODUCTION_HOST`, `FOURMOST_SURVEY_ID`, `check_writes_allowed`, `configure_from_env`. Also note the `check_writes_allowed(url)` calls in create/update/delete, the 2xx `status_ok` line, and `WRITES_ALLOWED = True` in the CLI write actions.
2. Replace the file with 4MOST's new version, then re-apply the block (`git diff` shows it clearly).
3. Run `python testing/test_4most_downtime.py`.

### 12.14 Upgrading dependencies
- Change a pin in `requirements.txt` and run §13 locally.
- Then re-deploy (the image is rebuilt).
- Upgrade Prefect deliberately: `prefect.yaml` and the CLI change between major versions.

### 12.15 Schema changes
- Add `database_setup/migrations/00N_description.sql`. Make it idempotent (`IF NOT EXISTS`, …).
- Run `./migrate.sh` (from `database_setup/`) on each database.
- **Never edit an applied migration.** Write a new one.

---

## 13. Testing

| Test | Command | Needs | What it proves |
|---|---|---|---|
| Mock pipeline | `TIDES_DB_DATABASE=<test db> python -c "…run_target_workflow(test_mode=True)"` (§5 step 7) | a test DB with the schema | matching, the LSST+ZTF merge, events, survey clock, deactivation; never writes 4MOST |
| 4MOST downtime | `python testing/test_4most_downtime.py` | `TIDES_TEST_DB_DATABASE` (local; name contains `test`; **truncated**) | down → `ambiguous` → recovery adopts targets by name; **no duplicates**; no request leaves the machine |
| Host matching | `python testing/test_host_matching.py` | test DB; **live Lasair Sherlock**; a valid Lasair token+endpoint (§9) | association, catalogue dedup, queue scenarios. 4MOST is faked |

**Adding a case:** add rows to `testing/mock_streams.py`:
- follow the contract (§7.4);
- use MJD dates in the **past** (the validator drops future dates);
- expect deactivation if a mock object is more than `DEACTIVATE_AFTER_DAYS` *observing nights* older than the newest mock alert of its survey.

**Testing against 4MOST behaviour:** use `testing/fake_4most.py`. After `fake = fake_4most.install()`, every request goes to an in-memory fake, and `fake.fail_mode = 'down' | 'lose_response' | None`. **Never** test against the real 4MOST.

There are no pytest-collectable tests yet: the files are scripts.

---

## 14. Safety rules

1. **Production 4MOST is real and shared.**
   - Only the production deployment has `TIDES_ENV=production` and `FOURMOST_MODE=live`.
   - Everything else uses `off`, `dry_run`, or the fake.
2. **Never act on subsurvey `source` targets.** Survey 15 is shared with another programme.
3. **Destructive scripts:**
   - `database_setup/databaseHardReset.py/.sh` and `DELETE-deleteAllTables.sql` truncate or drop `tides_master` **in whatever `TIDES_DB_DATABASE` points at** (the hard-reset scripts only ask you to type `RESET`).
   - Check `.env` twice. Never run them where production is reachable.
   - A reset also re-issues names (`TiDES26aaa`…) that may already exist in 4MOST.
4. **Tests truncate** their database. They refuse non-local hosts and names without `test`/`mock`, but they trust you on the rest.
5. **Don't edit applied migrations, and don't hand-edit `tides_events` / `tides_4most_log`.** Append only.
6. **Before any bulk change on 4MOST, back up the database** (§11.6) and dry-run first.

---

## 15. Known limitations and open work

From the October 2026 review (the full report is in the maintainer's notes, `implementation_plans/code_review_2026-10-08*`).

**Ingest**
- **R1 (4MOST down is a green run).** The circuit breaker stops quietly. Suggested fix: in `fourmost_sync.sync_pending_to_4most`, `raise RuntimeError(...)` instead of `print(...)` when `client.is_down`. The flow then finishes and is marked Failed.
- **R2 (dropped alerts aren't queryable).** Alerts dropped by validation are only printed, and their Kafka offsets are committed. Add a `tides_dead_letter` table if you need to audit them.
- **R3 (one native id per survey).** `surveys` keeps only the first native id per survey; all ids are in `tides_events`. Change the PK to `(tides_id, source_survey_id, transient_name)` if you need them there.
- **R4 (no "blocked" flag).** A manually deactivated object is reactivated by its next detection.
- **R5 (vendored client edited).** The 4MOST client is edited in place (§12.13). The alternative is a wrapper module, plus sending the 2xx fix upstream.
- **R6 (dead code).** `submit_to_4most`, `map_dtype` and other old helpers in `tides_controller.py`; `tidesCom.py`; `ingest_report.py`; `opr4-test-pipeline.py` (empty); `database_setup/createZTFTable.sql` (truncated); `check_imports.py`.

**Z: before enabling ZTF** (`SURVEYS_ENABLED=lsst,ztf`)
- Align the Lasair ZTF query's output columns with `tides_ztf.py` (`objectId, ra, decl, first_jd, latest_jd` + `latestFilter/latestMag` or `gmag/rmag`).
- Replace the `n_sources = 1` placeholder.
- Remember: ZTF reaches north of +40, and those objects are `not_submittable` for 4MOST (they're still tracked).

**H: host flow (§9)**
- **H1** Lasair endpoint and token pairing (`Unauthorized`, swallowed).
- **H2** One 4MOST target per association, not per galaxy. The same galaxy appears under several catalogue names (`NGC4415` vs `NGC 4415`), so de-duplicate by **position** (e.g. 1.5″).
- **H3** Host payload breaks the spec:
  - `cadence` must be 0 (it is 1048576);
  - `extent_flag=1` declares a surface brightness, but a total magnitude is sent;
  - `mag_type` is Sherlock's raw filter name, which is not on the whitelist.
- **H4** `retries=2` around one transaction while POSTs are sent immediately gives duplicate POSTs. PATCH results aren't checked.
- **H5** Sherlock errors are swallowed.
- **H6** It only matches *active* transients: run it daily, or widen the window.
- **H7** Number of hosts submitted per transient should depend on association confidence (max 3). A model-free rule based on Sherlock's reliability flag and offset/galaxy size has been proposed.
- **H8** Should a host be held back while *another* transient in the same galaxy is active?

**U: utilities**
- **U1** `qmost_db_utils`:
  - `--where` must become required (it defaults to `active = True`);
  - always filter on subsurvey;
  - dry-run by default, with `--confirm`;
  - then enable writes behind `--confirm`.

**Docs and housekeeping**
- `.env.example` and the old guides have been updated with this guide. Keep them in step.

**Open questions for 4MOST**
1. Is batch create all-or-nothing on 4xx? (We fall back to one-by-one either way.)
2. Can a target already ingested into the OSTD be **re**-activated with PATCH `is_active=true`? Fade → rebrighten depends on it.
3. Is a `mag`/`mag_type` PATCH accepted after OSTD ingestion (milestones)?
4. Does the exposure-time calculator use `MAG`? Under the milestone-only policy, a faded target keeps its brightest reported magnitude.
5. What do `t_exp_b/s = 4300` vs `t_exp_d/g = 38` mean?
6. Is survey 5 (`S_test`) usable for our development?

---

## 16. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `RuntimeError: Refusing to write to the PRODUCTION 4MOST API … TIDES_ENV is not 'production'` | `FOURMOST_MODE=live` on a non-production machine | Use `off`/`dry_run`, or set `TIDES_ENV=production` **only** in production. |
| `RuntimeError: 4MOST writes are switched off (FOURMOST_MODE is not 'live')` | something called create/update/delete without `configure_from_env()` (host flow, utils) or in `off`/`dry_run` | expected (§9, §12.12) |
| `RuntimeError: LASAIR_LSST_GROUP_ID must be set in production` | production without a fixed group id | set it in `.env` |
| `ValueError: CRITICAL FIX REQUIRED: Pipeline '…' version '…' does not exist` | `*_PIPELINE_NAME/VERSION` don't match a `pipelines` row | insert the row (§10.2) or fix `.env` |
| `TimeoutError: No Kafka partition assignment after 60 s` | Kafka unreachable, wrong topic, or network | check `LASAIR_LSST_TOPIC` and connectivity; it retries next run |
| Run is green but nothing was ingested for days | the Lasair filter passes nothing; or the topic name changed | check Lasair; the survey clock (§11.4) |
| Many `Dropping alert … is not a plausible MJD` | a plugin received JD (or future dates) | fix the plugin's unit conversion (§7.4) |
| `FileNotFoundError: ./sql_tasks/…` | run from the wrong directory | run from the repo root (the deployment sets this) |
| `psql: … password authentication failed` in `setupDB.sh`/`migrate.sh` | the scripts pass no password | use `~/.pgpass` or `PGPASSWORD`; check `TIDES_DB_*` in `../.env` |
| Prefect fails at start-up with `alembic … Can't locate revision identified by '…'` | the local Prefect database (`~/.prefect/prefect.db`) was created by a different Prefect version than the one installed | use a fresh Prefect home for this environment (`export PREFECT_HOME=~/.prefect-tides`), or `prefect server database reset` (erases local run history) |
| `test_mode needs a test database …` | `test_mode=True` against a DB without `test`/`scratch`/`mock` in its name | point `TIDES_DB_DATABASE` at a test DB |
| Objects flip on/off every few days | `DEACTIVATE_AFTER_DAYS` too small for the survey cadence | increase it (§12.9) |
| Two TiDES objects for one transient | they were > 1″ apart, or created before the 2026-10 patch | merge by hand (update survey ids/events to one `tides_id`, deactivate the other); review the 1″ radius if it recurs |
| Host flow finds no hosts | Lasair `Unauthorized` (§9) | fix the endpoint/token pairing |
| `duplicate key value violates unique constraint "tides_master_name_key"` | the name sequence was reset (hard reset), or the year cap was reached | never reset production; check `SELECT last_value FROM tides_seq` |

---

## 17. Glossary

| Term | Meaning |
|---|---|
| **4MOST** | 4-metre Multi-Object Spectroscopic Telescope (fibre spectrograph on ESO VISTA). |
| **OpSys / OSTD** | 4MOST operations system, and its target database. |
| **Lasair** | UK alert broker for LSST/ZTF. Runs our SQL filter and publishes Kafka topics. Also hosts **Sherlock** (contextual crossmatch, used for host galaxies). |
| **LSST / Rubin** | Legacy Survey of Space and Time at the Vera C. Rubin Observatory. `diaObject` = an LSST difference-image object; `diaObjectId` its id. |
| **ZTF** | Zwicky Transient Facility. |
| **MJD / JD** | Modified Julian Date = JD − 2400000.5. TiDES stores MJD; 4MOST date fields are JD. |
| **dayObs** | Rubin's observing-night date = floor(MJD − 0.5). |
| **q3c** | PostgreSQL extension for fast sky-position queries (`q3c_join`, `q3c_radial_query`, `q3c_dist`). |
| **Prefect** | Workflow orchestrator. *Flow* = a run; *task* = a step; *deployment* = a scheduled flow; *work pool / worker* = where runs execute. |
| **Kafka consumer group / offset** | How Kafka remembers how far a reader got. |
| **Outbox** | The pattern of marking rows `sync_pending` and pushing them to an external system later. |
| **Subsurvey / ruleset / template / cadence** | 4MOST target-catalogue fields (§8.2–8.3). |
| **Milestone** | 18/17/16/15 mag brightness thresholds reported to 4MOST. |

---

## 18. History and references

- **2025-11:** first ingest scripts (ZTF via Lasair) for the 4MOST operational rehearsal OPR4. Hence names like `opr4-deploy` and the flow name "delta OPR4 Workflow".
- **2026-01 → 02:** OPR4 build-out: Postgres staging/upsert, 4MOST submission, Docker/Prefect deployment.
- **2026-04 → 07:** LSST plugin, per-band JSONB magnitudes, the `pipelines` / `pipeline_selections` tables, the test harness, the host-galaxy flow, the `sync_pending` outbox, `sync_ostd`, `qmost_db_utils`. Parts of this period were developed with an AI assistant (Gemini).
- **2026-10:** full code review before LSST and 4MOST go-live. It added:
  - safe Kafka offsets;
  - per-survey isolation and the run lock;
  - the 4MOST outbox rewrite;
  - deterministic, duplicate-free matching;
  - the observing-night survey clock;
  - milestone magnitudes and spec-checked payloads;
  - the provenance tables;
  - migrations;
  - secrets kept out of the image.
- **References:**
  - 4MOST target catalogue specification: <https://4most.mpe.mpg.de/static/manual/Target_catalog_format_specification.htm>
  - Lasair: <https://lasair-lsst.lsst.ac.uk>
  - q3c: <https://github.com/segasai/q3c>
  - Prefect 3 docs: <https://docs.prefect.io>
