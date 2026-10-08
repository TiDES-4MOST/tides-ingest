"""
fourmost_sync.py

Pushes tides_master to the 4MOST transients API. tides_master is the "outbox":

  sync_pending = TRUE   4MOST still has to hear about this row (new target, or is_active changed)
  pk_4most              the 4MOST id, once 4MOST has confirmed it created the target
  mag_4most             the magnitude 4MOST has for it (for the 18/17/16/15 milestone updates)

Rules:
  1. sync_pending is only cleared when 4MOST CLEARLY confirmed the change: HTTP 2xx, a JSON body,
     and (for PATCH) the body shows the value we asked for. An error body is never a success.
  2. A row is marked sync_state='ambiguous' (and committed) BEFORE its POST is sent, and only
     un-marked when 4MOST's answer is clear. So a POST whose outcome we cannot know (timeout,
     connection reset, 5xx, an HTML page with 200, or this process dying mid-request) stays
     'ambiguous' and is looked up by name + subsurvey + position BEFORE it is ever POSTed again.
     That is what stops duplicates (4MOST does not require transient names to be unique).
  3. If a batch POST is rejected (4xx), the items are retried one by one, so one bad row
     cannot block the other 49 forever.
  4. After 3 consecutive network/server failures we stop talking to 4MOST for this run.

sync_state (NULL = normal):
  ambiguous        POST outcome unknown -> looked up by name before any re-POST (automatic)
  failed           4MOST rejected this row (4xx), or answered something unexpected. Needs a human:
                   read sync_last_error, fix, then  UPDATE tides_master SET sync_state = NULL ...
  conflict         the name lookup found several 4MOST targets, or one at a different position.
                   Needs a human (look in 4MOST; never auto-deleted).
  not_submittable  the payload builder refused it (e.g. dec > +40). Re-checked every run.
"""

import math
from urllib.parse import quote

import pandas as pd
import requests
from prefect import task
from prefect.cache_policies import NO_CACHE
from sqlalchemy import text

import fourmost_payload as fp
import provenance
import submit_transients as st

BATCH_SIZE = 50
TIMEOUT_SECONDS = 60
REJECTED_CODES = {400, 404, 409, 422}   # 4MOST looked at THIS item and said no
MATCH_RADIUS_ARCSEC = 1.0


class FourmostClient:
    """Sends requests, logs every one to tides_4most_log, and stops after repeated failures."""

    def __init__(self, conn, mode, max_consecutive_failures=3):
        self.conn = conn
        self.mode = mode                       # 'dry_run' or 'live'
        self.failures = 0
        self.max_failures = max_consecutive_failures

    @property
    def is_down(self):
        return self.failures >= self.max_failures

    def call(self, method, tides_ids, payload=None, pk=None, flt=None):
        """
        Returns (outcome, body). outcome is one of:
          'ok'        2xx with a JSON body (callers still check the body makes sense)
          'rejected'  4MOST refused this item (400/404/409/422) - retrying the same thing won't help
          'ambiguous' POST only: it may or may not have been created
          'retry'     nothing changed on 4MOST (or PATCH/GET, which are safe to repeat): try next run
          'dry_run'   nothing was sent
        """
        url = st.URL_SCHEMA if pk is None else f"{st.URL_SCHEMA.rstrip('/')}/{pk}/"
        if flt:
            url = f"{st.URL_SCHEMA.rstrip('/')}/?{flt}"
        if self.mode == 'dry_run' and method != 'GET':
            provenance.log_4most(self.conn, tides_ids, method, url, payload, None, 'dry_run', None)
            return 'dry_run', None

        response, error = None, None
        try:
            if method == 'POST':
                response = st.create_transient(data=payload, printout=False, timeout=TIMEOUT_SECONDS, return_mode='response')
            elif method == 'PATCH':
                response = st.update_transient(pk=pk, data=payload, printout=False, timeout=TIMEOUT_SECONDS, return_mode='response')
            else:
                response = st.get_list(flt=flt, timeout=TIMEOUT_SECONDS, return_mode='response')
        except requests.exceptions.InvalidJSONError as e:
            error = e                          # payload could not be encoded: never left this machine
        except requests.exceptions.RequestException as e:
            error = e                          # timeout / connection problem

        body = None
        if response is not None:
            try:
                body = response.json()
            except ValueError:
                body = None                    # e.g. an HTML "Internal error" page

        if isinstance(error, requests.exceptions.InvalidJSONError):
            outcome = 'rejected'
        elif error is not None:
            outcome = 'ambiguous' if method == 'POST' else 'retry'
        elif 200 <= response.status_code < 300 and body is not None:
            outcome = 'ok'
        elif response.status_code in REJECTED_CODES:
            outcome = 'rejected'
        elif method == 'POST' and response.status_code not in (401, 403, 429):
            outcome = 'ambiguous'              # 5xx, or 2xx with a non-JSON body
        else:
            outcome = 'retry'

        self.failures = self.failures + 1 if outcome in ('ambiguous', 'retry') else 0
        provenance.log_4most(self.conn, tides_ids, method, url, payload,
                             None if response is None else response.status_code, outcome,
                             repr(error) if error is not None else response.text)
        if outcome != 'ok':
            status = 'no response' if response is None else response.status_code
            print(f"4MOST {method} for tides_id(s) {list(tides_ids)[:5]}: {outcome} ({status}) {error or response.text[:200]}")
        return outcome, body


# ---------------------------------------------------------------- row state updates

def mark_synced(conn, tides_id, pk=None, mag=None):
    conn.execute(text("""
        UPDATE tides_master
        SET sync_pending = FALSE, sync_state = NULL, sync_last_error = NULL, sync_attempts = 0,
            pk_4most = COALESCE(:pk, pk_4most), mag_4most = COALESCE(:mag, mag_4most)
        WHERE tides_id = :id"""), {'id': int(tides_id), 'pk': pk, 'mag': mag})


def mark_problem(conn, tides_id, state, error):
    """state=None keeps the current sync_state (used for 'try again next run')."""
    conn.execute(text("""
        UPDATE tides_master
        SET sync_state = COALESCE(:state, sync_state), sync_attempts = sync_attempts + 1,
            sync_last_error = left(:err, 1000)
        WHERE tides_id = :id"""), {'id': int(tides_id), 'state': state, 'err': str(error)})


def handle_post_result(conn, rows, outcome, body):
    """Apply one POST outcome to the rows that were in it. Returns the rows that were created."""
    created = []
    if outcome == 'ok':
        items = body if isinstance(body, list) else [body]
        by_name = {str(i.get('name')): i for i in items if isinstance(i, dict) and i.get('id') is not None}
        for row in rows:
            item = by_name.get(str(row['name']))
            if item is None:
                mark_problem(conn, row['tides_id'], 'ambiguous', '2xx response did not list this name')
            else:
                mark_synced(conn, row['tides_id'], pk=int(item['id']), mag=row['payload']['mag'])
                provenance.log_events(conn, [row], '4most_created', details={'pk_4most': int(item['id'])})
                created.append({**row, 'pk_4most': int(item['id'])})
    elif outcome in ('rejected', 'ambiguous'):
        for row in rows:
            mark_problem(conn, row['tides_id'], 'failed' if outcome == 'rejected' else 'ambiguous',
                         f"POST {outcome}: {str(body)[:500]}")
    elif outcome == 'retry':                                     # e.g. 401/429: not created
        for row in rows:
            mark_problem(conn, row['tides_id'], None, f"POST not accepted, will retry: {str(body)[:500]}")
            # (stays 'ambiguous' from the write-ahead mark: costs one cheap GET next run)
    conn.commit()
    return created


# ---------------------------------------------------------------- the steps

def angular_sep_arcsec(ra1, dec1, ra2, dec2):
    ra1, dec1, ra2, dec2 = map(math.radians, (ra1, dec1, ra2, dec2))
    h = math.sin((dec2 - dec1) / 2) ** 2 + math.cos(dec1) * math.cos(dec2) * math.sin((ra2 - ra1) / 2) ** 2
    return math.degrees(2 * math.asin(min(1.0, math.sqrt(h)))) * 3600.0


def results_of(body):
    """4MOST list responses are either a list, or a paginated {'count', 'next', 'results'} dict."""
    if isinstance(body, dict):
        return body.get('results', [body]), body.get('next')
    return (body or []), None


def reconcile_ambiguous(client, conn):
    """Step 1: for every 'ambiguous' row, find out whether 4MOST already has it."""
    rows = pd.read_sql(text("SELECT * FROM tides_master WHERE sync_state = 'ambiguous' ORDER BY tides_id"), conn)
    conn.commit()
    for row in rows.to_dict('records'):
        if client.is_down:
            break
        flt = f"name={quote(str(row['name']))}&subsurvey=tides-sn"
        outcome, body = client.call('GET', [row['tides_id']], flt=flt)
        if outcome != 'ok':
            conn.commit()
            continue                                            # still ambiguous; next run
        found, next_page = results_of(body)
        if any(str(f.get('name')) != str(row['name']) or f.get('subsurvey') != 'tides-sn' for f in found):
            mark_problem(conn, row['tides_id'], None, 'name lookup returned other targets: filter not applied?')
        elif not found:                                         # never created: POST again below
            conn.execute(text("""
                UPDATE tides_master SET sync_state = NULL, sync_last_error = 'lookup: not in 4MOST, will POST again'
                WHERE tides_id = :id"""), {'id': int(row['tides_id'])})
        elif len(found) == 1 and next_page is None and angular_sep_arcsec(
                row['ra'], row['dec'], float(found[0]['ra']), float(found[0]['dec'])) <= MATCH_RADIUS_ARCSEC:
            item = found[0]                                     # it WAS created: adopt its id
            conn.execute(text("""
                UPDATE tides_master
                SET pk_4most = :pk, mag_4most = :mag, sync_state = NULL, sync_last_error = NULL,
                    sync_pending = (active IS DISTINCT FROM :is_active)
                WHERE tides_id = :id"""),
                {'pk': int(item['id']), 'mag': item.get('mag'), 'is_active': bool(item.get('is_active')),
                 'id': int(row['tides_id'])})
            provenance.log_events(conn, [row], '4most_created', reason='adopted_after_ambiguous_post',
                                  details={'pk_4most': int(item['id'])})
        else:
            mark_problem(conn, row['tides_id'], 'conflict',
                         f"{len(found)} 4MOST target(s) named {row['name']} (or wrong position): ids "
                         f"{[f.get('id') for f in found]}")
        conn.commit()


def clear_inactive_unregistered(conn):
    """Step 2: inactive and never registered (and not ambiguous): nothing to tell 4MOST."""
    conn.execute(text("""
        UPDATE tides_master SET sync_pending = FALSE
        WHERE sync_pending AND NOT active AND pk_4most IS NULL
          AND (sync_state IS NULL OR sync_state = 'not_submittable')"""))
    conn.commit()


def post_new_targets(client, conn):
    """Step 3: POST new active targets in batches; fall back to one-by-one if a batch is rejected."""
    rows = pd.read_sql(text("""
        SELECT * FROM tides_master
        WHERE sync_pending AND active AND pk_4most IS NULL
          AND (sync_state IS NULL OR sync_state = 'not_submittable')
        ORDER BY tides_id"""), conn)
    conn.commit()
    ready = []
    for row in rows.to_dict('records'):
        payload, reason = fp.build_transient_payload(row, st.FOURMOST_SURVEY_ID)
        if payload is None:
            mark_problem(conn, row['tides_id'], 'not_submittable', reason)
        else:
            ready.append({**row, 'payload': payload})
    conn.commit()

    created = []
    for i in range(0, len(ready), BATCH_SIZE):
        if client.is_down:
            break
        batch = ready[i:i + BATCH_SIZE]
        if client.mode == 'live':
            # Write-ahead: from the moment we send until we know the answer, these rows are
            # 'ambiguous'. If the process dies mid-request they get looked up, never re-POSTed blind.
            conn.execute(text("UPDATE tides_master SET sync_state = 'ambiguous' WHERE tides_id = ANY(:ids)"),
                         {'ids': [int(r['tides_id']) for r in batch]})
            conn.commit()
        outcome, body = client.call('POST', [r['tides_id'] for r in batch], payload=[r['payload'] for r in batch])
        if outcome == 'rejected' and len(batch) > 1:
            for row in batch:                                   # find the bad one(s)
                if client.is_down:
                    break
                one_outcome, one_body = client.call('POST', [row['tides_id']], payload=row['payload'])
                created += handle_post_result(conn, [row], one_outcome, one_body)
        elif outcome != 'dry_run':
            created += handle_post_result(conn, batch, outcome, body)
        conn.commit()
    return created


def patch_active_flags(client, conn):
    """Step 4: tell 4MOST about is_active changes. Checks that 4MOST echoes the new value."""
    rows = pd.read_sql(text("""
        SELECT * FROM tides_master
        WHERE sync_pending AND pk_4most IS NOT NULL AND sync_state IS NULL
        ORDER BY tides_id"""), conn)
    conn.commit()
    done = []
    for row in rows.to_dict('records'):
        if client.is_down:
            break
        want = bool(row['active'])
        outcome, body = client.call('PATCH', [row['tides_id']], payload={'is_active': want}, pk=int(row['pk_4most']))
        if outcome == 'ok' and isinstance(body, dict) and body.get('is_active') == want:
            mark_synced(conn, row['tides_id'])
            done.append(row)
        elif outcome == 'ok':
            mark_problem(conn, row['tides_id'], 'failed', f"PATCH is_active={want} not echoed: {str(body)[:500]}")
        elif outcome == 'rejected':
            mark_problem(conn, row['tides_id'], 'failed', f"PATCH rejected: {str(body)[:500]}")
        elif outcome == 'retry':
            mark_problem(conn, row['tides_id'], None, f"PATCH failed, will retry: {str(body)[:500]}")
        conn.commit()
    return done


def patch_mag_milestones(client, conn):
    """
    Step 5: PATCH mag + mag_type when an active target brightened past 18/17/16/15.
    Nothing is stored to say "a milestone is due": it is recomputed from the data every run
    and stops being due as soon as mag_4most is updated. A rejected milestone PATCH is logged
    and simply tried again next run (it does not block is_active updates).
    """
    rows = pd.read_sql(text("""
        SELECT tides_id, name, pk_4most, latest_mags, latest_mjd, mag_4most FROM tides_master
        WHERE active AND pk_4most IS NOT NULL AND sync_state IS NULL
          AND (mag_4most IS NULL OR mag_4most > 15)
        ORDER BY tides_id"""), conn)
    conn.commit()
    for row in rows.to_dict('records'):
        update = fp.milestone_mag_update(row)
        if update is None:
            continue
        if client.is_down:
            break
        outcome, body = client.call('PATCH', [row['tides_id']], payload=update, pk=int(row['pk_4most']))
        if (outcome == 'ok' and isinstance(body, dict) and fp.is_number(body.get('mag'))
                and abs(float(body['mag']) - update['mag']) < 1e-3):
            conn.execute(text("UPDATE tides_master SET mag_4most = :mag WHERE tides_id = :id"),
                         {'mag': update['mag'], 'id': int(row['tides_id'])})
            provenance.log_events(conn, [row], '4most_mag_update', details=update)
        elif outcome != 'dry_run':
            conn.execute(text("UPDATE tides_master SET sync_last_error = left(:err, 1000) WHERE tides_id = :id"),
                         {'err': f"mag PATCH {outcome}: {str(body)[:500]}", 'id': int(row['tides_id'])})
        conn.commit()


@task(cache_policy=NO_CACHE)
def sync_pending_to_4most(engine, mode):
    """
    Runs steps 1-5 above. mode: 'off' (do nothing), 'dry_run' (build + log payloads only), 'live'.
    Returns (new, updated, deactivated) DataFrames for the ingest report.
    """
    if mode == 'off':
        print("FOURMOST_MODE=off: not talking to 4MOST.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    with engine.connect() as conn:
        client = FourmostClient(conn, mode)
        if mode == 'live':
            reconcile_ambiguous(client, conn)
        clear_inactive_unregistered(conn)
        created = post_new_targets(client, conn)
        patched = patch_active_flags(client, conn)
        patch_mag_milestones(client, conn)
        if client.is_down:
            print("4MOST looks down (3 consecutive failures): stopped for this run; everything stays pending.")
    patched = pd.DataFrame(patched)
    updated = patched[patched['active']] if len(patched) else patched
    deactivated = patched[~patched['active']] if len(patched) else patched
    return pd.DataFrame(created), updated, deactivated
