"""
provenance.py

Two append-only tables that let us reconstruct, at the end of the survey, what TiDES selected,
when, with which code, and what 4MOST was told:

  tides_events     one row per thing that happened to a TiDES object
                   (created, alert, selected, activated, deactivated, manual_submission, ...)
  tides_4most_log  one row per request to the 4MOST API (payload, status code, response)

Both carry git_sha (which code) and flow_run_id (which Prefect run).
Rows are written in the SAME transaction as the change they describe, so they cannot disagree.
The selection-function definitions themselves live in the human-maintained `pipelines` table.
"""

import functools
import json
import os
import subprocess

import pandas as pd
from sqlalchemy import text


@functools.lru_cache(maxsize=1)
def git_sha():
    """GIT_SHA from the environment (set at deploy time), else `git rev-parse`, else 'unknown'."""
    sha = os.getenv('GIT_SHA')
    if sha:
        return sha
    try:
        out = subprocess.run(['git', 'rev-parse', '--short=12', 'HEAD'], capture_output=True, text=True,
                             timeout=5, cwd=os.path.dirname(os.path.abspath(__file__)))
        return out.stdout.strip() or 'unknown'
    except Exception:
        return 'unknown'


def flow_run_id():
    """The current Prefect flow run id, or None when not running inside a flow."""
    try:
        from prefect.runtime import flow_run
        return str(flow_run.id) if flow_run.id else None
    except Exception:
        return None


EVENT_SQL = text("""
    INSERT INTO tides_events (tides_id, event_type, reason, survey_id, survey_object_id,
                              pipeline_id, submitted_by, details, git_sha, flow_run_id)
    VALUES (:tides_id, :event_type, :reason, :survey_id, :survey_object_id,
            :pipeline_id, :submitted_by, CAST(:details AS jsonb), :git_sha, :flow_run_id)
""")


def log_events(conn, rows, event_type, **fixed):
    """
    Insert one tides_events row per element of ``rows`` (a DataFrame or list of dicts).
    Columns that exist in the rows (tides_id, reason, survey_id, survey_object_id, pipeline_id,
    submitted_by, details) are used; ``fixed`` sets the same value on every row.
    """
    if isinstance(rows, pd.DataFrame):
        rows = rows.to_dict('records')
    if not rows:
        return
    params = []
    for r in rows:
        r = {**r, **fixed}
        details = r.get('details')
        params.append({
            'tides_id': int(r['tides_id']),
            'event_type': event_type,
            'reason': r.get('reason'),
            'survey_id': None if pd.isna(r.get('survey_id')) else int(r['survey_id']),
            'survey_object_id': None if r.get('survey_object_id') is None else str(r['survey_object_id']),
            'pipeline_id': None if pd.isna(r.get('pipeline_id')) else int(r['pipeline_id']),
            'submitted_by': r.get('submitted_by'),
            'details': None if details is None else json.dumps(details, default=str),
            'git_sha': git_sha(),
            'flow_run_id': flow_run_id(),
        })
    conn.execute(EVENT_SQL, params)


LOG_4MOST_SQL = text("""
    INSERT INTO tides_4most_log (tides_ids, method, url, payload, status_code, outcome,
                                 response_text, git_sha, flow_run_id)
    VALUES (:tides_ids, :method, :url, CAST(:payload AS jsonb), :status_code, :outcome,
            :response_text, :git_sha, :flow_run_id)
""")


def log_4most(conn, tides_ids, method, url, payload, status_code, outcome, response_text):
    """One row per 4MOST request (or per dry-run payload)."""
    conn.execute(LOG_4MOST_SQL, {
        'tides_ids': [int(t) for t in tides_ids],
        'method': method,
        'url': url,
        'payload': None if payload is None else json.dumps(payload),
        'status_code': status_code,
        'outcome': outcome,
        'response_text': None if response_text is None else str(response_text)[:4000],
        'git_sha': git_sha(),
        'flow_run_id': flow_run_id(),
    })
