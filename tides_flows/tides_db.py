"""
tides_db.py

Database plumbing shared by the TiDES flows (tides_controller.py, db_4MOST_tasks/sync_ostd.py):

  make_engine()       SQLAlchemy engine from TIDES_DB_* with timeouts and TCP keepalives, so a dead
                      network or a rebooting database VM makes a run fail quickly instead of hanging
  wait_for_db()       at the start of a run, wait (up to 10 min) for the database, so a run that
                      starts while the database VM is still booting does not fail
  take_run_lock()     the TiDES run lock (a PostgreSQL advisory lock). Only one ingest or OSTD sync
                      runs at a time. If the lock is busy, report who holds it and for how long, and
                      raise if the holder looks stuck (so the run is marked Failed and someone is alerted)
"""

import os
import time

import sqlalchemy

# pg_try_advisory_lock key, so two runs never overlap: the bytes of "tiDES".
# Ingest and sync_ostd use the SAME key.
TIDES_RUN_LOCK_KEY = 0x7469444553
# A run normally takes minutes and the ingest flow times out after 60 min. A holder older than this
# is a stuck session (e.g. a half-open connection after a VM crash), which needs a human.
LOCK_STALE_MINUTES = 90


def make_engine(application_name):
    """Engine from the TIDES_DB_* environment variables (call load_dotenv() first)."""
    url = sqlalchemy.engine.URL.create(
        'postgresql+psycopg2',
        username=os.getenv('TIDES_DB_USER'),
        password=os.getenv('TIDES_DB_PASS') or None,   # URL.create quotes special characters
        host=os.getenv('TIDES_DB_HOST') or 'localhost',
        port=int(os.getenv('TIDES_DB_PORT') or 5432),
        database=os.getenv('TIDES_DB_DATABASE'),
    )
    return sqlalchemy.create_engine(
        url, future=True,
        pool_pre_ping=True,                    # never hand out a connection that died (e.g. DB restart)
        connect_args={
            'connect_timeout': 30,             # seconds to establish a connection
            'keepalives': 1,                   # notice a dead peer within ~2 min, not ~2 h
            'keepalives_idle': 60,
            'keepalives_interval': 10,
            'keepalives_count': 6,
            'application_name': application_name,   # shows up in pg_stat_activity / the lock report
        })


def wait_for_db(engine, max_wait_seconds=600, every_seconds=30):
    """Return once the database answers. Raise after max_wait_seconds (the run then fails)."""
    deadline = time.monotonic() + max_wait_seconds
    while True:
        try:
            with engine.connect() as conn:
                conn.execute(sqlalchemy.text("SELECT 1"))
            return
        except sqlalchemy.exc.OperationalError as e:
            if time.monotonic() >= deadline:
                raise
            print(f"Database not reachable yet ({str(e).splitlines()[0]}); retrying in {every_seconds} s...")
            time.sleep(every_seconds)


LOCK_HOLDER_SQL = sqlalchemy.text("""
    SELECT a.pid, a.application_name, a.client_addr::text AS client_addr,
           round(EXTRACT(EPOCH FROM now() - a.backend_start) / 60.0) AS minutes
    FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
    WHERE l.locktype = 'advisory' AND l.granted AND l.objsubid = 1
      AND l.classid = CAST(:hi AS oid) AND l.objid = CAST(:lo AS oid)
""")


def take_run_lock(lock_conn, run_name):
    """
    Try to take the run lock on ``lock_conn`` (a connection kept open for the whole run).
    Returns True if we hold it. Returns False if a healthy run holds it (this run should do
    nothing). Raises RuntimeError if the holder has held it for more than LOCK_STALE_MINUTES.
    The lock belongs to the session: it is released by pg_advisory_unlock, or by PostgreSQL as
    soon as the session ends (including when the process or the VM dies).
    """
    got = lock_conn.execute(sqlalchemy.text("SELECT pg_try_advisory_lock(:k)"), {'k': TIDES_RUN_LOCK_KEY}).scalar()
    lock_conn.commit()     # the lock belongs to the session, not the transaction: keep it, end the transaction
    if got:
        return True
    holder = lock_conn.execute(LOCK_HOLDER_SQL, {'hi': (TIDES_RUN_LOCK_KEY >> 32) & 0xFFFFFFFF,
                                                 'lo': TIDES_RUN_LOCK_KEY & 0xFFFFFFFF}).mappings().first()
    lock_conn.commit()
    who = (f"pid {holder['pid']} ({holder['application_name'] or 'no application_name'}, "
           f"from {holder['client_addr'] or 'local'}, session {holder['minutes']} min old)") if holder else "an unknown session"
    if holder and holder['minutes'] is not None and holder['minutes'] > LOCK_STALE_MINUTES:
        raise RuntimeError(
            f"{run_name}: the TiDES run lock has been held by {who} for more than {LOCK_STALE_MINUTES} min. "
            "That session is probably stuck. Check it in pg_stat_activity and, if it is dead, end it with "
            "SELECT pg_terminate_backend(<pid>); (see the operations runbook).")
    print(f"{run_name}: another TiDES run holds the run lock ({who}): doing nothing this time.")
    return False


def release_run_lock(lock_conn):
    lock_conn.execute(sqlalchemy.text("SELECT pg_advisory_unlock(:k)"), {'k': TIDES_RUN_LOCK_KEY})
    lock_conn.commit()
