"""
stream_utils.py

Small helpers shared by the survey plugins (tides_lsst.py, tides_ztf.py, ...).

Kafka in one paragraph: a consumer belongs to a "group". Kafka remembers, per group, how far
the group has read (the committed offset). We switch OFF automatic committing and commit only
AFTER the survey's rows are safely committed in our database. If the run dies in between, the
next run simply re-reads the same alerts, and the upsert is written so a re-read changes nothing.

Group ids:
  - production: a FIXED group id from the environment (e.g. LASAIR_LSST_GROUP_ID), so each run
    continues where the last one stopped;
  - development: if the variable is unset we make a random one. A new group starts at the oldest
    retained message, so every dev run re-reads the whole topic (deliberate dev practice).
"""

import json
import os
import time
import uuid

from confluent_kafka import Consumer, KafkaError, KafkaException

# The columns every survey plugin must return (the "tides-ingest contract").
CONTRACT_COLUMNS = ['object_id', 'survey_id', 'pipeline_id', 'ra', 'dec', 'jdmin', 'jdmax',
                    'latest_filter', 'latest_mag', 'n_sources']


def group_id_from_env(env_var, dev_prefix):
    """Fixed group id from ``env_var``; random dev id if unset (refused when TIDES_ENV=production)."""
    group_id = os.getenv(env_var)
    if group_id:
        return group_id
    if os.getenv('TIDES_ENV', 'development').lower() == 'production':
        raise RuntimeError(f"{env_var} must be set in production (a fixed Kafka group id).")
    return f"{dev_prefix}-dev-{uuid.uuid4().hex[:8]}"


def open_consumer(bootstrap_servers, topic, group_id):
    """Same settings as lasair.lasair_consumer, plus enable.auto.commit=False."""
    consumer = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',   # a brand-new group starts at the oldest retained message
        'enable.auto.commit': False,       # we commit ourselves, after the database commit
    })
    consumer.subscribe([topic])
    print(f"Connecting to Kafka topic {topic} with group {group_id}...")
    return consumer


def read_messages(consumer, id_key, max_messages=20000, idle_timeout=10.0, join_timeout=60.0):
    """
    Read JSON alerts until the topic is idle for ``idle_timeout`` seconds or ``max_messages``
    have been read (whatever is left is read by the next run).

    The idle timeout only starts counting once Kafka has assigned us partitions: joining the
    group takes a few seconds (3.1 s measured), and before that an empty poll means nothing.

    Messages that are not valid JSON, or have no ``id_key``, are printed and skipped (they will
    never become valid, so they must not block the stream). Kafka errors raise.

    Returns (list_of_dicts, n_skipped).
    """
    records, n_skipped = [], 0
    started = time.monotonic()
    while len(records) < max_messages:
        assigned = len(consumer.assignment()) > 0
        msg = consumer.poll(idle_timeout if assigned else 1.0)
        if msg is None:
            if assigned:
                break                                   # caught up
            if time.monotonic() - started > join_timeout:
                raise TimeoutError(f"No Kafka partition assignment after {join_timeout} s")
            continue                                    # still joining the group
        if msg.error():
            raise KafkaException(msg.error())
        try:
            record = json.loads(msg.value())
        except (TypeError, ValueError) as e:
            n_skipped += 1
            print(f"Skipping unreadable message at {msg.topic()}[{msg.partition()}]@{msg.offset()}: {e}")
            continue
        if not isinstance(record, dict) or record.get(id_key) is None:
            n_skipped += 1
            print(f"Skipping message without '{id_key}' at {msg.topic()}[{msg.partition()}]@{msg.offset()}")
            continue
        records.append(record)
    if len(records) >= max_messages:
        print(f"Stopped after max_messages={max_messages}; the rest will be read next run.")
    print(f"Read {len(records)} messages ({n_skipped} skipped).")
    return records, n_skipped


def commit_offsets(consumer):
    """Tell Kafka we are done with everything read so far. Call ONLY after the DB commit."""
    if consumer is None:
        return
    try:
        consumer.commit(asynchronous=False)
    except KafkaException as e:
        if e.args and e.args[0].code() == KafkaError._NO_OFFSET:
            return                                      # nothing was read, nothing to commit
        raise


def close_consumer(consumer):
    """Leave the group cleanly. Safe to call with None or twice."""
    if consumer is not None:
        try:
            consumer.close()
        except RuntimeError:
            pass                                        # already closed
