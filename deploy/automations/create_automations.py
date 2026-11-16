#!/usr/bin/env python
"""
create_automations.py: the alerting for the TiDES Prefect deployment.

Creates (or replaces: it is idempotent) one webhook block and six automations, all named "tides: ...":

  tides: zombie flow runs        a Running flow run sent no heartbeat for 15 min (its process or VM died)
                                 -> mark it Crashed (which then triggers "run failed")
  tides: stuck pending runs      a flow run stayed Pending for 30 min (died while starting) -> Crashed
  tides: run failed              any flow run ends Failed / Crashed / TimedOut      -> webhook
  tides: no completed ingest     no Completed run of tides-ingest/spo_4most-ingest for 3 h -> webhook
  tides: no completed ostd sync  no Completed run of tides-ostd-sync/spo_4most-ostd-sync for 26 h -> webhook
  tides: work pool not ready     the spo_4most pool has no live worker               -> webhook

The webhook is a Slack or Mattermost *incoming webhook* (both accept {"text": ...}). Its URL is read
from the environment, never from the command line (shell history):

    sudo cat /etc/tides/alert-webhook.url          # one line: https://...
    set -a; . /etc/tides/worker.env; set +a       # PREFECT_API_URL + PREFECT_API_AUTH_STRING
    TIDES_ALERT_WEBHOOK_URL=$(sudo cat /etc/tides/alert-webhook.url) \
        /opt/tides/venv/bin/python deploy/automations/create_automations.py
    ... --dry-run    show what would be created, change nothing

Run it after deploy/deploy.sh (the deployments and the work pool must exist), and again whenever this
file changes. Note: the alerts live on the Prefect server. If the whole Prefect VM is down, nothing
here can fire: see the "dead man's switch" in the operations runbook.
"""

import argparse
import os
import sys
from datetime import timedelta

from prefect.automations import Automation, EventTrigger, Posture
from prefect.blocks.notifications import CustomWebhookNotificationBlock
from prefect.client.orchestration import get_client
from prefect.client.schemas.objects import StateType
from prefect.events.actions import ChangeFlowRunState, SendNotification

BLOCK_NAME = "tides-alerts"
POOL = "spo_4most"
INGEST = "tides-ingest/spo_4most-ingest"
OSTD = "tides-ostd-sync/spo_4most-ostd-sync"
FINAL_STATES = ["prefect.flow-run.Completed", "prefect.flow-run.Failed", "prefect.flow-run.Cancelled",
                "prefect.flow-run.Crashed", "prefect.flow-run.TimedOut"]


def notify(block_id, subject, body):
    return SendNotification(block_document_id=block_id, subject=subject, body=body)


def build(block_id, ingest_id, ostd_id):
    run_body = ("Flow: {{ flow.name }}   run: {{ flow_run.name }}\n"
                "State: {{ flow_run.state.name }}: {{ flow_run.state.message }}\n"
                "{{ flow_run|ui_url }}")
    autos = [
        Automation(
            name="tides: zombie flow runs",
            description="Mark Running flow runs without a heartbeat for 15 min as Crashed "
                        "(needs PREFECT_RUNNER_HEARTBEAT_FREQUENCY, set in prefect.yaml).",
            trigger=EventTrigger(
                after={"prefect.flow-run.heartbeat"},
                expect={"prefect.flow-run.heartbeat", *FINAL_STATES},
                match={"prefect.resource.id": ["prefect.flow-run.*"]},
                for_each={"prefect.resource.id"},
                posture=Posture.Proactive, threshold=1, within=timedelta(minutes=15)),
            actions=[ChangeFlowRunState(
                state=StateType.CRASHED,
                message="No heartbeat for 15 min: the flow-run process or its VM died (marked by automation).")],
        ),
        Automation(
            name="tides: stuck pending runs",
            description="Mark flow runs that stay Pending for 30 min as Crashed (e.g. the VM went down "
                        "while a run was starting: no heartbeat is ever sent for those).",
            trigger=EventTrigger(
                after={"prefect.flow-run.Pending"},
                expect={"prefect.flow-run.Running", *FINAL_STATES},
                match={"prefect.resource.id": ["prefect.flow-run.*"]},
                for_each={"prefect.resource.id"},
                posture=Posture.Proactive, threshold=1, within=timedelta(minutes=30)),
            actions=[ChangeFlowRunState(
                state=StateType.CRASHED,
                message="Pending for 30 min without starting: its container or VM died (marked by automation).")],
        ),
        Automation(
            name="tides: run failed",
            description="Any flow run that ends Failed, Crashed or TimedOut.",
            trigger=EventTrigger(
                expect={"prefect.flow-run.Failed", "prefect.flow-run.Crashed", "prefect.flow-run.TimedOut"},
                match={"prefect.resource.id": ["prefect.flow-run.*"]},
                posture=Posture.Reactive, threshold=1, within=timedelta(0)),
            actions=[notify(block_id, "TiDES: {{ flow.name }} run {{ flow_run.state.name }}", run_body)],
        ),
        Automation(
            name="tides: no completed ingest",
            description=f"No Completed run of {INGEST} for 3 hours (worker, server, DB or lock problem).",
            trigger=EventTrigger(
                expect={"prefect.flow-run.Completed"},
                match_related={"prefect.resource.id": f"prefect.deployment.{ingest_id}"},
                posture=Posture.Proactive, threshold=1, within=timedelta(hours=3)),
            actions=[notify(block_id, "TiDES: no completed ingest run for 3 hours",
                            f"No run of {INGEST} has completed in the last 3 hours. "
                            "Check the worker (journalctl -u spo_4most-worker), the Prefect UI and the "
                            "operations runbook.")],
        ),
        Automation(
            name="tides: no completed ostd sync",
            description=f"No Completed run of {OSTD} for 26 hours.",
            trigger=EventTrigger(
                expect={"prefect.flow-run.Completed"},
                match_related={"prefect.resource.id": f"prefect.deployment.{ostd_id}"},
                posture=Posture.Proactive, threshold=1, within=timedelta(hours=26)),
            actions=[notify(block_id, "TiDES: no completed OSTD sync for 26 hours",
                            f"No run of {OSTD} has completed in the last 26 hours.")],
        ),
        Automation(
            name="tides: work pool not ready",
            description=f"The {POOL} work pool has no live worker.",
            trigger=EventTrigger(
                expect={"prefect.work-pool.not-ready"},
                match={"prefect.resource.name": POOL},
                posture=Posture.Reactive, threshold=1, within=timedelta(0)),
            actions=[notify(block_id, f"TiDES: work pool {POOL} has no worker",
                            "No worker is polling the pool: no flow will run. "
                            "Check: systemctl status spo_4most-worker; journalctl -u spo_4most-worker.")],
        ),
    ]
    return autos


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--dry-run", action="store_true", help="print what would be created; change nothing")
    args = ap.parse_args()

    if args.dry_run:
        from uuid import UUID
        fake = UUID(int=0)
        for a in build(fake, fake, fake):
            print(a.model_dump_json(indent=1, exclude={"id"}))
        print("dry run: nothing created")
        return

    url = os.getenv("TIDES_ALERT_WEBHOOK_URL")
    if not url:
        sys.exit("Set TIDES_ALERT_WEBHOOK_URL (the Slack/Mattermost incoming-webhook URL) in the environment.")

    with get_client(sync_client=True) as client:
        ingest_id = client.read_deployment_by_name(INGEST).id
        ostd_id = client.read_deployment_by_name(OSTD).id
        client.read_work_pool(POOL)        # fails clearly if the pool does not exist yet

    block_id = CustomWebhookNotificationBlock(
        name=BLOCK_NAME, url=url,
        json_data={"text": "*{{subject}}*\n{{body}}"},   # Slack and Mattermost incoming webhooks
    ).save(BLOCK_NAME, overwrite=True)
    print(f"webhook block '{BLOCK_NAME}': {block_id}")

    for auto in build(block_id, ingest_id, ostd_id):
        try:
            Automation.read(name=auto.name).delete()
            print(f"replaced: {auto.name}")
        except ValueError:
            pass                            # not there yet
        created = auto.create()
        print(f"created:  {created.name} ({created.id})")
    print("Test the channel: Prefect UI -> Blocks -> tides-alerts, or run a flow that fails.")


if __name__ == "__main__":
    main()
