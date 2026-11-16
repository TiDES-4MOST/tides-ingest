# deploy/: running tides-ingest in production

Everything needed to run the TiDES flows unattended on one Linux VM (the "Prefect host"), talking to the TiDES PostgreSQL on another VM. It all comes back by itself after a reboot. Host names and IPs are placeholders: this repository is public, so keep the real values and secrets in `/etc/tides/` and in your password manager.

```
Prefect host                                                      DB host
├─ Docker: tides-prefect (compose)  prefect-server/               └─ PostgreSQL 17 + q3c
│    server (prefecthq/prefect:3.6.11) + its Postgres                  database spo_4most_tides
│    data on the data volume (/srv/tides/prefect-postgres)            logins: spo_4most_ingest (tides_ingest),
├─ systemd: spo_4most-worker        worker/                                   tides_backup (tides_readonly)
│    prefect worker --pool spo_4most (type docker)
│    → one container per flow run: image spo_4most_ingest:<git sha>
├─ systemd: spo_4most-flow-drain    worker/   stops flow containers cleanly at shutdown
├─ systemd timer: tides-db-backup   backup/   nightly pg_dump of both databases
└─ /etc/tides/  spo_4most.env (the flows' .env), worker.env, deploy.env, backup.env, backup.pgpass
```

| File | Purpose |
|---|---|
| `prefect-server/docker-compose.yml`, `.env.example` | Prefect server + its database. `restart: unless-stopped`; API auth string; ports on localhost and the private IP only; the DB refuses to start if the data volume is missing (`create_host_path: false`) |
| `worker/spo_4most-worker.service`, `worker.env.example` | the worker; waits for the API after boot; `Restart=always` |
| `worker/spo_4most-flow-drain.service` | shutdown hook: SIGTERM to running flow containers, so they report Crashed and release the DB lock |
| `deploy.sh`, `deploy.env.example` | the only way to deploy: checks the settings, refuses uncommitted code, runs `prefect deploy --all` (`../prefect.yaml`) |
| `automations/create_automations.py` | alerts to a Slack/Mattermost webhook, plus marking dead runs Crashed (idempotent) |
| `backup/tides-db-backup.{sh,service,timer}`, `backup.env.example` | nightly verified dumps with retention, an off-site hook, and an alert on failure |
| `docker/daemon.json` | Docker log rotation |
| `docker/after-data-volume.conf` | systemd drop-in: Docker starts after the data volume is mounted |
| `fstab.example` | data volume by UUID with `nofail` |
| `os/apt-timeout.conf` | systemd drop-in for `apt-daily` and `apt-daily-upgrade` on both VMs: a hung apt run is killed after 2 h instead of blocking security updates |

## Retiring the old set-up (once, on the current Prefect VM)

The VM still runs the January/OPR4 stack, which holds port 4200 and has the data volume mounted at the
wrong path (`/mnt/mnt/postgres_tides_ingest_data`, while its Prefect database sits on the root disk).
Nothing below deletes data.

1. Optional, keep the old run history:
   `docker exec tides-orchestrate_postgres_1 sh -c 'pg_dump -Fc -U "$POSTGRES_USER" prefect' > ~/prefect-opr4-$(date +%F).dump`
2. Stop the old stacks (they were started with Compose v1, hence `docker-compose`):
   `cd ~/tides-orchestrate && docker-compose down`, then `cd ~/tides-test-worker-pool && docker-compose down`.
3. Free the data volume: `ls -la /mnt/mnt/postgres_tides_ingest_data` (look before reusing it: it holds
   about 90 MB), then `sudo umount /mnt/mnt/postgres_tides_ingest_data` and delete that line from `/etc/fstab`.
4. Remove the old run containers: `docker container prune` (414 exited ones, all from the OPR4 deployment).

Then follow the set-up below from step 1.

## Set-up, in order (first time, or rebuilding the host)

1. **Data volume.** Mount it at `/srv/tides` (`fstab.example`). Create `prefect-postgres/` and `backups/` (700).
2. **OS updates (both VMs).** Install `os/apt-timeout.conf` as `/etc/systemd/system/apt-daily.service.d/timeout.conf`
   and as `/etc/systemd/system/apt-daily-upgrade.service.d/timeout.conf`, then `systemctl daemon-reload`.
3. **Docker.** Compose v2 (`apt install docker-compose-v2`); copy `docker/daemon.json` to `/etc/docker/daemon.json`;
   copy `docker/after-data-volume.conf` to `/etc/systemd/system/docker.service.d/` (so Docker waits for the
   data volume at boot); `systemctl daemon-reload && systemctl restart docker`.
4. **Code and venv.**
   - `git clone … /opt/tides/tides-ingest` (a clean clone, only ever on a pushed commit);
   - `python3 -m venv /opt/tides/venv && /opt/tides/venv/bin/pip install prefect==3.6.11 prefect-docker==0.7.1 importlib-metadata==8.7.1`
     (Prefect 3.6.11 imports `importlib_metadata` without declaring it on Python 3.12; 8.7.1 is the version in the
     `prefecthq/prefect:3.6.11-python3.12` image). Check with `/opt/tides/venv/bin/prefect version`, then record
     the exact set: `/opt/tides/venv/bin/pip freeze > /opt/tides/venv-requirements.lock`;
   - `mkdir /opt/tides/prefect-home`;
   - `sudo install -d -m 750 -o root -g docker /etc/tides`.
5. **Server.**
   ```bash
   cd prefect-server && cp .env.example .env && chmod 600 .env
   # fill in .env, then:
   docker compose up -d
   curl -fsS http://127.0.0.1:4200/api/health
   ```
   Expect `true`; an API call without auth gives 401.
6. **Worker.**
   1. Copy `worker/worker.env.example` to `/etc/tides/worker.env` (640 root:docker) and set the same auth string.
   2. `set -a; . /etc/tides/worker.env; set +a; /opt/tides/venv/bin/prefect work-pool create spo_4most --type docker`.
   3. Copy both `.service` files to `/etc/systemd/system/`, then `systemctl enable --now spo_4most-flow-drain spo_4most-worker`.
7. **Database** (as the admin, from `../database_setup/`):
   1. `CREATE DATABASE spo_4most_tides`;
   2. `setupDB.sh` (it also creates q3c);
   3. `migrate.sh`;
   4. `psql -f roles.sql`;
   5. create the logins (instructions at the top of `roles.sql`).
8. **Flow settings.** Write `/etc/tides/spo_4most.env` (640 root:docker; template `../.env.example`): `TIDES_ENV=production`, `FOURMOST_MODE=dry_run` first, the `tides_ingest` login, and a fixed `LASAIR_LSST_GROUP_ID`.
9. **Deploy.** `echo TIDES_ENV_FILE=/etc/tides/spo_4most.env > /etc/tides/deploy.env` (640 root:docker), then `cd /opt/tides/tides-ingest && deploy/deploy.sh`. Trigger a first run with `prefect deployment run 'tides-ingest/spo_4most-ingest'`.
10. **Alerts.** `TIDES_ALERT_WEBHOOK_URL=… python automations/create_automations.py` (with `worker.env` loaded).
11. **Backups.**
    1. Copy `backup/tides-db-backup.sh` to `/usr/local/sbin/`; copy the unit and timer to `/etc/systemd/system/`.
    2. Write `/etc/tides/backup.env` and `/etc/tides/backup.pgpass` (600).
    3. `systemctl enable --now tides-db-backup.timer`, then `systemctl start tides-db-backup` once.
12. **Reboot drill.** Reboot, then check:
    - the volume is mounted;
    - `docker compose ps` is healthy;
    - the worker is active and the pool READY;
    - the next run is Completed.
13. **Go live.** After a night in `dry_run`, set `FOURMOST_MODE=live` in `/etc/tides/spo_4most.env`. It takes effect at the next run.

## Day to day

| Task | How |
|---|---|
| Deploy new code | `cd /opt/tides/tides-ingest && git pull && deploy/deploy.sh` (after `database_setup/migrate.sh` if there is a new migration) |
| Roll back | `git checkout <old sha> && deploy/deploy.sh` |
| Change a setting | edit `/etc/tides/spo_4most.env`; the next run uses it |
| Pause everything (maintenance) | `prefect deployment schedule pause --all` … `resume --all` |
| Worker logs | `journalctl -u spo_4most-worker -f` |
| Server logs | `cd prefect-server && docker compose logs -f server` |
| Inventory / health of a VM | `sudo bash ../deploy-facts/collect.sh` |

## What happens on a reboot or crash

| Event | Result |
|---|---|
| Clean reboot | The drain hook stops flow containers (each run reports Crashed and releases the lock). Then Docker brings back the server and its DB, and systemd starts the worker once the API answers. Missed runs start once; all but one do nothing. |
| Power cut mid-run | The run stays Running, and the zombie automation marks it Crashed after 15 min without heartbeats (Pending: 30 min). The DB lock and the transaction are released by PostgreSQL; the Kafka offsets were not committed, so the next run re-reads those alerts. |
| Worker restart | Running flows finish normally. |
| DB VM still booting | Each run waits up to 10 min for the database. |
| Data volume missing | The VM boots (`nofail`), and the Prefect DB container refuses to start. Re-attach the volume, then `mount /srv/tides && docker compose up -d`. |

Why there is no Prefect deployment `concurrency_limit`: the PostgreSQL advisory run lock (`../tides_flows/tides_db.py`) prevents overlapping runs and is freed when a session dies. Prefect 3.6's limit kept its lease in server memory: a server restart killed the running flow, and an unclean restart could leave the slot taken, cancelling every later run.
