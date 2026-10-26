#!/usr/bin/env bash
# collect.sh: read-only fact gathering for the TiDES ingest deployment (Somerville OpenStack).
#
# Run it on the Prefect worker VM and on the database VM:
#     sudo bash collect.sh                         # auto-detects the Prefect worker and/or Postgres
#     sudo bash collect.sh --role db --db tides    # force a role / limit Postgres checks to one database
#     sudo bash collect.sh --repo /path/to/tides-ingest --prefect-api https://host/api
# It writes ./collect-<host>-<UTC time>.tar.gz. Read 00_FLAGS.txt, look over the lines it prints at
# the end (possible secrets), then copy the tarball into deploy-facts/ in the repository.
#
# What it does NOT do:
#   - change anything: no installs, no restarts, no writes (psql runs with default_transaction_read_only);
#   - contact 4MOST in any way (not even a DNS lookup);
#   - consume from Kafka (it only opens a TCP socket to the broker port and closes it);
#   - print secrets: .env values are shown only for non-secret keys, and all output passes through
#     a redaction filter. Hostnames and IP addresses ARE included (needed to check boot dependencies).
# It also runs without sudo, but then sees less (other users' processes, docker, journal, Postgres).

ROLE=auto; DB_ONLY=""; REPO_DIR=""; API_ARG=""
while [ $# -gt 0 ]; do
    case "$1" in
        --role) ROLE="$2"; shift 2 ;;
        --db) DB_ONLY="$2"; shift 2 ;;
        --repo) REPO_DIR="$2"; shift 2 ;;
        --prefect-api) API_ARG="$2"; shift 2 ;;
        -h|--help) sed -n '2,18p' "$0"; exit 0 ;;
        *) echo "unknown argument: $1 (see --help)" >&2; exit 2 ;;
    esac
done

umask 077
export LC_ALL=C
HOST=$(hostname -s 2>/dev/null || hostname)
OUT="$PWD/collect-$HOST-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUT" || exit 1
FLAGS="$OUT/.flags"; : > "$FLAGS"
REDACT="$OUT/.redact.sed"
IS_ROOT=0; [ "$(id -u)" -eq 0 ] && IS_ROOT=1
T=20   # default per-command timeout (s)

have() { command -v "$1" >/dev/null 2>&1; }
flag() { echo "$1: $2" >> "$FLAGS"; }   # FLAG = likely problem, INFO = check it is intended

# ---------------------------------------------------------------------------------------------
# Redaction: applied to every output file
# ---------------------------------------------------------------------------------------------
cat > "$REDACT" <<'SED'
# credentials embedded in URLs:  scheme://user:pass@host
s#([a-z][a-z0-9+.-]*://)[^/@[:space:]"']+@#\1<REDACTED>@#gI
# Authorization headers and bearer tokens
s#(authorization["']?[[:space:]]*[:=][[:space:]]*).*#\1<REDACTED>#gI
s#(bearer[[:space:]]+)[A-Za-z0-9._~+/=-]+#\1<REDACTED>#gI
# NAME=value / "name": "value" / name: value, for secret-looking names
s#((pass(word|wd)?|pwd|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|credentials?)[A-Za-z0-9_.-]*["']?[[:space:]]*[:=][[:space:]]*["']?)[^"'[:space:],}]+#\1<REDACTED>#gI
# Prefect Cloud API keys, JWTs
s#pnu_[A-Za-z0-9]{16,}#pnu_<REDACTED>#g
s#eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]+#<REDACTED-JWT>#g
SED
if ! sed --version 2>/dev/null | grep -q GNU; then
    echo "This script needs GNU sed (for case-insensitive redaction). Aborting." >&2; exit 1
fi

# ---------------------------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------------------------
run() {     # run CMD ARGS... (with a timeout)
    echo; echo "### \$ $*"
    if ! have "$1"; then echo "(not installed: $1)"; return 0; fi
    timeout "$T" "$@" 2>&1; local rc=$?
    [ $rc -ne 0 ] && echo "(exit $rc)"
    return 0
}
sh_() {     # run a shell snippet (with a timeout; optional 2nd arg = timeout)
    echo; echo "### \$ $1"
    timeout "${2:-$T}" bash -c "$1" 2>&1; local rc=$?
    [ $rc -ne 0 ] && echo "(exit $rc)"
    return 0
}
show() {    # show FILE... without comment/blank lines
    local f
    for f in "$@"; do
        [ -e "$f" ] || continue
        echo; echo "### file: $f  ($(stat -c '%U:%G mode=%a modified=%y' "$f" 2>/dev/null))"
        if [ -r "$f" ]; then sed -e 's/[[:space:]]*$//' "$f" | grep -v -E '^[[:space:]]*(#|;|//|$)'
        else echo "(not readable)"; fi
    done
}
section() { # section NAME FUNCTION: run FUNCTION, redact, write NAME.txt
    local name="$1"; shift
    echo "  - $name" >&2
    { echo "# $name   host=$HOST   $(date -u '+%F %T') UTC   root=$IS_ROOT"; "$@"; } 2>&1 \
        | sed -E -f "$REDACT" > "$OUT/$name.txt"
}

# ---------------------------------------------------------------------------------------------
# Detection (runs in the main shell so later sections can use the results)
# ---------------------------------------------------------------------------------------------
p_user() { ps -o user= -p "$1" 2>/dev/null | tr -d ' '; }
p_cg()   { grep -m1 -E '^0::|name=systemd' "/proc/$1/cgroup" 2>/dev/null | cut -d: -f3-; }
p_cmd()  { tr '\0' ' ' < "/proc/$1/cmdline" 2>/dev/null; }

WORKER_PIDS=$(pgrep -f 'prefect worker start' 2>/dev/null | tr '\n' ' ')
SERVER_PIDS=$(pgrep -f 'prefect server start|prefect\.server\.api' 2>/dev/null | tr '\n' ' ')
PG_RUNNING=0; { pgrep -x postgres || pgrep -x postmaster; } >/dev/null 2>&1 && PG_RUNNING=1

IS_WORKER=0; IS_DB=0
case "$ROLE" in
    worker) IS_WORKER=1 ;;
    db) IS_DB=1 ;;
    both) IS_WORKER=1; IS_DB=1 ;;
    auto)
        { [ -n "$WORKER_PIDS$SERVER_PIDS" ] || have docker || have prefect; } && IS_WORKER=1
        { [ $PG_RUNNING -eq 1 ] || have pg_lsclusters; } && IS_DB=1 ;;
    *) echo "--role must be worker, db, both or auto" >&2; exit 2 ;;
esac

# The (first) worker process: who runs it, under what, with which Python
W=""; WUSER=""; WCG=""; WKIND=""; PY=""; PREFECT_BIN=""; WCONTAINER=""
for p in $WORKER_PIDS; do W=$p; break; done
if [ -n "$W" ]; then
    WUSER=$(p_user "$W"); WCG=$(p_cg "$W")
    case "$WCG" in
        *docker*|*containerd*|*libpod*) WKIND=container
            WCONTAINER=$(basename "$WCG" | sed -E 's/^docker-//; s/\.scope$//') ;;
        */system.slice/cron.service|*/system.slice/crond.service) WKIND=cron ;;
        */system.slice/*.service) WKIND=system-service ;;
        */user@*.service/*) WKIND=user-service ;;
        *) WKIND=login-session ;;     # tmux / screen / nohup / a terminal
    esac
    if [ "$WKIND" != container ]; then
        # argv, not /proc/PID/exe: a venv's python resolves to the system python, which lacks prefect
        mapfile -d '' -t WARGV < "/proc/$W/cmdline" 2>/dev/null
        for a in "${WARGV[@]}"; do case "$a" in */prefect) PREFECT_BIN="$a" ;; esac; done
        case "${WARGV[0]}" in /*python*) PY="${WARGV[0]}" ;; esac
        [ -z "$PY" ] && [ -n "$PREFECT_BIN" ] && [ -x "$(dirname "$PREFECT_BIN")/python" ] && PY="$(dirname "$PREFECT_BIN")/python"
    fi
fi
WUNIT=""; [ "$WKIND" = system-service ] && WUNIT=$(basename "$WCG")
# All prefect executables on the box (conda envs, venvs, pip --user)
PREFECT_BINS=$( { command -v prefect; ls -1 /home/*/{miniconda3,anaconda3,miniforge3,mambaforge}/envs/*/bin/prefect \
    /home/*/{miniconda3,anaconda3,miniforge3,mambaforge}/bin/prefect /home/*/.local/bin/prefect \
    /home/*/*/bin/prefect /opt/*/bin/prefect /opt/*/envs/*/bin/prefect /root/*/envs/*/bin/prefect; } 2>/dev/null | sort -u)
if [ -z "$PREFECT_BIN" ] && [ "$WKIND" != container ]; then
    PREFECT_BIN=$(echo "$PREFECT_BINS" | head -1)
    [ -n "$PREFECT_BIN" ] && [ -x "$(dirname "$PREFECT_BIN")/python" ] && PY="$(dirname "$PREFECT_BIN")/python"
fi

# PREFECT_* settings the worker runs with (API URL/key), so we query the same server it uses
PENV=()
[ -n "$W" ] && mapfile -t PENV < <(tr '\0' '\n' < "/proc/$W/environ" 2>/dev/null | grep -E '^PREFECT_')
[ -n "$API_ARG" ] && PENV+=("PREFECT_API_URL=$API_ARG")
API_URL=""
for kv in "${PENV[@]}"; do case "$kv" in PREFECT_API_URL=*) API_URL="${kv#PREFECT_API_URL=}" ;; esac; done
WHOME=""; [ -n "$WUSER" ] && WHOME=$(getent passwd "$WUSER" | cut -d: -f6)
if [ -z "$API_URL" ] && [ -n "$WHOME" ] && [ -r "$WHOME/.prefect/profiles.toml" ]; then
    API_URL=$(grep -m1 -E '^\s*PREFECT_API_URL' "$WHOME/.prefect/profiles.toml" | cut -d= -f2- | tr -d " \"'")
fi

as_worker() {   # run CMD... as the worker's user, with the worker's PREFECT_* environment
    if [ -n "$WUSER" ] && [ "$WUSER" != "$(id -un)" ]; then
        [ $IS_ROOT -eq 1 ] || { echo "(skipped: the worker runs as $WUSER; re-run with sudo)"; return 0; }
        # pass the settings in a file only $WUSER can read (not on the command line, where ps shows them)
        local envf kv; envf=$(mktemp /tmp/collect-env.XXXXXX) || return 0
        for kv in "${PENV[@]}"; do printf 'export %s=%q\n' "${kv%%=*}" "${kv#*=}"; done > "$envf"
        chown "$WUSER" "$envf"
        (cd /tmp && timeout 90 sudo -u "$WUSER" -H bash -c '. "$0"; rm -f "$0"; exec "$@"' "$envf" "$@")
        rm -f "$envf"
    else
        (
            for kv in "${PENV[@]}"; do export "$kv"; done
            # no worker found: never let Prefect create state in this user's home
            [ -z "$WUSER" ] && export PREFECT_HOME="$OUT/.prefect_home"
            cd /tmp && exec timeout 90 "$@"
        )
    fi
}

# Read-only Prefect API query (deployments, pools, workers, recent runs)
read -r -d '' PREFECT_SNIPPET <<'PY'
import asyncio, collections, json
from prefect.client.orchestration import get_client

def dump(o):
    try:
        d = o.model_dump(mode="json")
    except Exception:
        d = str(o)
    print(json.dumps(d, indent=1, sort_keys=True, default=str))

async def main():
    async with get_client() as c:
        print("api_url:", c.api_url)
        try:
            print("server version:", await c.api_version())
        except Exception as e:
            print("server version: ERROR", repr(e))
        print("\n=== work pools")
        try:
            for p in await c.read_work_pools():
                dump(p)
                try:
                    for w in await c.read_workers_for_work_pool(p.name):
                        print("  worker:", w.name, "status:", getattr(w, "status", "?"),
                              "last_heartbeat:", w.last_heartbeat_time)
                except Exception as e:
                    print("  workers: ERROR", repr(e))
        except Exception as e:
            print("ERROR", repr(e))
        print("\n=== deployments")
        try:
            names = {}
            for d in await c.read_deployments():
                if d.flow_id not in names:
                    try:
                        names[d.flow_id] = (await c.read_flow(d.flow_id)).name
                    except Exception:
                        names[d.flow_id] = "?"
                print("\n--- deployment %s/%s" % (names[d.flow_id], d.name))
                dump(d)
        except Exception as e:
            print("ERROR", repr(e))
        print("\n=== last 300 flow runs")
        try:
            from prefect.client.schemas.sorting import FlowRunSort
            runs = await c.read_flow_runs(sort=FlowRunSort.EXPECTED_START_TIME_DESC, limit=300)
            print("by state:", dict(collections.Counter(r.state_name for r in runs)))
            for r in runs[:80]:
                print(r.expected_start_time, "| start", r.start_time, "|", r.state_name, "|", r.name,
                      "| dep", r.deployment_id, "| dur", r.total_run_time)
        except Exception as e:
            print("ERROR", repr(e))
        print("\n=== global concurrency limits")
        try:
            for l in await c.read_global_concurrency_limits(limit=50):
                dump(l)
        except Exception as e:
            print("ERROR", repr(e))

asyncio.run(main())
PY

PREFECT_RAW="$OUT/.prefect_raw"; : > "$PREFECT_RAW"
if [ $IS_WORKER -eq 1 ] && [ -n "$PY" ]; then
    if [ -n "$API_URL" ]; then
        echo "Querying the Prefect API at $API_URL (read-only)..." >&2
        as_worker "$PY" -c "$PREFECT_SNIPPET" > "$PREFECT_RAW" 2>&1
    else
        echo "(skipped: no PREFECT_API_URL found; without it Prefect would start a temporary local server." \
             " Pass --prefect-api URL)" > "$PREFECT_RAW"
    fi
fi

# Repository checkouts and .env files
REPOS=$( {
    [ -n "$REPO_DIR" ] && echo "$REPO_DIR"
    for p in $WORKER_PIDS; do d=$(readlink -f "/proc/$p/cwd" 2>/dev/null); [ -f "$d/prefect.yaml" ] && echo "$d"; done
    for top in / /home /opt /srv /data /mnt /root; do
        [ -d "$top" ] && timeout 60 find "$top" -xdev -maxdepth 6 -name prefect.yaml \
            -not -path '*/site-packages/*' -not -path '/proc/*' -printf '%h\n' 2>/dev/null
    done; } | sort -u)
ENV_FILES=$( {
    # host path of the .env that the deployment mounts into the run container
    grep -o -E '[^" ]+:/opt/prefect/tides-ingest/\.env' "$PREFECT_RAW" 2>/dev/null | cut -d: -f1
    for r in $REPOS; do [ -e "$r/.env" ] && readlink -f "$r/.env"; done; } | sort -u)

envget() { grep -E "^[[:space:]]*(export[[:space:]]+)?$2=" "$1" 2>/dev/null | tail -1 | cut -d= -f2- | tr -d "\"' "; }
DB_HOST=""; DB_PORT=""; DB_NAME_ENV=""
for f in $ENV_FILES; do
    h=$(envget "$f" TIDES_DB_HOST)
    if [ -n "$h" ]; then DB_HOST=$h; DB_PORT=$(envget "$f" TIDES_DB_PORT); DB_NAME_ENV=$(envget "$f" TIDES_DB_DATABASE); break; fi
done

# =============================================================================================
# Sections
# =============================================================================================
s_summary() {
    echo "script: deploy-facts/collect.sh   role requested=$ROLE   worker=$IS_WORKER db=$IS_DB"
    echo "kernel: $(uname -r)   os: $(. /etc/os-release 2>/dev/null; echo "$PRETTY_NAME")"
    echo "uptime since: $(uptime -s 2>/dev/null)"
    echo "prefect worker pids: ${WORKER_PIDS:-none}   user=$WUSER   kind=$WKIND   cgroup=$WCG"
    echo "prefect server pids (local): ${SERVER_PIDS:-none}"
    echo "worker python: ${PY:-?}   prefect cli: ${PREFECT_BIN:-?}"
    echo "prefect executables found:"; echo "$PREFECT_BINS" | sed 's/^/  /'
    echo "prefect API url: ${API_URL:-unknown}"
    echo "repo checkouts:"; echo "$REPOS" | sed 's/^/  /'
    echo ".env files:"; echo "$ENV_FILES" | sed 's/^/  /'
    echo "TiDES DB from .env: host=$DB_HOST port=$DB_PORT db=$DB_NAME_ENV"
    echo "postgres running here: $PG_RUNNING"
}

s_os() {
    show /etc/os-release
    run uname -a; run uptime
    run nproc; run free -h; run swapon --show
    sh_ "ps -eo pid,user,%mem,%cpu,etime,cmd --sort=-%mem | head -15 | cut -c1-220"
    run lsblk -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINT,SERIAL
    run df -hT -x tmpfs -x devtmpfs -x overlay -x squashfs
    run df -i -x tmpfs -x devtmpfs -x overlay -x squashfs
    show /etc/fstab
    run findmnt -o TARGET,SOURCE,FSTYPE,OPTIONS -t ext4,xfs,btrfs,nfs,nfs4,cifs,vfat
    sh_ "ls -la /var/run/reboot-required* 2>&1"
    sh_ "apt-config dump 2>/dev/null | grep -i -E 'unattended|periodic'"
    show /etc/dnf/automatic.conf
    sh_ "apt list --upgradable 2>/dev/null | tail -n +2 | wc -l | sed 's/^/upgradable apt packages: /'" 60
    sh_ "apt list --upgradable 2>/dev/null | grep -i -E 'docker|containerd|linux-image|postgres|python3\.' | head -30" 60
    run pro status
    sh_ "grep -h -E '^deb ' /etc/apt/sources.list /etc/apt/sources.list.d/*.list 2>/dev/null; ls /etc/apt/sources.list.d/ 2>/dev/null"
    sh_ "journalctl --since '-60d' _TRANSPORT=kernel --no-pager 2>/dev/null | grep -i -E 'out of memory|oom-kill|killed process|I/O error|EXT4-fs error|blk_update_request' | tail -30" 60
}

s_boot() {
    run systemctl get-default
    run systemctl --failed --no-pager
    run systemctl list-unit-files --state=enabled --no-pager
    run systemctl list-timers --all --no-pager
    run systemd-analyze
    sh_ "systemd-analyze blame --no-pager | head -25"
    run systemd-analyze critical-chain --no-pager
    sh_ "journalctl --list-boots --no-pager | tail -30"
    sh_ "last -x -F reboot shutdown 2>/dev/null | head -40"
    echo; echo "### units of interest"
    local u
    for u in $( { echo "$WUNIT"; systemctl list-unit-files --no-legend --no-pager 2>/dev/null | awk '{print $1}' \
               | grep -i -E 'prefect|tides|opr4|docker|containerd|postgres|pgbouncer|backup'; } | grep . | sort -u); do
        echo; echo "#### systemctl cat $u"
        timeout "$T" systemctl cat "$u" --no-pager 2>&1
        timeout "$T" systemctl show "$u" --no-pager -p ActiveState,SubState,UnitFileState,Restart,RestartUSec,StartLimitBurst,StartLimitIntervalUSec,After,Wants,Requires,BindsTo,User,WorkingDirectory,ActiveEnterTimestamp,NRestarts 2>&1
    done
    echo; echo "### user-level systemd units and lingering"
    sh_ "ls -la /var/lib/systemd/linger 2>&1"
    local f
    for f in /home/*/.config/systemd/user/*.service /root/.config/systemd/user/*.service; do show "$f"; done
    echo; echo "### cron"
    show /etc/crontab /etc/cron.d/*
    sh_ "ls -la /etc/cron.daily /etc/cron.hourly /etc/cron.weekly 2>&1"
    if [ $IS_ROOT -eq 1 ]; then
        for u in $(cut -d: -f1 /etc/passwd); do
            c=$(crontab -l -u "$u" 2>/dev/null | grep -v -E '^\s*(#|$)')
            [ -n "$c" ] && { echo; echo "#### crontab -u $u"; echo "$c"; }
        done
    else
        run crontab -l
    fi
    echo; echo "### processes started from terminals (tmux/screen/nohup do not survive a reboot)"
    sh_ "ps -eo user,pid,lstart,cmd | grep -E '[t]mux|[s]creen -|[n]ohup' | cut -c1-200"
}

s_network() {
    run ip -br addr; run ip route
    sh_ "ip -o link | awk '{print \$2, \$4, \$5}'"
    show /etc/netplan/*.yaml /etc/network/interfaces /etc/systemd/network/*.network
    sh_ "ls /etc/sysconfig/network-scripts/ /etc/NetworkManager/system-connections/ 2>/dev/null"
    show /etc/hosts /etc/resolv.conf
    sh_ "grep -E '^hosts:' /etc/nsswitch.conf"
    run resolvectl status --no-pager
    run timedatectl
    run chronyc -n tracking; run chronyc -n sources
    show /etc/chrony/chrony.conf /etc/chrony.conf /etc/systemd/timesyncd.conf /etc/ntp.conf
    run ss -ltnup
    run ufw status verbose
    sh_ "iptables -S 2>/dev/null | grep -v -E '^-A DOCKER|^-N DOCKER' | head -80"
    sh_ "nft list ruleset 2>/dev/null | grep -v -i docker | head -120"
    echo; echo "### cloud-init (instance boot configuration from OpenStack)"
    run cloud-init --version
    run cloud-init status --long
    run cloud-id
    sh_ "ls -la /etc/cloud/cloud-init.disabled /var/lib/cloud/scripts/per-boot /var/lib/cloud/scripts/per-instance /var/lib/cloud/scripts/per-once 2>&1"
    show /etc/cloud/cloud.cfg /etc/cloud/cloud.cfg.d/*.cfg /run/cloud-init/result.json /var/lib/cloud/data/status.json
    sh_ "journalctl -b --no-pager -o short-monotonic -u cloud-init-local -u cloud-init -u cloud-config -u cloud-final 2>/dev/null | tail -40"
    sh_ "grep -h -i -E 'url_helper|giving up|timed out|DataSource|crawl|warning' /var/log/cloud-init.log 2>/dev/null | tail -40"
    sh_ "curl -sS -m 3 -o /dev/null -w 'OpenStack metadata service: HTTP %{http_code} in %{time_total}s\n' http://169.254.169.254/openstack/ 2>&1"
}

s_prefect() {
    echo "worker pids: ${WORKER_PIDS:-none}"
    local p
    for p in $WORKER_PIDS $SERVER_PIDS; do
        echo; echo "### process $p"
        echo "cmd:    $(p_cmd "$p" | cut -c1-300)"
        echo "user:   $(p_user "$p")   started: $(ps -o lstart= -p "$p" 2>/dev/null)"
        echo "cgroup: $(p_cg "$p")"
        echo "exe:    $(readlink -f "/proc/$p/exe" 2>/dev/null)   cwd: $(readlink -f "/proc/$p/cwd" 2>/dev/null)"
        echo "parent chain: $(ps -o pid=,comm= -p "$(ps -o ppid= -p "$p" | tr -d ' ')" 2>/dev/null)"
        echo "environment (PREFECT_*/DOCKER_* values, other names only):"
        tr '\0' '\n' < "/proc/$p/environ" 2>/dev/null | while IFS= read -r kv; do
            case "$kv" in
                PREFECT_*|DOCKER_*|PATH=*|HOME=*|USER=*|CONDA_DEFAULT_ENV=*|VIRTUAL_ENV=*|TZ=*) echo "  $kv" ;;
                *) echo "  ${kv%%=*}" ;;
            esac
        done
    done
    if [ -n "$WCONTAINER" ] && have docker; then
        echo; echo "### the worker runs inside container $WCONTAINER"
        timeout "$T" docker inspect --format 'name={{.Name}} image={{.Config.Image}} restart={{.HostConfig.RestartPolicy.Name}} mounts={{json .Mounts}}' "$WCONTAINER" 2>&1
    fi
    if [ -n "$WUSER" ]; then
        echo; echo "### worker user"
        id "$WUSER"
        run loginctl show-user "$WUSER" -p Linger
        sh_ "ls -la '$WHOME/.prefect' 2>&1; du -sh '$WHOME/.prefect' 2>/dev/null"
        show "$WHOME/.prefect/profiles.toml"
    fi
    if [ -n "$PREFECT_BIN" ]; then
        echo; echo "### \$ prefect version   (as ${WUSER:-current user})"
        as_worker "$PREFECT_BIN" version 2>&1
        echo; echo "### \$ prefect config view --show-sources"
        as_worker "$PREFECT_BIN" config view --show-sources 2>&1
    fi
    if [ -n "$PY" ]; then
        echo; echo "### worker python environment: $PY"
        "$PY" --version 2>&1
        echo; echo "### \$ pip freeze"
        timeout 60 "$PY" -m pip freeze 2>&1
    fi
    echo; echo "### Prefect API (read-only): deployments, work pools, workers, recent runs"
    cat "$PREFECT_RAW"
    if [ -n "$SERVER_PIDS" ]; then
        echo; echo "### local Prefect server database"
        for p in $SERVER_PIDS; do
            tr '\0' '\n' < "/proc/$p/environ" 2>/dev/null | grep -E '^PREFECT_(HOME|SERVER_DATABASE|API_DATABASE|SERVER_SERVICES)'
            d=$(tr '\0' '\n' < "/proc/$p/environ" 2>/dev/null | grep '^PREFECT_HOME=' | cut -d= -f2-)
            [ -z "$d" ] && d="$(getent passwd "$(p_user "$p")" | cut -d: -f6)/.prefect"
            ls -la "$d"/*.db* 2>/dev/null
        done
    fi
}

s_docker() {
    run docker version
    run docker info
    show /etc/docker/daemon.json
    sh_ "systemctl is-enabled docker.service docker.socket containerd.service 2>&1"
    run docker images --digests --format 'table {{.Repository}}:{{.Tag}}\t{{.ID}}\t{{.CreatedAt}}\t{{.Size}}'
    run docker image inspect opr4_ingest:latest --format 'id={{.Id}} created={{.Created}} workdir={{.Config.WorkingDir}} env={{json .Config.Env}} labels={{json .Config.Labels}}'
    sh_ "docker image history --no-trunc --format '{{.CreatedBy}}' opr4_ingest:latest 2>&1 | cut -c1-200 | head -30"
    sh_ "echo containers total: \$(docker ps -aq | wc -l)   running: \$(docker ps -q | wc -l)   exited: \$(docker ps -aq -f status=exited | wc -l)"
    sh_ "docker ps -a --format '{{.ID}}\t{{.Image}}\t{{.Status}}\t{{.CreatedAt}}\t{{.Names}}' | head -40"
    sh_ "for c in \$(docker ps -q); do docker inspect --format '{{.Name}} image={{.Config.Image}} restart={{.HostConfig.RestartPolicy.Name}} log={{.HostConfig.LogConfig.Type}} {{json .HostConfig.LogConfig.Config}}' \$c; done"
    run docker system df
}

env_summary() {   # env_summary FILE [EXAMPLE]: keys, and values only for non-secret keys
    local f="$1" ex="$2" safe k v line
    safe='^(TIDES_ENV|FOURMOST_MODE|TIDES_DB_HOST|TIDES_DB_PORT|TIDES_DB_DATABASE|TIDES_TEST_DB_DATABASE|FOURMOST_SCHEMA|FOURMOST_SURVEY_ID|SURVEYS_ENABLED|LASAIR_[A-Z]+_TOPIC|LASAIR_[A-Z]+_GROUP_ID|[A-Z]+_PIPELINE_NAME|[A-Z]+_PIPELINE_VERSION|DEACTIVATE_AFTER_DAYS|KAFKA_MAX_MESSAGES|GIT_SHA)$'
    echo; echo "### env file: $f   ($(stat -c 'owner=%U:%G mode=%a size=%s modified=%y' "$f" 2>/dev/null))"
    [ -r "$f" ] || { echo "(not readable)"; return; }
    grep -E '^[[:space:]]*(export[[:space:]]+)?[A-Za-z_][A-Za-z0-9_]*=' "$f" | while IFS= read -r line; do
        line=$(echo "$line" | sed -E 's/^[[:space:]]*(export[[:space:]]+)?//')
        k="${line%%=*}"; v="${line#*=}"; v=$(echo "$v" | sed -E "s/^[\"']//; s/[\"'][[:space:]]*$//")
        if [[ "$k" =~ $safe ]]; then printf '  %-28s %s\n' "$k" "$v"
        elif [ -z "$v" ]; then printf '  %-28s (empty)\n' "$k"
        else printf '  %-28s (set, %d chars)\n' "$k" "${#v}"; fi
    done
    keys() { grep -oE '^[[:space:]]*(export[[:space:]]+)?[A-Za-z_][A-Za-z0-9_]*=' "$1" | sed -E 's/^[[:space:]]*(export[[:space:]]+)?//; s/=$//' | sort; }
    local dups; dups=$(keys "$f" | uniq -d | tr '\n' ' ')
    [ -n "$dups" ] && echo "  defined more than once (the last one wins): $dups"
    if [ -n "$ex" ] && [ -f "$ex" ]; then
        echo "  in .env.example but missing here: $(comm -23 <(keys "$ex" | uniq) <(keys "$f" | uniq) | tr '\n' ' ')"
    fi
}

s_repo() {
    local r G
    for r in $REPOS; do
        echo; echo "### repository: $r"
        G=(git -c safe.directory='*' -C "$r")
        sh_ "ls -la '$r'"
        if [ -d "$r/.git" ] || [ -f "$r/.git" ]; then
            echo "HEAD: $("${G[@]}" log -1 --format='%H %ci %s' 2>&1)"
            echo "branch: $("${G[@]}" branch --show-current 2>&1)"
            echo "remotes:"; "${G[@]}" remote -v 2>&1
            echo "status (modified / untracked):"; "${G[@]}" status --short 2>&1 | head -60
            echo "submodules:"; "${G[@]}" submodule status 2>&1
            echo "stash:"; "${G[@]}" stash list 2>&1 | head -5
        fi
        show "$r/prefect.yaml" "$r/requirements.txt" "$r/Dockerfile" "$r/.dockerignore"
    done
    echo; echo "### .env files (deployment mount and repo checkouts)"
    local f ex
    for f in $ENV_FILES; do
        ex=""; for r in $REPOS; do [ -f "$r/.env.example" ] && { ex="$r/.env.example"; break; }; done
        env_summary "$f" "$ex"
    done
}

check_target() {   # check_target HOST PORT LABEL: DNS + TCP reachability (no data sent)
    local h="$1" p="$2" l="$3" how ip tcp="-"
    how="DNS name"
    [[ "$h" =~ ^[0-9.]+$ ]] && how="IP literal, no DNS needed"
    grep -q -E "[[:space:]]$h([[:space:]]|$)" /etc/hosts 2>/dev/null && how="in /etc/hosts"
    ip=$(getent ahostsv4 "$h" 2>/dev/null | awk 'NR==1{print $1}')
    if [ -n "$p" ]; then
        if timeout 5 bash -c "exec 3<>/dev/tcp/$h/$p" 2>/dev/null; then tcp=open; else tcp=UNREACHABLE; fi
    fi
    printf '%-36s %-34s resolves=%-16s tcp/%s=%-12s [%s]\n' "$h" "$l" "${ip:-NO}" "$p" "$tcp" "$how"
}

s_connectivity() {
    echo "4MOST (4most.mpe.mpg.de) is deliberately NOT contacted."
    echo
    [ -n "$DB_HOST" ] && check_target "$DB_HOST" "${DB_PORT:-5432}" "TiDES database"
    if [ -n "$API_URL" ]; then
        local hp ah ap
        hp="${API_URL#*://}"; hp="${hp%%/*}"; hp="${hp##*@}"; ah="${hp%%:*}"; ap="${hp##*:}"
        [ "$ap" = "$hp" ] && { [[ "$API_URL" == https* ]] && ap=443 || ap=80; }
        check_target "$ah" "$ap" "Prefect API"
    fi
    check_target lasair-lsst-kafka.lsst.ac.uk 9092 "Lasair LSST Kafka"
    check_target kafka.lsst.ac.uk 9092 "Lasair ZTF Kafka (if enabled)"
    check_target pypi.org 443 "PyPI (only for rebuilds)"
    check_target registry-1.docker.io 443 "Docker Hub (only for rebuilds)"
    check_target github.com 443 "GitHub (only for code updates)"
    if [ -n "$API_URL" ]; then
        sh_ "curl -sS -m 10 -w '  -> HTTP %{http_code} in %{time_total}s\n' '${API_URL%/}/health'"
        if [[ "$API_URL" == https* ]]; then
            sh_ "echo | openssl s_client -connect '$ah:$ap' -servername '$ah' 2>/dev/null | openssl x509 -noout -subject -issuer -dates"
        fi
    fi
}

s_logs() {
    show /etc/systemd/journald.conf /etc/systemd/journald.conf.d/*.conf
    sh_ "ls -ld /var/log/journal 2>&1"
    run journalctl --disk-usage
    sh_ "du -sh /var/log /var/lib/docker /var/lib/postgresql /var/lib/pgsql /var/lib/containerd 2>/dev/null" 180
    sh_ "ls /etc/logrotate.d/"
    show /etc/logrotate.d/docker* /etc/logrotate.d/postgresql*
    sh_ "journalctl -b -p err --no-pager -o short-iso 2>/dev/null | tail -80 | cut -c1-300"
    sh_ "journalctl -b -1 -p err --no-pager -o short-iso 2>/dev/null | tail -40 | cut -c1-300"
    local u
    for u in $( { echo "$WUNIT"; systemctl list-unit-files --no-legend --no-pager 2>/dev/null | awk '{print $1}' \
               | grep -i -E 'prefect|tides|opr4'; } | grep . | sort -u); do
        sh_ "journalctl -u '$u' --no-pager -o short-iso 2>/dev/null | tail -120 | cut -c1-300"
    done
    sh_ "journalctl -u docker --since '-14d' -p warning --no-pager -o short-iso 2>/dev/null | tail -40 | cut -c1-300"
    echo; echo "### backup evidence"
    sh_ "grep -r -l -E 'pg_dump|pg_basebackup|pgbackrest|barman|wal-g|rsync|restic|borg' /etc/cron* /var/spool/cron /etc/systemd/system 2>/dev/null"
    sh_ "find / -xdev -maxdepth 6 \\( -name '*.dump' -o -name '*.sql.gz' -o -name '*.backup' -o -name '*.pgdump' \\) -mtime -30 -printf '%TY-%Tm-%Td %TH:%TM  %s  %p\n' 2>/dev/null | sort | tail -25" 120
    sh_ "findmnt -t nfs,nfs4,cifs,fuse.sshfs 2>&1"
}

pgq() {   # pgq DB SQL [psql options]: read-only session, never prompts for a password
    local db="$1" sql="$2"; shift 2
    local conn="dbname=$db options='-c default_transaction_read_only=on -c statement_timeout=30000'"
    if [ $IS_ROOT -eq 1 ] && id postgres >/dev/null 2>&1; then
        (cd /tmp && timeout 60 sudo -u postgres psql -X -w -P pager=off "$@" -d "$conn" -c "$sql") 2>&1
    else
        timeout 60 psql -X -w -P pager=off "$@" -d "$conn" -c "$sql" 2>&1
    fi
}
q() { echo; echo "### [$1] $2"; pgq "$1" "$2"; }
PG_OK=0
[ $IS_DB -eq 1 ] && [ "$(pgq postgres 'SELECT 1' -At 2>/dev/null | head -1)" = 1 ] && PG_OK=1

s_postgres() {
    run pg_lsclusters
    if [ $PG_OK -eq 0 ]; then
        echo "(cannot connect to Postgres as the postgres user; the output of a test query follows)"
        pgq postgres "SELECT 1"
        return
    fi
    q postgres "SELECT version();"
    q postgres "SELECT name, setting, unit, source FROM pg_settings WHERE name IN ('data_directory','config_file','hba_file','listen_addresses','port','max_connections','shared_buffers','work_mem','maintenance_work_mem','effective_cache_size','wal_level','archive_mode','archive_command','max_wal_size','checkpoint_timeout','autovacuum','log_destination','logging_collector','log_directory','log_rotation_age','log_rotation_size','log_min_duration_statement','TimeZone','log_timezone','ssl','password_encryption','idle_in_transaction_session_timeout','statement_timeout','shared_preload_libraries','synchronous_commit','fsync','full_page_writes','data_checksums') ORDER BY name;"
    q postgres "SELECT datname, pg_size_pretty(pg_database_size(datname)) AS size, age(datfrozenxid) AS xid_age, datconnlimit FROM pg_database ORDER BY 1;"
    q postgres "SELECT rolname, rolsuper, rolcanlogin, rolconnlimit, rolvaliduntil FROM pg_roles WHERE rolname !~ '^pg_' ORDER BY 1;"
    q postgres "SELECT line_number, type, database, user_name, address, netmask, auth_method, error FROM pg_hba_file_rules ORDER BY line_number;"
    q postgres "SELECT datname, usename, client_addr, application_name, state, count(*) FROM pg_stat_activity WHERE backend_type = 'client backend' GROUP BY 1,2,3,4,5 ORDER BY 6 DESC;"
    q postgres "SELECT pg_is_in_recovery(), * FROM pg_stat_archiver;"
    q postgres "SELECT client_addr, state, sync_state, replay_lag FROM pg_stat_replication;"
    q postgres "SELECT count(*) AS wal_files, pg_size_pretty(sum(size)) AS wal_size FROM pg_ls_waldir();"
    local dd; dd=$(pgq postgres "SHOW data_directory" -At | head -1)
    if [ -d "$dd" ]; then
        echo; echo "### data directory $dd"
        df -hT "$dd"; findmnt -T "$dd" -o TARGET,SOURCE,FSTYPE,OPTIONS
    fi
    local db
    for db in $(pgq postgres "SELECT datname FROM pg_database WHERE NOT datistemplate AND datname <> 'postgres' ORDER BY 1" -At); do
        [ -n "$DB_ONLY" ] && [ "$db" != "$DB_ONLY" ] && continue
        echo; echo "==================== database $db"
        q "$db" "SELECT now(), current_date, current_setting('TimeZone') AS tz;"
        q "$db" "SELECT extname, extversion FROM pg_extension ORDER BY 1;"
        q "$db" "SELECT relname, pg_size_pretty(pg_total_relation_size(relid)) AS total, n_live_tup, n_dead_tup, last_vacuum, last_autovacuum, last_analyze, last_autoanalyze FROM pg_stat_user_tables ORDER BY pg_total_relation_size(relid) DESC LIMIT 25;"
        q "$db" "SELECT schemaname, sequencename, data_type, last_value, max_value, cycle FROM pg_sequences ORDER BY 1, 2;"
        if [ "$(pgq "$db" "SELECT to_regclass('public.tides_master') IS NOT NULL" -At | head -1)" = t ]; then
            q "$db" "SELECT * FROM schema_migrations ORDER BY version;"
            q "$db" "SELECT coalesce(sync_state, 'normal') AS state, count(*) FROM tides_master WHERE sync_pending OR sync_state IS NOT NULL GROUP BY 1;"
            q "$db" "SELECT active, count(*) FROM tides_master GROUP BY 1;"
            q "$db" "SELECT date_trunc('day', request_time) AS day, method, outcome, count(*) FROM tides_4most_log WHERE request_time > now() - interval '7 days' GROUP BY 1, 2, 3 ORDER BY 1 DESC, 2, 3;"
            q "$db" "SELECT survey_id, date '1858-11-17' + max(night) AS last_night, count(*) AS nights FROM survey_nights GROUP BY 1;"
            q "$db" "SELECT min(event_time), max(event_time), count(*) FROM tides_events;"
        fi
    done
    sh_ "grep -h -E 'FATAL|PANIC|ERROR' /var/log/postgresql/*.log /var/lib/pgsql/*/data/log/*.log 2>/dev/null | tail -40 | cut -c1-300"
}

# ---------------------------------------------------------------------------------------------
# Automatic checks -> 00_FLAGS.txt
# ---------------------------------------------------------------------------------------------
checks() {
    [ $IS_ROOT -eq 0 ] && flag INFO "run without sudo: some facts are missing"
    df -P -x tmpfs -x devtmpfs -x overlay -x squashfs 2>/dev/null | awk 'NR>1 {gsub("%","",$5); if ($5+0 >= 80) print $6, $5}' \
        | while read -r m p; do flag FLAG "filesystem $m is $p% full"; done
    awk '!/^[[:space:]]*#/ && NF>=4 && $2!="/" && $2!="/boot" && $2!="/boot/efi" && $3!="swap" && $4!~/nofail/ {print $2" ("$1")"}' /etc/fstab 2>/dev/null \
        | while read -r m; do flag INFO "fstab: $m has no 'nofail': if that volume is not attached at boot, boot stops in emergency mode (no SSH). Intended?"; done
    [ -e /var/run/reboot-required ] && flag INFO "a reboot is pending (/var/run/reboot-required)"
    apt-config dump 2>/dev/null | grep -q -i 'Automatic-Reboot "true"' && flag INFO "unattended-upgrades reboots the VM automatically"
    timedatectl show -p NTPSynchronized --value 2>/dev/null | grep -q no && flag FLAG "system clock is not NTP-synchronised"
    [ -d /var/log/journal ] || flag INFO "journal is not persistent: logs from before a reboot are lost"
    systemctl --failed --no-legend 2>/dev/null | grep -q . && flag FLAG "failed systemd units: $(systemctl --failed --no-legend --plain 2>/dev/null | awk '{print $1}' | tr '\n' ' ')"
    ls /etc/netplan/*cloud-init* >/dev/null 2>&1 && grep -q -i 'dhcp4: *true' /etc/netplan/*cloud-init* 2>/dev/null \
        && flag INFO "network is DHCP configured by cloud-init: the address comes from OpenStack Neutron at each boot"

    if [ $IS_WORKER -eq 1 ]; then
        if [ -z "$WORKER_PIDS" ]; then
            flag FLAG "no 'prefect worker start' process is running"
        else
            case "$WKIND" in
                login-session) flag FLAG "the Prefect worker was started from a login session (tmux/screen/nohup: cgroup $WCG); it will not restart after a reboot" ;;
                user-service) loginctl show-user "$WUSER" -p Linger 2>/dev/null | grep -q yes \
                    || flag FLAG "the worker is a systemd --user service but lingering is off for $WUSER: it starts only when $WUSER logs in" ;;
                container) flag INFO "the worker runs in a container ($WCONTAINER): check its restart policy in 50_prefect.txt" ;;
                cron) flag INFO "the worker was started by cron (@reboot?): nothing restarts it if it exits" ;;
                system-service)
                    [ "$(systemctl is-enabled "$WUNIT" 2>/dev/null)" = enabled ] || flag FLAG "worker unit $WUNIT is not enabled at boot"
                    [ "$(systemctl show "$WUNIT" -p Restart --value 2>/dev/null)" = no ] && flag FLAG "worker unit $WUNIT has Restart=no"
                    systemctl show "$WUNIT" -p After -p Requires -p Wants --value 2>/dev/null | grep -q docker \
                        || flag INFO "worker unit $WUNIT is not ordered after docker.service" ;;
            esac
        fi
        if have docker; then
            [ "$(systemctl is-enabled docker.service 2>/dev/null)" = enabled ] || flag FLAG "docker.service is not enabled at boot"
            if docker info >/dev/null 2>&1; then
                docker image inspect opr4_ingest:latest >/dev/null 2>&1 || flag FLAG "image opr4_ingest:latest is not on this host (pull policy IfNotPresent: runs would try Docker Hub)"
                local ex; ex=$(docker ps -aq -f status=exited 2>/dev/null | wc -l)
                [ "$ex" -gt 200 ] && flag FLAG "$ex exited containers kept: one per flow run, they pile up (about 8,800 a year at hourly runs)"
            fi
        elif [ -n "$WORKER_PIDS" ]; then
            flag FLAG "a Docker work pool is configured but docker is not installed here"
        fi
        [ -z "$API_URL" ] && flag FLAG "could not determine PREFECT_API_URL (pass --prefect-api)"
        grep -q 'server version: ERROR' "$PREFECT_RAW" 2>/dev/null && flag FLAG "the Prefect API did not answer (see 50_prefect.txt)"
        [ -n "$SERVER_PIDS" ] && flag INFO "a Prefect server runs on this VM: it is part of the boot chain (and its database grows)"
        local f mode
        for f in $ENV_FILES; do
            mode=$(stat -c %a "$f" 2>/dev/null)
            case "$mode" in 600|640|400|440) ;; *) flag FLAG "$f holds secrets but has mode $mode (expected 600 or 640)" ;; esac
        done
        [ -z "$ENV_FILES" ] && flag FLAG "no .env file found (pass --repo)"
        [ -n "$DB_HOST" ] && ! timeout 5 bash -c "exec 3<>/dev/tcp/$DB_HOST/${DB_PORT:-5432}" 2>/dev/null \
            && flag FLAG "the TiDES database $DB_HOST:${DB_PORT:-5432} is not reachable from here"
        [ -n "$DB_HOST" ] && ! [[ "$DB_HOST" =~ ^[0-9.]+$ ]] && ! grep -q -w "$DB_HOST" /etc/hosts \
            && flag INFO "the database host '$DB_HOST' needs DNS to resolve"
    fi

    if [ $IS_DB -eq 1 ]; then
        systemctl list-unit-files --no-legend 'postgresql*' 2>/dev/null | grep -q enabled \
            || flag FLAG "no postgresql unit is enabled at boot"
        [ $PG_OK -eq 0 ] && { flag FLAG "could not query Postgres as the postgres user (see 90_postgres.txt)"; return; }
        local r
        r=$(pgq postgres "SELECT rolname || ' expires ' || rolvaliduntil FROM pg_roles WHERE rolcanlogin AND rolvaliduntil IS NOT NULL AND rolvaliduntil < now() + interval '5 years'" -At)
        [ -n "$r" ] && echo "$r" | while read -r l; do flag FLAG "database role $l (within 5 years)"; done
        r=$(pgq postgres "SELECT datname || ' xid age ' || age(datfrozenxid) FROM pg_database WHERE age(datfrozenxid) > 500000000" -At)
        [ -n "$r" ] && flag FLAG "transaction ID age is high: $r"
        local dd mnt; dd=$(pgq postgres "SHOW data_directory" -At | head -1)
        mnt=$(findmnt -n -o TARGET -T "$dd" 2>/dev/null)
        [ "$mnt" = / ] && flag INFO "Postgres data ($dd) is on the root disk: check whether the VM boots from a volume (an ephemeral root disk is lost if the VM is rebuilt)"
        [ "$(pgq postgres "SHOW archive_mode" -At | head -1)" = off ] \
            && ! grep -r -q -E 'pg_dump|pg_basebackup|pgbackrest|barman|wal-g' /etc/cron* /var/spool/cron /etc/systemd/system 2>/dev/null \
            && flag FLAG "no backup evidence on this VM (no WAL archiving, no pg_dump/basebackup in cron or timers)"
    fi
}

# ---------------------------------------------------------------------------------------------
echo "Collecting into $OUT" >&2
section 00_summary s_summary
section 10_os s_os
section 20_boot_services s_boot
section 30_network_time_cloudinit s_network
if [ $IS_WORKER -eq 1 ]; then
    section 50_prefect s_prefect
    have docker && section 60_docker s_docker
    section 70_repo_env s_repo
    section 80_connectivity s_connectivity
fi
[ $IS_DB -eq 1 ] && section 90_postgres s_postgres
section 95_logs_backups s_logs
checks
{
    echo "Automatic checks. FLAG = probably a problem; INFO = check that it is intended."
    echo "These are a starting point only; the other files hold the evidence."
    echo
    grep '^FLAG' "$FLAGS" | sort -u
    grep '^INFO' "$FLAGS" | sort -u
} | sed -E -f "$REDACT" > "$OUT/00_FLAGS.txt"

rm -rf "$FLAGS" "$REDACT" "$PREFECT_RAW" "$OUT/.prefect_home"
tar -czf "$OUT.tar.gz" -C "$(dirname "$OUT")" "$(basename "$OUT")"

echo >&2
cat "$OUT/00_FLAGS.txt" >&2
echo >&2
echo "Lines that mention secret-like words but were not redacted (check before sharing):" >&2
grep -r -n -i -E 'pass|secret|token|apikey|api_key|private' "$OUT" | grep -v -E 'REDACTED|\(set, [0-9]+ chars\)|\(empty\)' | cut -c1-200 | head -40 >&2
echo >&2
echo "Done: $OUT.tar.gz   (the folder $OUT has the same content)" >&2
