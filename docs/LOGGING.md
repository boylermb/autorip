# Logging

autorip emits a single, consistent log stream from every component
(rip script, transcode worker, API agent, backfill helper) so they can
be aggregated by [Loki](https://grafana.com/oss/loki/) and queried as
one timeline in Grafana.  See `FourthStreetHomeNetwork/docs/LOGGING.md`
for the cluster-side bits (Loki, Promtail, dashboards).

## Where each component writes

| Component               | Stdout / journald               | File on host                              |
|-------------------------|---------------------------------|-------------------------------------------|
| `autorip.sh` (per drive)| `autorip-srX` (journalctl tag)  | `/var/log/autorip/srX.log`                |
| `transcode-worker.sh`   | `autorip-transcode-worker`      | `/var/log/autorip/transcode-worker.log`   |
| `autorip-agent` (Flask) | `autorip-agent`                 | `/var/log/autorip/agent.jsonl` (JSON)     |
| `backfill-tv-artwork.sh`| stdout (interactive)            | (none — manually run)                     |

In addition, **every shell component** appends a structured
`/var/log/autorip/events.jsonl` line for every `log` call (see
[`bin/lib/log.sh`](../bin/lib/log.sh)).  This is the file Promtail
ships to Loki.

## JSON schema

One object per line, fields are nullable and may be omitted when blank:

```json
{
  "ts":        "2026-04-21T18:23:01.123Z",
  "level":     "info",
  "host":      "garnet",
  "service":   "autorip",
  "device":    "sr0",
  "job_id":    "garnet-sr0-1714329581",
  "stage":     "rip",
  "disc_type": "DVD",
  "title":     "Futurama S2D1",
  "msg":       "Ripping title 03"
}
```

`autorip-agent` adds a few extra fields for HTTP requests:
`method`, `path`, `status`, `remote`, `duration_ms`.

## Setting context from a shell script

`lib/log.sh` reads a handful of environment variables on every call.
Export them once near the top of a code path and every subsequent
`log`/`log_info`/`log_warn`/`log_error` will be tagged correctly:

```bash
source /usr/local/lib/autorip/log.sh

export AUTORIP_LOG_SERVICE="autorip"
export AUTORIP_LOG_DEVICE="sr0"
export AUTORIP_LOG_JOB_ID="$(autorip_log_new_job_id sr0)"

log_info "Disc detected"                  # info
log_warn "Drive not ready, retrying"      # warn  → stderr
log_error "MakeMKV exited 1"              # error → stderr
log debug "ffprobe took 12ms"             # filtered out unless AUTORIP_LOG_LEVEL=debug
```

`update_status` in `autorip.sh` automatically refreshes
`AUTORIP_LOG_DISC_TYPE`, `AUTORIP_LOG_TITLE`, and `AUTORIP_LOG_STAGE`
whenever the dashboard status changes, so most callers don't need to
touch them manually.

## Setting context from Python (`autorip-agent`)

Use `extra=` on the standard `logging` calls and the JSON formatter
will lift them into the record:

```python
_log.info("rip enqueued", extra={
    "device": "sr0",
    "job_id": "ruby-sr0-1714329581",
    "stage": "enqueue",
})
```

## Useful Loki queries

```logql
# Everything autorip-related, last 15 min
{job="autorip"} | json

# Just errors from any component
{job="autorip"} | json | level="error"

# Trace a single rip end-to-end across hosts
{job="autorip"} | json | job_id="garnet-sr0-1714329581"

# Per-host transcode rate (lines/min)
sum by (host) (rate({job="autorip", service="transcode-worker"}[5m]))
```

## Rotation

The `make install` target installs `/etc/logrotate.d/autorip` which
weekly-rotates both `*.log` and `*.jsonl` (4 generations, gzip,
`copytruncate` so Promtail doesn't lose its file handle).
