# =============================================================================
# lib/log.sh - Shared structured logging for autorip shell scripts
# https://github.com/boylermb/autorip
# =============================================================================
# Sourced by autorip.sh, transcode-worker.sh, backfill-tv-artwork.sh.
#
# Every line a script wants to emit goes through `log` (or one of the
# convenience wrappers below). Two streams are produced for every call:
#
#   1. Human-readable line on stdout (captured by the existing
#      StandardOutput=append:/var/log/autorip/<unit>.log directive and by
#      journald via SyslogIdentifier=).
#
#   2. A single JSON line appended to /var/log/autorip/events.jsonl.
#      Promtail tails this file and ships the structured records to Loki
#      with all the fields preserved as labels / extracted log fields.
#
# JSON schema (one line per event):
#   {
#     "ts":      "2026-04-21T18:23:01.123Z",   ISO 8601 UTC, ms resolution
#     "level":   "info" | "warn" | "error" | "debug",
#     "host":    "ruby",                       hostname
#     "service": "autorip" | "transcode-worker" | "backfill-tv-artwork",
#     "device":  "sr0",                        optional, blank for non-rip
#     "job_id":  "ruby-sr0-1714329581",        optional correlation id
#     "stage":   "rip" | "transcode" | "rename" | "review" | ...,
#     "disc_type": "dvd" | "bluray" | "uhd" | "audio-cd",
#     "title":   "Futurama S2D1",              optional
#     "msg":     "Ripping title 03"
#   }
#
# Callers may set the following environment variables before sourcing or
# at any time after; they're picked up on every log call:
#
#   AUTORIP_LOG_SERVICE   service name (defaults to script basename)
#   AUTORIP_LOG_DEVICE    device shortname (sr0, sr1, …)
#   AUTORIP_LOG_JOB_ID    correlation id for the current rip/job
#   AUTORIP_LOG_STAGE     pipeline stage (rip, transcode, rename, …)
#   AUTORIP_LOG_DISC_TYPE dvd | bluray | uhd | audio-cd
#   AUTORIP_LOG_TITLE     human-readable title for the current job
#   AUTORIP_LOG_DIR       where to write events.jsonl (default /var/log/autorip)
#   AUTORIP_LOG_LEVEL     minimum level to emit: debug|info|warn|error
#                         (default info)
#   AUTORIP_LOG_JSON_ONLY if "yes" suppress the human-readable stdout line
#
# All of these are optional - if unset the corresponding JSON field is
# omitted.  The library never fails the caller: any error writing the
# JSON sidecar is silently dropped so that rip jobs are not aborted by
# a full disk in /var/log.
# =============================================================================

# Guard against double-sourcing.
if [ -n "${__AUTORIP_LOG_SH_LOADED:-}" ]; then
    return 0 2>/dev/null || true
fi
__AUTORIP_LOG_SH_LOADED=1

# Pick up sensible defaults the first time we're sourced.
: "${AUTORIP_LOG_DIR:=/var/log/autorip}"
: "${AUTORIP_LOG_LEVEL:=info}"
: "${AUTORIP_LOG_SERVICE:=$(basename "${BASH_SOURCE[1]:-$0}" .sh)}"

# Map level names to numeric severities for filtering.
__autorip_log_severity() {
    case "${1:-info}" in
        debug) echo 10 ;;
        info)  echo 20 ;;
        warn|warning) echo 30 ;;
        error|err) echo 40 ;;
        *)     echo 20 ;;
    esac
}

# Escape a value for inclusion inside a JSON string.
__autorip_log_json_escape() {
    # Handles backslash, double-quote, control chars (\n, \r, \t).
    printf '%s' "$1" | sed -e 's/\\/\\\\/g' \
                            -e 's/"/\\"/g' \
                            -e ':a;N;$!ba;s/\n/\\n/g' \
                            -e 's/\r/\\r/g' \
                            -e 's/\t/\\t/g'
}

# Append `"key":"value"` to the JSON buffer if the value is non-empty.
__autorip_log_json_kv() {
    local key="$1" value="$2"
    [ -z "$value" ] && return 0
    printf ',"%s":"%s"' "$key" "$(__autorip_log_json_escape "$value")"
}

# Public: log <level> <message...>
log() {
    local level="info"
    case "${1:-}" in
        debug|info|warn|warning|error|err)
            level="$1"; shift
            ;;
    esac
    [ "$level" = "warning" ] && level="warn"
    [ "$level" = "err" ] && level="error"

    local msg="$*"
    local sev want
    sev=$(__autorip_log_severity "$level")
    want=$(__autorip_log_severity "$AUTORIP_LOG_LEVEL")
    if [ "$sev" -lt "$want" ]; then
        return 0
    fi

    local host
    host=$(hostname 2>/dev/null || echo unknown)
    local ts_human ts_iso
    ts_human=$(date '+%Y-%m-%d %H:%M:%S')
    # GNU date supports %N (nanoseconds); BSD/macOS prints '%3N' literally
    # so detect that and fall back to second resolution.
    ts_iso=$(date -u '+%Y-%m-%dT%H:%M:%S.%3NZ' 2>/dev/null)
    if [ -z "$ts_iso" ] || [[ "$ts_iso" == *N* ]]; then
        ts_iso=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
    fi

    # ---- Human-readable line (stdout / file / journald) ----
    if [ "${AUTORIP_LOG_JSON_ONLY:-no}" != "yes" ]; then
        local prefix="[${AUTORIP_LOG_SERVICE}"
        [ -n "${AUTORIP_LOG_DEVICE:-}" ] && prefix="${prefix} ${AUTORIP_LOG_DEVICE}"
        prefix="${prefix}]"
        case "$level" in
            error) printf '%s %s ERROR %s\n' "$ts_human" "$prefix" "$msg" >&2 ;;
            warn)  printf '%s %s WARN  %s\n' "$ts_human" "$prefix" "$msg" >&2 ;;
            *)     printf '%s %s %s\n'        "$ts_human" "$prefix" "$msg" ;;
        esac
    fi

    # ---- JSON sidecar (Promtail / Loki) ----
    # All errors swallowed so logging never breaks a rip.
    {
        mkdir -p "$AUTORIP_LOG_DIR" 2>/dev/null || true
        local line
        line=$(printf '{"ts":"%s","level":"%s","host":"%s","service":"%s"' \
            "$ts_iso" "$level" "$host" "$(__autorip_log_json_escape "$AUTORIP_LOG_SERVICE")")
        line+=$(__autorip_log_json_kv device    "${AUTORIP_LOG_DEVICE:-}")
        line+=$(__autorip_log_json_kv job_id    "${AUTORIP_LOG_JOB_ID:-}")
        line+=$(__autorip_log_json_kv stage     "${AUTORIP_LOG_STAGE:-}")
        line+=$(__autorip_log_json_kv disc_type "${AUTORIP_LOG_DISC_TYPE:-}")
        line+=$(__autorip_log_json_kv title     "${AUTORIP_LOG_TITLE:-}")
        line+=$(printf ',"msg":"%s"}' "$(__autorip_log_json_escape "$msg")")
        printf '%s\n' "$line" >> "$AUTORIP_LOG_DIR/events.jsonl"
    } 2>/dev/null || true
}

log_debug() { log debug "$@"; }
log_info()  { log info  "$@"; }
log_warn()  { log warn  "$@"; }
log_error() { log error "$@"; }

# Helper: derive a stable correlation id for a rip.
# autorip_log_new_job_id <device-basename>  →  ruby-sr0-1714329581
autorip_log_new_job_id() {
    local dev="${1:-${AUTORIP_LOG_DEVICE:-job}}"
    local h
    h=$(hostname 2>/dev/null || echo host)
    printf '%s-%s-%s\n' "$h" "$dev" "$(date +%s)"
}
