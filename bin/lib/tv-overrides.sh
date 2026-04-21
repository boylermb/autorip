#!/usr/bin/env bash
# tv-overrides.sh — manual TV-show name/ID overrides for autorip.
#
# Loaded by transcode-worker.sh.  All functions are pure-shell and safe to
# source multiple times.
#
# Override file format (JSON, default /etc/autorip/tv-overrides.json):
#
#   {
#     "shows": {
#       "the sopranos":   {"tmdb_id": 1398, "name": "The Sopranos"},
#       "futurama":       {"tmdb_id": 615},
#       "sp1":            {"name": "Star Trek The Original Series"}
#     },
#     "label_rewrites": [
#       {"pattern": "^SP[0-9]+$",    "show": "Star Trek The Original Series"},
#       {"pattern": "^TOS_DISC_[0-9]+$", "show": "Star Trek"}
#     ]
#   }
#
# Lookup order:
#   1. label_rewrites — regex matched against the FULL raw disc title (case
#      insensitive); first match wins, replaces the show name.
#   2. shows         — keyed by lowercase show name (after label_rewrites
#      have been applied); provides forced TMDb id and/or canonical name.
#
# Public function: tv_apply_overrides <raw_show_name> <raw_disc_title>
#   Sets:
#     TV_OVERRIDE_SHOW     - new canonical show name (may be empty)
#     TV_OVERRIDE_TMDB_ID  - forced TMDb id (may be empty)
#   Returns:
#     0 if any override applied, 1 otherwise.

if [ -n "${_TV_OVERRIDES_LIB_SOURCED:-}" ]; then
    return 0
fi
_TV_OVERRIDES_LIB_SOURCED=1

TV_OVERRIDES_FILE="${TV_OVERRIDES_FILE:-/etc/autorip/tv-overrides.json}"

_tv_overrides_log() {
    if declare -F log >/dev/null 2>&1; then
        log "overrides: $*"
    else
        echo "overrides: $*" >&2
    fi
}

# Apply overrides to a raw (show, disc-title) pair.
tv_apply_overrides() {
    local raw_show="$1"
    local disc_title="$2"

    TV_OVERRIDE_SHOW=""
    TV_OVERRIDE_TMDB_ID=""

    [ -f "$TV_OVERRIDES_FILE" ] || return 1
    [ -s "$TV_OVERRIDES_FILE" ] || return 1

    local result
    result=$(python3 - "$TV_OVERRIDES_FILE" "$raw_show" "$disc_title" <<'PY' 2>/dev/null
import sys, json, re
path, raw_show, disc_title = sys.argv[1], sys.argv[2], sys.argv[3]
try:
    with open(path) as f:
        data = json.load(f)
except Exception as e:
    print(f"ERROR::{e}")
    sys.exit(0)

show_name = raw_show
forced_id = ""
name_was_overridden = False

# 1. label_rewrites against the full raw disc title
for rw in data.get("label_rewrites", []) or []:
    pat = rw.get("pattern")
    new = rw.get("show")
    if not pat or not new:
        continue
    try:
        if re.search(pat, disc_title, re.IGNORECASE):
            show_name = new
            name_was_overridden = True
            break
    except re.error:
        continue

# 2. shows map keyed by lowercase show name
shows = data.get("shows", {}) or {}
# Normalize keys to lowercase for lookup
lookup = { (k or "").strip().lower(): v for k, v in shows.items() }
entry = lookup.get(show_name.strip().lower())
if entry:
    forced_id = str(entry.get("tmdb_id", "")).strip()
    name_override = (entry.get("name") or "").strip()
    if name_override:
        show_name = name_override
        name_was_overridden = True

# Emit the show name only if explicitly overridden; otherwise leave it empty
# so the caller falls back to the canonical TMDb name (when an id is forced)
# or to its own canonicalize logic.
emit_name = show_name if name_was_overridden else ""
if name_was_overridden or forced_id:
    print(f"OK::{emit_name}::{forced_id}")
else:
    print("NONE")
PY
)

    case "$result" in
        OK::*)
            local rest="${result#OK::}"
            TV_OVERRIDE_SHOW="${rest%%::*}"
            TV_OVERRIDE_TMDB_ID="${rest##*::}"
            _tv_overrides_log "applied to '$raw_show' (disc='$disc_title') → show='$TV_OVERRIDE_SHOW' tmdb_id='${TV_OVERRIDE_TMDB_ID:-<none>}'"
            return 0
            ;;
        ERROR::*)
            _tv_overrides_log "failed to parse $TV_OVERRIDES_FILE: ${result#ERROR::}"
            return 1
            ;;
        *)
            return 1
            ;;
    esac
}
