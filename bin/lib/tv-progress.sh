#!/usr/bin/env bash
# =============================================================================
# tv-progress.sh — Per-show TV episode-numbering state
# =============================================================================
# Replaces the global EPISODES_PER_DISC heuristic with a per-show, per-season
# state file that records how many episodes each disc actually contributed.
#
# Layout: $TV_PROGRESS_DIR/<show-slug>-S<NN>.json
#
# Schema:
#   {
#     "show":   "Futurama",
#     "season": 2,
#     "discs": {
#       "1": {"episode_count": 4, "first_episode": 1, "ripped_at": "..."},
#       "2": {"episode_count": 4, "first_episode": 5, "ripped_at": "..."}
#     }
#   }
#
# Public API:
#   tv_progress_for_disc <show> <season> <disc> <title_count>
#       Sets TV_FIRST_EPISODE (1-based) and TV_PROGRESS_STATUS.
#       Status values:
#         ok      — disc recorded, first_episode is the next sequential number
#         reused  — disc was already recorded (re-rip); first_episode preserved
#         gap     — disc is out of order (e.g. disc 3 with no disc 2);
#                   first_episode is a best-guess; caller should route to
#                   _pending/ for human review
#       Returns 0 on ok|reused|gap; 1 on hard error.
#
#   tv_progress_clear <show> <season>
#       Delete the state file (manual reset).
# =============================================================================

if [ "${_TV_PROGRESS_LIB_SOURCED:-0}" = "1" ]; then
    return 0
fi
_TV_PROGRESS_LIB_SOURCED=1

TV_PROGRESS_DIR="${TV_PROGRESS_DIR:-${OUTPUT_BASE:-/srv/nas/Media}/.autorip-state/tv-progress}"

_tv_progress_log() {
    if declare -F log >/dev/null 2>&1; then
        log "tv-progress: $*"
    else
        echo "tv-progress: $*" >&2
    fi
}

_tv_progress_slug() {
    # filesystem-safe lowercase slug
    printf '%s' "$1" | tr '[:upper:]' '[:lower:]' \
        | tr -c 'a-z0-9' '-' | sed 's/--*/-/g; s/^-//; s/-$//'
}

_tv_progress_file() {
    local show="$1" season="$2"
    local slug
    slug=$(_tv_progress_slug "$show")
    printf "%s/%s-S%02d.json" "$TV_PROGRESS_DIR" "$slug" "$season"
}

# tv_progress_for_disc <show> <season> <disc> <title_count>
tv_progress_for_disc() {
    local show="$1" season="$2" disc="$3" count="$4"
    TV_FIRST_EPISODE=""
    TV_PROGRESS_STATUS=""

    if [ -z "$show" ] || [ -z "$season" ] || [ -z "$disc" ] || [ -z "$count" ]; then
        _tv_progress_log "missing args (show='$show' season='$season' disc='$disc' count='$count')"
        return 1
    fi

    local file
    file=$(_tv_progress_file "$show" "$season")
    mkdir -p "$(dirname "$file")"

    local out
    out=$(python3 - "$file" "$show" "$season" "$disc" "$count" <<'PY' 2>&1
import json, sys, os, datetime, tempfile

file, show = sys.argv[1], sys.argv[2]
season, disc, count = int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])

if os.path.exists(file):
    with open(file) as f:
        state = json.load(f)
else:
    state = {"show": show, "season": season, "discs": {}}

discs = state.setdefault("discs", {})
key = str(disc)

# Re-rip case: disc already recorded
if key in discs:
    print(f"{discs[key]['first_episode']} reused")
    sys.exit(0)

prior = sorted(int(k) for k in discs.keys())

# First disc ever for this season
if not prior:
    if disc != 1:
        # Gap: first rip but disc isn't #1
        print(f"1 gap")
        sys.exit(0)
    first_ep = 1
else:
    last = prior[-1]
    prev = discs[str(last)]
    expected_next = prev["first_episode"] + prev["episode_count"]
    if disc != last + 1:
        # Out of sequence — return best-guess but flag as gap
        print(f"{expected_next} gap")
        sys.exit(0)
    first_ep = expected_next

discs[key] = {
    "episode_count": count,
    "first_episode": first_ep,
    "ripped_at": datetime.datetime.now().isoformat(timespec="seconds"),
}

# Atomic write
fd, tmp = tempfile.mkstemp(dir=os.path.dirname(file), prefix=".progress-", suffix=".tmp")
with os.fdopen(fd, "w") as f:
    json.dump(state, f, indent=2)
os.replace(tmp, file)
print(f"{first_ep} ok")
PY
)
    local rc=$?
    if [ $rc -ne 0 ]; then
        _tv_progress_log "python helper failed (rc=$rc): $out"
        return 1
    fi

    TV_FIRST_EPISODE=$(awk '{print $1}' <<< "$out")
    TV_PROGRESS_STATUS=$(awk '{print $2}' <<< "$out")

    if [ -z "$TV_FIRST_EPISODE" ] || [ -z "$TV_PROGRESS_STATUS" ]; then
        _tv_progress_log "could not parse helper output: $out"
        return 1
    fi

    return 0
}

tv_progress_clear() {
    local show="$1" season="$2"
    local file
    file=$(_tv_progress_file "$show" "$season")
    rm -f "$file"
}
