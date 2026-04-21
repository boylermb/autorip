#!/usr/bin/env bash
# =============================================================================
# tmdb.sh — Sourceable TMDb client library for autorip
# =============================================================================
# Source this file from autorip.sh / transcode-worker.sh.
#
# Functions provided:
#   tmdb_search_show <name>            → sets TMDB_SHOW_ID, TMDB_SHOW_NAME
#   tmdb_fetch_season <id> <season>    → fills TMDB_EP_NAMES, TMDB_EP_RUNTIMES
#   tmdb_fetch_show_images <id>        → sets TMDB_POSTER_URL
#   tmdb_fetch_season_images <id> <s>  → sets TMDB_SEASON_POSTER_URL
#   tmdb_download_image <url> <dest>   → returns 0 on success
#   tmdb_load_show <name> <season>     → convenience: search + season + images
#
# Globals (cleared at the start of each top-level lookup):
#   TMDB_SHOW_ID                (integer)
#   TMDB_SHOW_NAME              (canonical name from TMDb)
#   TMDB_EP_NAMES[ep#]          (associative array)
#   TMDB_EP_RUNTIMES[ep#]       (associative array, minutes)
#   TMDB_POSTER_URL             (full URL to poster image)
#   TMDB_SEASON_POSTER_URL      (full URL to season poster image)
#
# Logging:
#   The caller's `log` function is called with "TMDb: ..." messages if it
#   exists; otherwise messages go to stderr.
#
# API key:
#   Reads $TMDB_API_KEY from the environment. Falls back to the historical
#   hardcoded key for backward compatibility.
# =============================================================================

# Guard against double-sourcing
if [ "${_TMDB_LIB_SOURCED:-0}" = "1" ]; then
    return 0
fi
_TMDB_LIB_SOURCED=1

# ---------- Configuration ----------
TMDB_API_KEY="${TMDB_API_KEY:-db972a607f2760bb19ff8bb34074b4c7}"
TMDB_BASE_URL="${TMDB_BASE_URL:-https://api.themoviedb.org/3}"
TMDB_IMAGE_BASE="${TMDB_IMAGE_BASE:-https://image.tmdb.org/t/p}"
TMDB_POSTER_SIZE="${TMDB_POSTER_SIZE:-w780}"

# ---------- Globals ----------
declare -A TMDB_EP_NAMES
declare -A TMDB_EP_RUNTIMES
TMDB_SHOW_ID=""
TMDB_SHOW_NAME=""
TMDB_POSTER_URL=""
TMDB_SEASON_POSTER_URL=""

# Caches (avoid repeat API calls within a single process)
declare -A _TMDB_SEARCH_CACHE       # name → id
_TMDB_SEASON_CACHE_KEY=""           # "id::season" of last loaded season
_TMDB_IMAGES_CACHE_KEY=""
_TMDB_SEASON_IMAGES_CACHE_KEY=""

# ---------- Internal helpers ----------
_tmdb_log() {
    if declare -F log >/dev/null 2>&1; then
        log "TMDb: $*"
    else
        echo "TMDb: $*" >&2
    fi
}

_tmdb_urlencode() {
    python3 -c "import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1]))" "$1" 2>/dev/null
}

_tmdb_curl() {
    # $1 = path (relative to TMDB_BASE_URL); echoes JSON response or returns 1
    local path="$1"
    local sep="?"
    case "$path" in *\?*) sep="&" ;; esac
    curl -sf --max-time 10 \
        "${TMDB_BASE_URL}${path}${sep}api_key=${TMDB_API_KEY}" 2>/dev/null
}

# ---------- Public API ----------

# Search TMDb for a TV show by name.
# Sets TMDB_SHOW_ID and TMDB_SHOW_NAME on success.
# Returns 0 if a result was found, 1 otherwise.
tmdb_search_show() {
    local name="$1"
    [ -n "$name" ] || return 1

    TMDB_SHOW_ID=""
    TMDB_SHOW_NAME=""

    # Cache hit?
    if [ -n "${_TMDB_SEARCH_CACHE[$name]:-}" ]; then
        local cached="${_TMDB_SEARCH_CACHE[$name]}"
        TMDB_SHOW_ID="${cached%%::*}"
        TMDB_SHOW_NAME="${cached#*::}"
        return 0
    fi

    local encoded
    encoded=$(_tmdb_urlencode "$name") || return 1

    local response
    response=$(_tmdb_curl "/search/tv?query=${encoded}") || {
        _tmdb_log "search API call failed for '$name'"
        return 1
    }

    local parsed
    parsed=$(printf '%s' "$response" | python3 -c "
import sys, json
data = json.load(sys.stdin)
results = data.get('results', [])
if results:
    r = results[0]
    print(f\"{r['id']}::{r.get('name', '')}\")
" 2>/dev/null) || return 1

    if [ -z "$parsed" ]; then
        _tmdb_log "no results for '$name'"
        return 1
    fi

    TMDB_SHOW_ID="${parsed%%::*}"
    TMDB_SHOW_NAME="${parsed#*::}"
    _TMDB_SEARCH_CACHE[$name]="$parsed"
    _tmdb_log "matched '$name' → '$TMDB_SHOW_NAME' (id $TMDB_SHOW_ID)"
    return 0
}

# Fetch episode names and runtimes for a season.
# Fills TMDB_EP_NAMES[ep_num] and TMDB_EP_RUNTIMES[ep_num].
# Returns 0 on success, 1 on failure.
tmdb_fetch_season() {
    local show_id="$1"
    local season="$2"
    [ -n "$show_id" ] && [ -n "$season" ] || return 1

    local cache_key="${show_id}::${season}"
    if [ "$_TMDB_SEASON_CACHE_KEY" = "$cache_key" ] && [ ${#TMDB_EP_NAMES[@]} -gt 0 ]; then
        return 0
    fi

    TMDB_EP_NAMES=()
    TMDB_EP_RUNTIMES=()
    _TMDB_SEASON_CACHE_KEY=""

    local response
    response=$(_tmdb_curl "/tv/${show_id}/season/${season}") || {
        _tmdb_log "season API call failed for show ${show_id} season ${season}"
        return 1
    }

    # Emit shell-safe assignment statements, one per episode.
    # Format:  ep_num<TAB>name<TAB>runtime
    local rows
    rows=$(printf '%s' "$response" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for ep in data.get('episodes', []):
    num = ep.get('episode_number')
    if num is None:
        continue
    name = (ep.get('name') or '').replace('\t', ' ').replace('\n', ' ')
    runtime = ep.get('runtime') or ''
    print(f'{num}\t{name}\t{runtime}')
" 2>/dev/null) || {
        _tmdb_log "failed to parse season data"
        return 1
    }

    if [ -z "$rows" ]; then
        _tmdb_log "season ${season} has no episodes"
        return 1
    fi

    while IFS=$'\t' read -r num name runtime; do
        [ -n "$num" ] || continue
        TMDB_EP_NAMES[$num]="$name"
        [ -n "$runtime" ] && TMDB_EP_RUNTIMES[$num]="$runtime"
    done <<< "$rows"

    _TMDB_SEASON_CACHE_KEY="$cache_key"
    _tmdb_log "loaded ${#TMDB_EP_NAMES[@]} episode(s) for show ${show_id} season ${season}"
    return 0
}

# Fetch poster URL for a show.
# Sets TMDB_POSTER_URL to the full https URL (or empty if none).
tmdb_fetch_show_images() {
    local show_id="$1"
    [ -n "$show_id" ] || return 1

    if [ "$_TMDB_IMAGES_CACHE_KEY" = "$show_id" ] && [ -n "$TMDB_POSTER_URL" ]; then
        return 0
    fi

    TMDB_POSTER_URL=""
    _TMDB_IMAGES_CACHE_KEY=""

    local response
    response=$(_tmdb_curl "/tv/${show_id}/images") || {
        _tmdb_log "show images API call failed for show ${show_id}"
        return 1
    }

    local poster_path
    poster_path=$(printf '%s' "$response" | python3 -c "
import sys, json
data = json.load(sys.stdin)
posters = data.get('posters', [])
# Prefer English, then no-language, then anything
def rank(p):
    lang = p.get('iso_639_1') or ''
    if lang == 'en': return 0
    if lang == '':   return 1
    return 2
posters.sort(key=lambda p: (rank(p), -(p.get('vote_average') or 0)))
if posters:
    print(posters[0]['file_path'])
" 2>/dev/null) || return 1

    if [ -z "$poster_path" ]; then
        _tmdb_log "no poster found for show ${show_id}"
        return 1
    fi

    TMDB_POSTER_URL="${TMDB_IMAGE_BASE}/${TMDB_POSTER_SIZE}${poster_path}"
    _TMDB_IMAGES_CACHE_KEY="$show_id"
    return 0
}

# Fetch season poster URL.
# Sets TMDB_SEASON_POSTER_URL to the full https URL (or empty if none).
tmdb_fetch_season_images() {
    local show_id="$1"
    local season="$2"
    [ -n "$show_id" ] && [ -n "$season" ] || return 1

    local cache_key="${show_id}::${season}"
    if [ "$_TMDB_SEASON_IMAGES_CACHE_KEY" = "$cache_key" ] && [ -n "$TMDB_SEASON_POSTER_URL" ]; then
        return 0
    fi

    TMDB_SEASON_POSTER_URL=""
    _TMDB_SEASON_IMAGES_CACHE_KEY=""

    local response
    response=$(_tmdb_curl "/tv/${show_id}/season/${season}/images") || {
        _tmdb_log "season images API call failed for show ${show_id} season ${season}"
        return 1
    }

    local poster_path
    poster_path=$(printf '%s' "$response" | python3 -c "
import sys, json
data = json.load(sys.stdin)
posters = data.get('posters', [])
def rank(p):
    lang = p.get('iso_639_1') or ''
    if lang == 'en': return 0
    if lang == '':   return 1
    return 2
posters.sort(key=lambda p: (rank(p), -(p.get('vote_average') or 0)))
if posters:
    print(posters[0]['file_path'])
" 2>/dev/null) || return 1

    if [ -z "$poster_path" ]; then
        _tmdb_log "no season poster for show ${show_id} season ${season}"
        return 1
    fi

    TMDB_SEASON_POSTER_URL="${TMDB_IMAGE_BASE}/${TMDB_POSTER_SIZE}${poster_path}"
    _TMDB_SEASON_IMAGES_CACHE_KEY="$cache_key"
    return 0
}

# Download an image to a destination path. Returns 0 on success.
tmdb_download_image() {
    local url="$1"
    local dest="$2"
    [ -n "$url" ] && [ -n "$dest" ] || return 1

    local tmp="${dest}.partial"
    if curl -sf --max-time 30 -o "$tmp" "$url" 2>/dev/null && [ -s "$tmp" ]; then
        mv -f "$tmp" "$dest"
        return 0
    fi
    rm -f "$tmp"
    _tmdb_log "image download failed: $url"
    return 1
}

# Convenience: search show + load season + load images.
# Sets TMDB_SHOW_ID, TMDB_SHOW_NAME, TMDB_EP_NAMES, TMDB_EP_RUNTIMES,
#      TMDB_POSTER_URL, TMDB_SEASON_POSTER_URL.
# Returns 0 if at least the search + season succeed.
tmdb_load_show() {
    local name="$1"
    local season="$2"

    tmdb_search_show "$name" || return 1
    tmdb_fetch_season "$TMDB_SHOW_ID" "$season" || return 1
    # Image lookups are best-effort
    tmdb_fetch_show_images "$TMDB_SHOW_ID" || true
    tmdb_fetch_season_images "$TMDB_SHOW_ID" "$season" || true
    return 0
}
