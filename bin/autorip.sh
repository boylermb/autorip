#!/usr/bin/env bash
# =============================================================================
# autorip.sh - Automatically rip inserted optical media
# https://github.com/boylermb/autorip
# =============================================================================
# Usage: autorip.sh /dev/sr0
#
# Detects the disc type and rips accordingly:
#   - 4K UHD Blu-ray → MakeMKV (LibreDrive) → enqueue (no transcode) → rename
#   - Blu-ray / DVD  → MakeMKV → transcode/enqueue → rename
#   - Audio CD        → abcde  → $OUTPUT_BASE/Audio/Music/
#
# Transcoding strategy:
#   All nodes rip with MakeMKV, then enqueue to .autorip-queue/ on
#   shared NFS.  The transcode-worker on the GPU node picks up jobs,
#   transcodes MPEG-2 → H.265 with hevc_nvenc, and handles renaming.
#
# Naming strategy:
#   TV discs  – Identified by disc title pattern (e.g. FUTURAMA_S2D1).
#               Files placed directly into TV/<Show>/Season XX/ with
#               sequential episode numbers.  No external metadata lookup
#               is needed because DVD TINFO data is unstandardised and
#               tools like mnamer can't reliably match individual episodes.
#   Movies    – Identified by mnamer (MIT) via TMDb.  Falls back to
#               Movies/<disc_title>/ if mnamer can't match.
# =============================================================================

set -euo pipefail

# ---------- Load configuration ----------
AUTORIP_CONF="${AUTORIP_CONF:-/etc/autorip/autorip.conf}"
if [ ! -f "$AUTORIP_CONF" ]; then
    echo "ERROR: Config file not found: $AUTORIP_CONF" >&2
    exit 1
fi
# shellcheck source=/etc/autorip/autorip.conf
source "$AUTORIP_CONF"

DEVICE="${1:?Usage: autorip.sh /dev/srX}"
LOGPREFIX="[autorip $(basename "$DEVICE")]"

# Output directories (derived from config)
MOVIES_DIR="$OUTPUT_BASE/Video/Movies"
TV_DIR="$OUTPUT_BASE/Video/TV"
STAGING_DIR="$OUTPUT_BASE/.autorip-staging"
QUEUE_DIR="$OUTPUT_BASE/.autorip-queue"
LOCK_DIR="/tmp/autorip"
STATUS_DIR="/var/lib/autorip"
HOSTNAME=$(hostname)

# mnamer output format strings (movies only — TV uses direct naming)
MNAMER_MOVIE_DIR="$OUTPUT_BASE/Video/Movies"
MNAMER_MOVIE_FORMAT="${MNAMER_MOVIE_FORMAT:-{name} ({year})/{name} ({year}){extension}}"

# Config defaults
MIN_TITLE_SECONDS="${MIN_TITLE_SECONDS:-0}"
EPISODES_PER_DISC="${EPISODES_PER_DISC:-4}"
UHD_KEEP_ORIGINAL="${UHD_KEEP_ORIGINAL:-yes}"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $LOGPREFIX $*"; }

# Escape a string for safe inclusion in a JSON value (handles \ and ")
json_escape() { printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'; }

# ---------- Dashboard status helpers ----------
update_status() {
    local status="$1"
    local disc_type="${2:-}"
    local title="${3:-}"
    local progress="${4:-}"
    local artist="${5:-}"
    local album="${6:-}"
    local tracks_json="${7:-[]}"
    local tracks_total="${8:-0}"
    local tracks_completed="${9:-0}"
    local current_track="${10:-}"
    local has_art="false"
    if [ -f "$STATUS_DIR/cover-$(basename "$DEVICE").jpg" ]; then
        has_art="true"
    fi
    mkdir -p "$STATUS_DIR"
    # Escape user-supplied strings for safe JSON embedding
    local s_title s_artist s_album s_disc_type s_progress s_current_track
    s_title=$(json_escape "$title")
    s_artist=$(json_escape "$artist")
    s_album=$(json_escape "$album")
    s_disc_type=$(json_escape "$disc_type")
    s_progress=$(json_escape "$progress")
    s_current_track=$(json_escape "$current_track")
    local dev_name
    dev_name=$(basename "$DEVICE")
    local tmpfile
    tmpfile=$(mktemp "$STATUS_DIR/.status.json.XXXXXX")
    chmod 644 "$tmpfile"
    cat > "$tmpfile" <<EOF
{
    "hostname": "${HOSTNAME}",
    "status": "${status}",
    "device": "${dev_name}",
    "disc_type": "${s_disc_type}",
    "title": "${s_title}",
    "progress": "${s_progress}",
    "artist": "${s_artist}",
    "album": "${s_album}",
    "tracks": ${tracks_json},
    "tracks_total": ${tracks_total},
    "tracks_completed": ${tracks_completed},
    "current_track": "${s_current_track}",
    "has_art": ${has_art},
    "updated": "$(date '+%Y-%m-%d %H:%M:%S')"
}
EOF
    # Per-device status file so multi-drive machines show independent cards
    mv -f "$tmpfile" "$STATUS_DIR/status-${dev_name}.json"
    # Backward compat: also update the legacy single-file status
    cp -f "$STATUS_DIR/status-${dev_name}.json" "$STATUS_DIR/status.json"
}

# ---------- Library duplicate check ----------
# Check whether the album already exists in the music library.
# Returns:
#   0 — exact duplicate (same track count) → caller should skip
#   1 — no match, safe to rip
#   2 — partial/different match → existing dir renamed to (old), safe to rip
check_library_duplicate() {
    local artist="$1"
    local album="$2"
    local disc_track_count="$3"
    local format="${CD_FORMAT:-mp3}"

    # Build the same sanitised path the worker will use
    local safe_artist safe_album
    safe_artist=$(echo "$artist" | sed -e 's/^\.*//' | tr -d ':><|*/"'"'"'?\\!')
    safe_album=$(echo "$album" | sed -e 's/^\.*//' | tr -d ':><|*/"'"'"'?\\!')
    local music_dir="$OUTPUT_BASE/Audio/Music"
    local library_dir="$music_dir/$safe_artist/$safe_album"

    # No existing directory — nothing to worry about
    if [ ! -d "$library_dir" ]; then
        return 1
    fi

    # Count audio files in the existing library directory
    local existing_count
    existing_count=$(find "$library_dir" -maxdepth 1 -type f -name "*.${format}" 2>/dev/null | wc -l)

    if [ "$existing_count" -eq "$disc_track_count" ] && [ "$disc_track_count" -gt 0 ]; then
        # Exact match — same number of tracks
        log "Library already contains $artist / $album ($existing_count tracks) — skipping rip"
        return 0
    fi

    # Partial or different — rename existing to (old)
    local old_dir="$music_dir/$safe_artist/${safe_album} (old)"
    # If (old) already exists, remove it to avoid stacking
    if [ -d "$old_dir" ]; then
        rm -rf "$old_dir"
    fi
    mv "$library_dir" "$old_dir"
    log "Renamed existing $artist / $album ($existing_count tracks) to '${safe_album} (old)' — re-ripping"
    return 2
}

# Fetch CD metadata from MusicBrainz via python-discid + musicbrainzngs
fetch_cd_metadata() {
    local device="$1"
    CD_ARTIST=""
    CD_ALBUM=""
    CD_TRACKS_JSON="[]"
    CD_DISC_ID=""
    CD_SUBMISSION_URL=""

    if ! command -v python3 >/dev/null 2>&1; then
        log "python3 not available, skipping metadata lookup"
        return
    fi

    local _meta_tmp
    _meta_tmp=$(mktemp /tmp/autorip-meta.XXXXXX)
    python3 - "$device" "$_meta_tmp" <<'PYEOF' 2>/dev/null || true
import json, sys, os
outfile = sys.argv[2]

disc_id = ""
submission_url = ""
track_count = 0

try:
    import discid
    disc = discid.read(sys.argv[1])
    disc_id = disc.id
    submission_url = disc.submission_url
    track_count = len(disc.tracks)
except Exception as e:
    print('# discid read failed: ' + str(e), file=sys.stderr)

found = False
if disc_id:
    try:
        import musicbrainzngs
        musicbrainzngs.set_useragent('autorip', '1.0', 'https://github.com/boylermb/autorip')
        result = musicbrainzngs.get_releases_by_discid(disc_id, includes=['artists', 'recordings'])
        releases = result.get('disc', {}).get('release-list', [])

        if releases:
            found = True
            release = releases[0]
            artist = release.get('artist-credit-phrase') or 'Unknown Artist'
            # Fallback: build from artist-credit array if phrase is null
            if artist == 'Unknown Artist':
                credits = release.get('artist-credit', [])
                if credits:
                    built = ''.join(
                        c.get('name', c.get('artist', {}).get('name', ''))
                        + c.get('joinphrase', '')
                        for c in credits if isinstance(c, dict)
                    ).strip()
                    if built:
                        artist = built
            album = release.get('title') or 'Unknown Album'
            medium_list = release.get('medium-list', [])
            disc_total = len(medium_list)

            # Find the medium that matches our disc ID
            matched_medium = None
            disc_number = 1
            for medium in medium_list:
                for md in medium.get('disc-list', []):
                    if md.get('id') == disc_id:
                        matched_medium = medium
                        disc_number = int(medium.get('position', 1))
                        break
                if matched_medium:
                    break

            # Fallback: use the first medium if no disc-list match
            if not matched_medium and medium_list:
                matched_medium = medium_list[0]
                disc_number = int(matched_medium.get('position', 1))

            tracks = []
            if matched_medium:
                for track in matched_medium.get('track-list', []):
                    rec = track.get('recording', {})
                    tracks.append(rec.get('title', 'Track ' + track.get('number', '?')))

            # Append disc number to album when multi-disc release
            if disc_total > 1:
                album = album + ' (Disc ' + str(disc_number) + ')'

            meta = {"artist": artist, "album": album, "tracks": tracks,
                    "disc_number": disc_number, "disc_total": disc_total,
                    "disc_id": disc_id, "submission_url": submission_url}
    except Exception as e:
        print('# musicbrainz lookup failed: ' + str(e), file=sys.stderr)

if not found:
    # MusicBrainz didn't match — use disc_id to make a unique album name
    # so multiple unknown discs don't clobber each other in staging.
    short_id = disc_id[:8] if disc_id else hex(int.from_bytes(os.urandom(4)))[2:]
    album = f"Unknown Album ({short_id})"
    # Generate placeholder track names from the disc's track count
    tracks = [f"Track {i+1}" for i in range(track_count)]
    meta = {"artist": "Unknown Artist", "album": album, "tracks": tracks,
            "disc_number": 1, "disc_total": 1,
            "disc_id": disc_id, "submission_url": submission_url}

with open(outfile, 'w') as f:
    json.dump(meta, f)
PYEOF

    if [ -f "$_meta_tmp" ] && [ -s "$_meta_tmp" ]; then
        CD_ARTIST=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['artist'])" "$_meta_tmp" 2>/dev/null || echo "Unknown Artist")
        CD_ALBUM=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['album'])" "$_meta_tmp" 2>/dev/null || echo "Unknown Album")
        CD_TRACKS_JSON=$(python3 -c "import json,sys; print(json.dumps(json.load(open(sys.argv[1]))['tracks']))" "$_meta_tmp" 2>/dev/null || echo "[]")
        CD_DISC_ID=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1])).get('disc_id',''))" "$_meta_tmp" 2>/dev/null || echo "")
        CD_SUBMISSION_URL=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1])).get('submission_url',''))" "$_meta_tmp" 2>/dev/null || echo "")
    fi
    rm -f "$_meta_tmp"

    if [ -n "$CD_DISC_ID" ]; then
        log "Metadata: Artist=$CD_ARTIST, Album=$CD_ALBUM, Disc ID=$CD_DISC_ID"
    else
        log "Metadata: Artist=$CD_ARTIST, Album=$CD_ALBUM (no disc ID available)"
    fi
}

# ---------- Rip log ----------
# Append an entry to the shared rip log on the NAS after every successful rip.
# The log lives at $OUTPUT_BASE/.rip-log.json — a JSON array of objects.
# Both audio and video rips are recorded.
RIP_LOG="$OUTPUT_BASE/.rip-log.json"

log_rip_entry() {
    local disc_type="$1"
    local artist="$2"
    local album="$3"
    local tracks_json="$4"       # JSON array of track names, e.g. ["Run-Around","Hook"]
    local cover_art_path="$5"    # Relative path from $OUTPUT_BASE, or empty

    local s_artist s_album s_cover
    s_artist=$(json_escape "$artist")
    s_album=$(json_escape "$album")
    s_cover=$(json_escape "$cover_art_path")

    local entry
    entry=$(cat <<ENTRY
{
    "timestamp": "$(date -u '+%Y-%m-%dT%H:%M:%SZ')",
    "hostname": "${HOSTNAME}",
    "device": "$(basename "$DEVICE")",
    "disc_type": "${disc_type}",
    "artist": "${s_artist}",
    "album": "${s_album}",
    "tracks": ${tracks_json},
    "cover_art": "${s_cover}"
}
ENTRY
)

    # Atomic append: read existing log, append entry, write back with a tmpfile
    # Uses flock to prevent concurrent writes from multiple nodes
    (
        flock -w 5 201 || { log "WARNING: Could not acquire rip-log lock"; return; }

        existing="[]"
        if [ -f "$RIP_LOG" ]; then
            existing=$(cat "$RIP_LOG" 2>/dev/null || echo "[]")
            # Validate it looks like a JSON array
            if ! echo "$existing" | head -c1 | grep -q '\['; then
                existing="[]"
            fi
        fi

        # Use python3 to safely append to the JSON array
        python3 -c "
import json, sys
try:
    log = json.loads(sys.argv[1])
except Exception:
    log = []
entry = json.loads(sys.argv[2])
log.append(entry)
print(json.dumps(log, indent=2))
" "$existing" "$entry" > "${RIP_LOG}.tmp" && mv -f "${RIP_LOG}.tmp" "$RIP_LOG"

    ) 201>"${RIP_LOG}.lock"

    log "Rip logged: $disc_type — $artist / $album ($(echo "$tracks_json" | python3 -c 'import json,sys; print(len(json.loads(sys.stdin.read())))' 2>/dev/null || echo '?') tracks)"
}

# Fetch album art and save to status directory
fetch_album_art() {
    local artist="$1"
    local album="$2"
    if command -v python3 >/dev/null 2>&1; then
        python3 -c "
import urllib.request, urllib.parse, json, sys
try:
    query = urllib.parse.quote(f'artist:\"{sys.argv[1]}\" AND release:\"{sys.argv[2]}\"')
    url = f'https://musicbrainz.org/ws/2/release/?query={query}&fmt=json&limit=1'
    req = urllib.request.Request(url, headers={'User-Agent': 'autorip/1.0'})
    resp = urllib.request.urlopen(req, timeout=10)
    data = json.loads(resp.read())
    releases = data.get('releases', [])
    if releases:
        mbid = releases[0]['id']
        art_url = f'https://coverartarchive.org/release/{mbid}/front-250'
        urllib.request.urlretrieve(art_url, sys.argv[3])
        print('OK')
except Exception as e:
    print(f'FAIL: {e}', file=sys.stderr)
" "$artist" "$album" "$STATUS_DIR/cover-$(basename "$DEVICE").jpg" 2>/dev/null || true
    fi
}

# ---------- TV disc title parsing ----------
parse_tv_disc_title() {
    local disc_title="$1"

    if echo "$disc_title" | grep -qiE '_S[0-9]+D[0-9]+$'; then
        TV_SHOW=$(echo "$disc_title" | sed -E 's/_[Ss][0-9]+[Dd][0-9]+$//' | tr '_' ' ')
        TV_SHOW=$(echo "$TV_SHOW" | awk '{for(i=1;i<=NF;i++){$i=toupper(substr($i,1,1))tolower(substr($i,2))}}1')
        TV_SEASON=$(echo "$disc_title" | grep -oiE 'S([0-9]+)D' | grep -oE '[0-9]+' | sed 's/^0*//')
        TV_DISC=$(echo "$disc_title" | grep -oiE 'D([0-9]+)$' | grep -oE '[0-9]+' | sed 's/^0*//')
        log "TV disc detected: show='$TV_SHOW' season=$TV_SEASON disc=$TV_DISC"
        return 0
    fi

    if echo "$disc_title" | grep -qiE '_SEASON_[0-9]+_DISC_[0-9]+$'; then
        TV_SHOW=$(echo "$disc_title" | sed -E 's/_[Ss][Ee][Aa][Ss][Oo][Nn]_[0-9]+_[Dd][Ii][Ss][Cc]_[0-9]+$//' | tr '_' ' ')
        TV_SHOW=$(echo "$TV_SHOW" | awk '{for(i=1;i<=NF;i++){$i=toupper(substr($i,1,1))tolower(substr($i,2))}}1')
        TV_SEASON=$(echo "$disc_title" | grep -oiE 'SEASON_([0-9]+)' | grep -oE '[0-9]+' | sed 's/^0*//')
        TV_DISC=$(echo "$disc_title" | grep -oiE 'DISC_([0-9]+)' | grep -oE '[0-9]+' | sed 's/^0*//')
        log "TV disc detected (alt format): show='$TV_SHOW' season=$TV_SEASON disc=$TV_DISC"
        return 0
    fi

    return 1
}

# ---------- TV episode naming ----------
tv_rename() {
    local src_dir="$1"
    local show="$TV_SHOW"
    local season="$TV_SEASON"
    local disc="$TV_DISC"

    local season_dir
    season_dir=$(printf "Season %02d" "$season")
    local dest_dir="$TV_DIR/$show/$season_dir"
    mkdir -p "$dest_dir"

    local ep_start=$(( (disc - 1) * EPISODES_PER_DISC + 1 ))
    local ep_num=$ep_start

    local count=0
    for mkv in $(find "$src_dir" -maxdepth 1 -name "*.mkv" | sort); do
        [ -f "$mkv" ] || continue
        local ep_name
        ep_name=$(printf "%s - S%02dE%02d.mkv" "$show" "$season" "$ep_num")
        mv -f "$mkv" "$dest_dir/$ep_name"
        log "Renamed $(basename "$mkv") → $ep_name"
        ep_num=$((ep_num + 1))
        count=$((count + 1))
    done

    log "Placed $count episode(s) in $dest_dir (starting at E$(printf '%02d' "$ep_start"))"
    rmdir "$src_dir" 2>/dev/null || true
}

# ---------- mnamer rename helper (movies only) ----------
mnamer_rename() {
    local src_dir="$1"
    local disc_title="$2"
    local disc_type="$3"

    if parse_tv_disc_title "$disc_title"; then
        log "Using direct TV naming for $disc_title (mnamer skipped — DVD/Blu-ray episode matching is unreliable)"
        tv_rename "$src_dir"
        return
    fi

    if ! command -v mnamer >/dev/null 2>&1; then
        log "mnamer not installed — falling back to manual sort"
        movie_fallback "$src_dir" "$disc_title"
        return
    fi

    log "Running mnamer to identify and rename movie files..."
    update_status "ripping" "$disc_type" "$disc_title" "Identifying media..." "" "$disc_title" "[]"

    if mnamer --batch \
        --media=movie \
        --movie-api=tmdb \
        --movie-directory="$MNAMER_MOVIE_DIR" \
        --movie-format="$MNAMER_MOVIE_FORMAT" \
        "$src_dir"/*.mkv 2>&1 | tee -a /var/log/autorip/mnamer.log; then
        remaining=$(find "$src_dir" -name "*.mkv" 2>/dev/null | wc -l)
        if [ "$remaining" -eq 0 ]; then
            log "mnamer matched all files as Movie content"
        else
            log "mnamer matched some files, $remaining remain unmatched"
        fi
    fi

    remaining=$(find "$src_dir" -name "*.mkv" 2>/dev/null | wc -l)
    if [ "$remaining" -gt 0 ]; then
        log "WARNING: mnamer could not identify $remaining file(s), falling back to manual sort"
        movie_fallback "$src_dir" "$disc_title"
    fi

    rmdir "$src_dir" 2>/dev/null || true
}

# Fallback: move unidentified movie files into Movies/<disc_title>/
movie_fallback() {
    local src_dir="$1"
    local disc_title="$2"
    local fallback_dir="$MOVIES_DIR/$disc_title"

    mkdir -p "$fallback_dir"
    find "$src_dir" -name "*.mkv" -exec mv -f {} "$fallback_dir/" \;
    log "Moved unidentified files to $fallback_dir"
}

# ---------- Enqueue video disc job for GPU worker ----------
# Creates a single job per disc with a files[] array listing all ripped MKV
# titles.  The transcode worker processes each file and creates one .review
# job for the whole disc, avoiding the staging-dir-deleted-under-siblings bug.
enqueue_video_disc() {
    local disc_title="$1"
    local disc_type="$2"
    local is_uhd="${3:-false}"
    local staging_dir="$4"
    shift 4
    # Remaining args are "title_index:file_path" pairs
    local -a file_entries=("$@")

    mkdir -p "$QUEUE_DIR"
    local job_file="$QUEUE_DIR/${HOSTNAME}_${disc_title}_$(date +%s).json"
    local tmpfile
    tmpfile=$(mktemp "$QUEUE_DIR/.job.XXXXXX")
    chmod 644 "$tmpfile"

    local files_json="["
    local first=true
    local title_count=${#file_entries[@]}
    for entry in "${file_entries[@]}"; do
        local tidx="${entry%%:*}"
        local fpath="${entry#*:}"
        $first || files_json+=","
        first=false
        files_json+="
        {\"title_index\": $tidx, \"file_path\": \"$fpath\"}"
    done
    files_json+="
    ]"

    cat > "$tmpfile" <<ENDJOB
{
    "disc_title": "$(printf '%s' "$disc_title" | sed 's/\\/\\\\/g; s/"/\\"/g')",
    "disc_type": "$disc_type",
    "is_uhd": $is_uhd,
    "staging_dir": "$staging_dir",
    "title_count": $title_count,
    "files": $files_json,
    "source_host": "$HOSTNAME",
    "submitted": "$(date '+%Y-%m-%d %H:%M:%S')"
}
ENDJOB
    mv -f "$tmpfile" "$job_file"
    log "Queued disc: $(basename "$job_file") ($title_count title(s))"
}

# ---------- Enqueue audio CD post-processing job ----------
enqueue_audio_job() {
    local artist="$1"
    local album="$2"
    local tracks_json="$3"       # JSON array of track names
    local staging_dir="$4"       # abcde staging output dir

    mkdir -p "$QUEUE_DIR"
    local safe_album
    safe_album=$(echo "$album" | tr ' /:' '_' | tr -d '"')
    local job_file="$QUEUE_DIR/${HOSTNAME}_audiocd_${safe_album}_$(date +%s).json"
    local tmpfile
    tmpfile=$(mktemp "$QUEUE_DIR/.job.XXXXXX")
    chmod 644 "$tmpfile"

    # Copy cover art into staging dir if we fetched one
    if [ -f "$STATUS_DIR/cover-$(basename "$DEVICE").jpg" ] && [ -d "$staging_dir" ]; then
        cp -f "$STATUS_DIR/cover-$(basename "$DEVICE").jpg" "$staging_dir/cover.jpg"
    fi

    local s_artist s_album s_disc_id s_submission_url
    s_artist=$(json_escape "$artist")
    s_album=$(json_escape "$album")
    s_disc_id=$(json_escape "${CD_DISC_ID:-}")
    s_submission_url=$(json_escape "${CD_SUBMISSION_URL:-}")

    cat > "$tmpfile" <<ENDJOB
{
    "job_type": "audio-cd",
    "artist": "${s_artist}",
    "album": "${s_album}",
    "tracks": ${tracks_json},
    "staging_dir": "${staging_dir}",
    "format": "${CD_FORMAT:-mp3}",
    "source_host": "$HOSTNAME",
    "submitted": "$(date '+%Y-%m-%d %H:%M:%S')",
    "disc_id": "${s_disc_id}",
    "submission_url": "${s_submission_url}"
}
ENDJOB
    mv -f "$tmpfile" "$job_file"
    log "Queued audio CD job: $(basename "$job_file")"
}

# ---------- Lock to prevent concurrent rips on same drive ----------
mkdir -p "$LOCK_DIR"
LOCKFILE="$LOCK_DIR/$(basename "$DEVICE").lock"
exec 200>"$LOCKFILE"
if ! flock -n 200; then
    log "Another rip is already running on $DEVICE, exiting."
    exit 0
fi

# ---------- Cooldown to prevent re-trigger after eject ----------
# Some drives auto-close the tray after eject, which fires a new udev event
# and re-triggers this script.  Ignore events within COOLDOWN_SECONDS of a
# previous successful rip.
COOLDOWN_SECONDS=60
COOLDOWN_FILE="$LOCK_DIR/$(basename "$DEVICE").cooldown"
if [ -f "$COOLDOWN_FILE" ]; then
    last_rip=$(cat "$COOLDOWN_FILE" 2>/dev/null || echo 0)
    now=$(date +%s)
    elapsed=$(( now - last_rip ))
    if [ "$elapsed" -lt "$COOLDOWN_SECONDS" ]; then
        log "Cooldown active (${elapsed}s < ${COOLDOWN_SECONDS}s since last rip) — ignoring re-trigger."
        exit 0
    fi
fi

# ---------- Wait for drive to become ready ----------
DRIVE_READY_TIMEOUT=90
DRIVE_READY_INTERVAL=3

wait_for_drive() {
    local elapsed=0
    log "Waiting for $DEVICE to become ready (timeout ${DRIVE_READY_TIMEOUT}s)..."
    while [ "$elapsed" -lt "$DRIVE_READY_TIMEOUT" ]; do
        local props
        props=$(udevadm info --query=property --name="$DEVICE" 2>/dev/null || true)
        if echo "$props" | grep -q "ID_FS_TYPE="; then
            log "Drive $DEVICE is ready (filesystem detected) after ${elapsed}s"
            return 0
        elif echo "$props" | grep -q "ID_CDROM_MEDIA_TRACK_COUNT_AUDIO="; then
            log "Drive $DEVICE is ready (audio CD detected) after ${elapsed}s"
            return 0
        fi
        sleep "$DRIVE_READY_INTERVAL"
        elapsed=$((elapsed + DRIVE_READY_INTERVAL))
    done
    log "ERROR: Drive $DEVICE did not become ready within ${DRIVE_READY_TIMEOUT}s"
    return 1
}

if ! wait_for_drive; then
    update_status "error" "" "" "Drive not ready"
    exit 1
fi

# Clean up old cover art from previous rip
rm -f "$STATUS_DIR/cover-$(basename "$DEVICE").jpg"

# ---------- Detect disc type ----------
disc_info=$(udevadm info --query=property --name="$DEVICE" 2>/dev/null || true)

is_bluray=false
is_dvd=false
is_audio_cd=false

if echo "$disc_info" | grep -q "ID_CDROM_MEDIA_BD=1"; then
    is_bluray=true
    # UHD Blu-rays also report ID_CDROM_MEDIA_BD=1.  We distinguish them
    # later using MakeMKV's disc info (AACS v2 / 4K resolution) once we
    # have the disc title scan results.
    log "Detected: Blu-ray disc (checking for UHD...)"
    update_status "ripping" "Blu-ray" "" "Detecting..."
elif echo "$disc_info" | grep -q "ID_CDROM_MEDIA_DVD=1"; then
    is_dvd=true
    log "Detected: DVD disc"
    update_status "ripping" "DVD" "" "Detecting..."
elif echo "$disc_info" | grep -q "ID_CDROM_MEDIA_CD=1"; then
    if echo "$disc_info" | grep -q "ID_CDROM_MEDIA_TRACK_COUNT_AUDIO"; then
        is_audio_cd=true
        log "Detected: Audio CD"
        update_status "ripping" "Audio CD" "" "Detecting..."
    else
        log "Detected: Data CD - skipping (not a media disc)"
        eject "$DEVICE" 2>/dev/null || true
        exit 0
    fi
else
    log "Unknown or empty disc type, skipping."
    exit 0
fi

# ---------- Helper: rip video disc (Blu-ray or DVD) ----------
rip_video_disc() {
    local disc_type="$1"
    local fallback_title="Unknown_${disc_type}_$(date +%Y%m%d_%H%M%S)"

    log "Starting $disc_type rip..."

    # Single MakeMKV info pass — captures disc title, UHD markers, and title list.
    # Robot-mode outputs CINFO/TINFO/TCOUNT on stdout; stderr is suppressed.
    log "Scanning disc for titles (minlength=${MIN_TITLE_SECONDS}s)..."
    update_status "ripping" "$disc_type" "" "Scanning titles..." "" "" "[]"
    MAKEMKV_INFO=$(makemkvcon -r info dev:"$DEVICE" --minlength="$MIN_TITLE_SECONDS" 2>/dev/null || true)

    # --- Disc title ---
    # CINFO:32 = volume label (e.g. "APOLLO13_UHD_UPK1") — safe for folder names.
    # Fall back to CINFO:2 (human-friendly name) if 32 is missing.
    DISC_TITLE=$(echo "$MAKEMKV_INFO" | grep '^CINFO:32,' | head -1 | sed 's/^CINFO:32,[^,]*,//; s/^"//; s/"$//' | tr ' ' '_' || echo "")
    if [ -z "$DISC_TITLE" ]; then
        DISC_TITLE=$(echo "$MAKEMKV_INFO" | grep '^CINFO:2,' | head -1 | sed 's/^CINFO:2,[^,]*,//; s/^"//; s/"$//' | tr ' ' '_' || echo "")
    fi
    if [ -z "$DISC_TITLE" ]; then
        DISC_TITLE="$fallback_title"
        log "WARNING: Could not determine disc title, using $DISC_TITLE"
    fi
    log "Disc title: $DISC_TITLE"

    # ---------- UHD Blu-ray detection ----------
    # MakeMKV reports "AACS2" or "BDMV 4K" for UHD discs.  Check the info
    # output for indicators:
    #   - CINFO with "Ultra HD" or "UHD" in the disc label
    #   - AACS v2 (AACS 2.0) encryption markers
    #   - 3840x2160 resolution in title info
    local is_uhd=false
    if [ "$disc_type" = "Blu-ray" ]; then
        if echo "$MAKEMKV_INFO" | grep -qiE 'Ultra.?HD|UHD|AACS2|AACS v2|3840x2160'; then
            is_uhd=true
            disc_type="UHD Blu-ray"
            log "Detected: 4K UHD Blu-ray (AACS v2 / LibreDrive)"
            update_status "ripping" "UHD Blu-ray" "$DISC_TITLE" "Scanning titles..." "" "$DISC_TITLE" "[]"
        else
            log "Detected: Standard Blu-ray"
        fi
    fi

    update_status "ripping" "$disc_type" "$DISC_TITLE" "Scanning titles..." "" "$DISC_TITLE" "[]"

    # --- Title list ---
    # Parse TINFO lines — each TINFO:N,... line represents a title that passed
    # the --minlength filter.  Extract unique title indices (field before first comma).
    TITLE_IDS=()
    while IFS= read -r tid; do
        TITLE_IDS+=( "$tid" )
    done < <(echo "$MAKEMKV_INFO" | grep '^TINFO:' | cut -d',' -f1 | sed 's/^TINFO://' | sort -un)

    TITLE_COUNT=${#TITLE_IDS[@]}
    if [ "$TITLE_COUNT" -eq 0 ]; then
        log "ERROR: No titles found on $disc_type disc"
        update_status "error" "$disc_type" "$DISC_TITLE" "No titles found" "" "$DISC_TITLE" "[]"
        eject "$DEVICE" 2>/dev/null || true
        exit 1
    fi

    log "Found $TITLE_COUNT title(s) to rip: ${TITLE_IDS[*]}"

    OUTPUT_DIR="$STAGING_DIR/$DISC_TITLE"
    mkdir -p "$OUTPUT_DIR"

    # Collect title_index:file_path entries for the single disc job
    local -a ripped_files=()
    CURRENT=0
    for tid in "${TITLE_IDS[@]}"; do
        CURRENT=$((CURRENT + 1))
        log "Ripping title $tid ($CURRENT/$TITLE_COUNT)..."
        update_status "ripping" "$disc_type" "$DISC_TITLE" "Ripping title $CURRENT/$TITLE_COUNT..." "" "$DISC_TITLE" "[]"

        if makemkvcon mkv dev:"$DEVICE" "$tid" "$OUTPUT_DIR" \
            --minlength="$MIN_TITLE_SECONDS" \
            --noscan \
            --progress=-stdout 2>&1; then

            mkv_file=$(find "$OUTPUT_DIR" -maxdepth 1 -name "*_t$(printf '%02d' "$tid").mkv" -newer "$LOCKFILE" 2>/dev/null | head -1)
            if [ -z "$mkv_file" ]; then
                mkv_file=$(find "$OUTPUT_DIR" -maxdepth 1 -name "*.mkv" -newer "$LOCKFILE" 2>/dev/null | sort -t/ -k2 | tail -1)
            fi

            if [ -n "$mkv_file" ] && [ -f "$mkv_file" ]; then
                if ! ffprobe -loglevel error -select_streams a -show_entries stream=codec_type -of csv=p=0 "$mkv_file" 2>/dev/null | grep -q audio; then
                    log "WARNING: $(basename "$mkv_file") has no audio streams!"
                fi

                log "Title $tid ripped: $(basename "$mkv_file")"
                ripped_files+=("${CURRENT}:${mkv_file}")
                touch "$LOCKFILE"
            else
                log "WARNING: Could not find MKV file for title $tid"
            fi
        else
            log "WARNING: MakeMKV failed for title $tid, continuing with remaining titles"
        fi
    done

    if [ ${#ripped_files[@]} -gt 0 ]; then
        enqueue_video_disc "$DISC_TITLE" "$disc_type" "$is_uhd" "$OUTPUT_DIR" "${ripped_files[@]}"
    else
        log "WARNING: No titles ripped successfully"
    fi

    update_status "complete" "$disc_type" "$DISC_TITLE" "${#ripped_files[@]} title(s) queued for transcode" "" "$DISC_TITLE" "[]"

    # Record this rip in the shared log
    local video_tracks_json
    video_tracks_json=$(printf '%s\n' "${TITLE_IDS[@]}" | python3 -c "
import sys, json
titles = [f'Title {line.strip()}' for line in sys.stdin if line.strip()]
print(json.dumps(titles))
" 2>/dev/null || echo '[]')
    log_rip_entry "$disc_type" "" "$DISC_TITLE" "$video_tracks_json" ""
}

# ---------- Rip Blu-ray ----------
if $is_bluray; then
    rip_video_disc "Blu-ray"
fi

# ---------- Rip DVD ----------
if $is_dvd; then
    rip_video_disc "DVD"
fi

# ---------- Rip Audio CD ----------
if $is_audio_cd; then
    log "Starting Audio CD rip..."
    update_status "ripping" "Audio CD" "" "Looking up disc..."

    fetch_cd_metadata "$DEVICE"
    fetch_album_art "$CD_ARTIST" "$CD_ALBUM"

    # Count tracks from metadata for duplicate check
    cd_track_count=0
    if [ "$CD_TRACKS_JSON" != "[]" ]; then
        cd_track_count=$(python3 -c "import json,sys; print(len(json.loads(sys.argv[1])))" "$CD_TRACKS_JSON" 2>/dev/null || true)
        cd_track_count="${cd_track_count:-0}"
        # Ensure it's a plain integer
        if ! [[ "$cd_track_count" =~ ^[0-9]+$ ]]; then
            cd_track_count=0
        fi
    fi

    # Check if this album already exists in the library
    if [ "$CD_ARTIST" != "Unknown Artist" ] && [ "$CD_ALBUM" != "Unknown Album" ]; then
        dup_result=0
        check_library_duplicate "$CD_ARTIST" "$CD_ALBUM" "$cd_track_count" || dup_result=$?
        if [ "$dup_result" -eq 0 ]; then
            # Exact duplicate — skip rip, eject disc
            update_status "complete" "Audio CD" "$CD_ALBUM" "Already in library — skipped" "$CD_ARTIST" "$CD_ALBUM" "$CD_TRACKS_JSON"
            log "Ejecting disc (duplicate)..."
            sleep 2
            eject "$DEVICE" 2>/dev/null || true
            date +%s > "$COOLDOWN_FILE"
            update_status "idle"
            log "Done (skipped duplicate)."
            exit 0
        fi
        # dup_result 1 = no match, 2 = renamed old → continue ripping either way
    fi

    update_status "ripping" "Audio CD" "$CD_ALBUM" "Ripping track 1/$cd_track_count..." "$CD_ARTIST" "$CD_ALBUM" "$CD_TRACKS_JSON" "$cd_track_count" "0" ""

    # ---------- Isolate abcde output to a per-rip staging directory ----------
    # abcde does its own MusicBrainz lookup for directory naming, which may
    # disagree with our fetch_cd_metadata result.  For unidentified discs
    # it writes to a generic "Unknown Artist/Unknown Album/" path.  If two
    # nodes rip unknown CDs at the same time, both would write to the same
    # NFS-shared directory and clobber each other.
    #
    # Fix: give each rip its own private OUTPUTDIR via a temporary config
    # overlay.  After the rip completes we locate the Artist/Album dir
    # inside it and move it to the canonical staging path (which includes
    # the disc-id for uniqueness).
    RIP_STAGING=$(mktemp -d "$STAGING_DIR/.rip-XXXXXX")
    log "Per-rip staging directory: $RIP_STAGING"

    # Create a per-rip config overlay that sources the real config then
    # overrides OUTPUTDIR.  abcde has no CLI flag for output directory.
    RIP_CONF=$(mktemp /tmp/autorip-abcde-conf.XXXXXX)
    cat > "$RIP_CONF" <<ENDCONF
. /etc/abcde.conf
OUTPUTDIR="$RIP_STAGING"
ENDCONF

    # Run abcde in the background and monitor its output for per-track progress.
    # -n = no CDDB/MusicBrainz lookup (we already did our own in fetch_cd_metadata;
    #       letting abcde retry is redundant and can fail on SSL errors).
    ABCDE_LOG=$(mktemp /tmp/autorip-abcde.XXXXXX)
    abcde -d "$DEVICE" -N -n -c "$RIP_CONF" > >(tee "$ABCDE_LOG" >&2) 2>&1 &
    ABCDE_PID=$!

    # Monitor abcde output for track progress
    tracks_seen=0
    current_track_name=""
    while kill -0 "$ABCDE_PID" 2>/dev/null; do
        # Parse the latest "Grabbing track N:" line from abcde output
        latest_grab=$(grep -oP 'Grabbing track \d+: \K.*(?=\.\.\.)' "$ABCDE_LOG" 2>/dev/null | tail -1 || true)
        new_count=$(grep -c 'Grabbing track' "$ABCDE_LOG" 2>/dev/null || true)
        new_count="${new_count:-0}"
        if ! [[ "$new_count" =~ ^[0-9]+$ ]]; then new_count=0; fi
        # Tracks completed = tracks started minus the one currently in progress
        completed=$((new_count > 0 ? new_count - 1 : 0))

        if [ "$new_count" -ne "$tracks_seen" ] || [ "$latest_grab" != "$current_track_name" ]; then
            tracks_seen=$new_count
            current_track_name="$latest_grab"
            update_status "ripping" "Audio CD" "$CD_ALBUM" \
                "Ripping track $new_count/$cd_track_count..." \
                "$CD_ARTIST" "$CD_ALBUM" "$CD_TRACKS_JSON" \
                "$cd_track_count" "$completed" "$current_track_name"
        fi
        sleep 3
    done

    # abcde finished — get its exit code
    wait "$ABCDE_PID"
    abcde_rc=$?
    rm -f "$ABCDE_LOG" "$RIP_CONF"

    if [ "$abcde_rc" -eq 0 ]; then
        update_status "ripping" "Audio CD" "$CD_ALBUM" \
            "Ripping complete ($cd_track_count/$cd_track_count)" \
            "$CD_ARTIST" "$CD_ALBUM" "$CD_TRACKS_JSON" \
            "$cd_track_count" "$cd_track_count" ""
        log "Audio CD rip complete"

        # Locate the Artist/Album directory abcde created inside our
        # isolated per-rip staging dir and move it to the canonical path.
        artist_dir=$(echo "$CD_ARTIST" | sed -e 's/^\.*//' | tr -d ':><|*/"'"'"'?\\!')
        album_dir=$(echo "$CD_ALBUM" | sed -e 's/^\.*//' | tr -d ':><|*/"'"'"'?\\!')

        # abcde creates Artist/Album/ inside RIP_STAGING.  Find the deepest
        # directory — our expected path first, then whatever abcde created.
        staging_album="$RIP_STAGING/$artist_dir/$album_dir"
        if [ ! -d "$staging_album" ]; then
            staging_album=$(find "$RIP_STAGING" -mindepth 2 -maxdepth 2 -type d 2>/dev/null | head -1)
        fi

        if [ -d "$staging_album" ]; then
            # Move to the canonical staging path: STAGING_DIR/Artist/Album/
            # The album_dir already contains the disc-id suffix for unknown
            # discs (e.g. "Unknown Album (a3f7c1b2)") so the path is unique.
            canonical_dir="$STAGING_DIR/$artist_dir/$album_dir"
            mkdir -p "$STAGING_DIR/$artist_dir"
            # Remove any leftover from a previous failed rip
            if [ -d "$canonical_dir" ]; then
                rm -rf "$canonical_dir"
            fi
            mv "$staging_album" "$canonical_dir"
            log "Moved rip output to canonical staging: $canonical_dir"
            staging_album="$canonical_dir"

            # Clean up the now-empty per-rip directory
            rm -rf "$RIP_STAGING"

            # Enqueue post-processing job
            enqueue_audio_job "$CD_ARTIST" "$CD_ALBUM" "$CD_TRACKS_JSON" "$staging_album"
            update_status "complete" "Audio CD" "$CD_ALBUM" "Queued for post-processing" "$CD_ARTIST" "$CD_ALBUM" "$CD_TRACKS_JSON"
        else
            log "ERROR: Could not find staging directory inside $RIP_STAGING for $CD_ARTIST / $CD_ALBUM"
            rm -rf "$RIP_STAGING"
            update_status "error" "Audio CD" "$CD_ALBUM" "Staging dir not found" "$CD_ARTIST" "$CD_ALBUM" "$CD_TRACKS_JSON"
            exit 1
        fi
    else
        log "ERROR: abcde failed for Audio CD rip"
        rm -rf "$RIP_STAGING"
        update_status "error" "Audio CD" "$CD_ALBUM" "abcde failed" "$CD_ARTIST" "$CD_ALBUM" "$CD_TRACKS_JSON"
        exit 1
    fi
fi

# ---------- Eject disc when done ----------
log "Ejecting disc..."
sleep 2
eject "$DEVICE" 2>/dev/null || true
date +%s > "$COOLDOWN_FILE"
update_status "idle"
log "Done."
