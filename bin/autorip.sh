#!/usr/bin/env bash
# =============================================================================
# autorip.sh - Automatically rip inserted optical media
# https://github.com/boylermb/autorip
# =============================================================================
# Usage: autorip.sh /dev/sr0
#
# Detects the disc type and rips accordingly:
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
MIN_TITLE_SECONDS="${MIN_TITLE_SECONDS:-120}"
EPISODES_PER_DISC="${EPISODES_PER_DISC:-4}"

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
    local has_art="false"
    if [ -f "$STATUS_DIR/cover.jpg" ]; then
        has_art="true"
    fi
    mkdir -p "$STATUS_DIR"
    # Escape user-supplied strings for safe JSON embedding
    local s_title s_artist s_album s_disc_type s_progress
    s_title=$(json_escape "$title")
    s_artist=$(json_escape "$artist")
    s_album=$(json_escape "$album")
    s_disc_type=$(json_escape "$disc_type")
    s_progress=$(json_escape "$progress")
    local tmpfile
    tmpfile=$(mktemp "$STATUS_DIR/.status.json.XXXXXX")
    chmod 644 "$tmpfile"
    cat > "$tmpfile" <<EOF
{
    "hostname": "${HOSTNAME}",
    "status": "${status}",
    "device": "$(basename "$DEVICE")",
    "disc_type": "${s_disc_type}",
    "title": "${s_title}",
    "progress": "${s_progress}",
    "artist": "${s_artist}",
    "album": "${s_album}",
    "tracks": ${tracks_json},
    "has_art": ${has_art},
    "updated": "$(date '+%Y-%m-%d %H:%M:%S')"
}
EOF
    mv -f "$tmpfile" "$STATUS_DIR/status.json"
}

# Fetch CD metadata from MusicBrainz via cd-discid and query
fetch_cd_metadata() {
    local device="$1"
    CD_ARTIST=""
    CD_ALBUM=""
    CD_TRACKS_JSON="[]"

    local discid
    discid=$(cd-discid "$device" 2>/dev/null || true)
    if [ -z "$discid" ]; then
        log "Could not read disc ID"
        return
    fi

    local tmpdir
    tmpdir=$(mktemp -d /tmp/abcde-meta.XXXXXX)

    local cddb_data
    cddb_data=$(cddbcmd query $discid 2>/dev/null || true)

    local track_count
    track_count=$(echo "$discid" | awk '{print $2}')

    if command -v python3 >/dev/null 2>&1; then
        eval "$(python3 -c "
import subprocess, json, re, sys
try:
    result = subprocess.run(['cd-discid', '$device'], capture_output=True, text=True, timeout=10)
    parts = result.stdout.strip().split()
    disc_id = parts[0] if parts else ''
    track_count = int(parts[1]) if len(parts) > 1 else 0

    import urllib.request, urllib.error
    url = f'https://musicbrainz.org/ws/2/discid/{disc_id}?fmt=json'
    req = urllib.request.Request(url, headers={'User-Agent': 'autorip/1.0'})
    resp = urllib.request.urlopen(req, timeout=10)
    data = json.loads(resp.read())

    releases = data.get('releases', [])
    if releases:
        release = releases[0]
        artist = release.get('artist-credit', [{}])[0].get('name', 'Unknown Artist')
        album = release.get('title', 'Unknown Album')
        tracks = []
        for medium in release.get('media', []):
            for track in medium.get('tracks', []):
                tracks.append(track.get('title', f'Track {track.get(\"number\", \"?\")}'))
        print(f'CD_ARTIST=\"{artist}\"')
        print(f'CD_ALBUM=\"{album}\"')
        tracks_escaped = json.dumps(tracks)
        print(f'CD_TRACKS_JSON={chr(39)}{tracks_escaped}{chr(39)}')
    else:
        print('CD_ARTIST=\"Unknown Artist\"')
        print('CD_ALBUM=\"Unknown Album\"')
        print('CD_TRACKS_JSON=\"[]\"')
except Exception as e:
    print(f'# metadata lookup failed: {e}', file=sys.stderr)
    print('CD_ARTIST=\"Unknown Artist\"')
    print('CD_ALBUM=\"Unknown Album\"')
    print('CD_TRACKS_JSON=\"[]\"')
" 2>/dev/null)"
    fi

    rm -rf "$tmpdir"
    log "Metadata: Artist=$CD_ARTIST, Album=$CD_ALBUM"
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
" "$artist" "$album" "$STATUS_DIR/cover.jpg" 2>/dev/null || true
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

    # Enqueue OCR-based identification for each unidentified file
    if [ -x /usr/local/bin/enqueue-identify.sh ]; then
        for mkv in "$fallback_dir"/*.mkv; do
            [ -f "$mkv" ] || continue
            log "Enqueueing OCR identification job for $(basename "$mkv")"
            /usr/local/bin/enqueue-identify.sh "$mkv" || \
                log "WARNING: Failed to enqueue OCR identification job for $(basename "$mkv")"
        done
    else
        log "enqueue-identify.sh not available, skipping OCR identification"
    fi
}

# ---------- Enqueue transcode job for GPU worker ----------
enqueue_transcode() {
    local disc_title="$1"
    local file_path="$2"
    local disc_type="$3"
    local title_index="$4"
    local title_count="$5"

    mkdir -p "$QUEUE_DIR"
    local job_file="$QUEUE_DIR/${HOSTNAME}_${disc_title}_t$(printf '%02d' "$title_index")_$(date +%s).json"
    local tmpfile
    tmpfile=$(mktemp "$QUEUE_DIR/.job.XXXXXX")
    chmod 644 "$tmpfile"
    cat > "$tmpfile" <<ENDJOB
{
    "disc_title": "$(printf '%s' "$disc_title" | sed 's/\\/\\\\/g; s/"/\\"/g')",
    "file_path": "$file_path",
    "disc_type": "$disc_type",
    "title_index": $title_index,
    "title_count": $title_count,
    "source_host": "$HOSTNAME",
    "submitted": "$(date '+%Y-%m-%d %H:%M:%S')"
}
ENDJOB
    mv -f "$tmpfile" "$job_file"
    log "Queued: $(basename "$job_file")"
}

# ---------- Lock to prevent concurrent rips on same drive ----------
mkdir -p "$LOCK_DIR"
LOCKFILE="$LOCK_DIR/$(basename "$DEVICE").lock"
exec 200>"$LOCKFILE"
if ! flock -n 200; then
    log "Another rip is already running on $DEVICE, exiting."
    exit 0
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
rm -f "$STATUS_DIR/cover.jpg"

# ---------- Detect disc type ----------
disc_info=$(udevadm info --query=property --name="$DEVICE" 2>/dev/null || true)

is_bluray=false
is_dvd=false
is_audio_cd=false

if echo "$disc_info" | grep -q "ID_CDROM_MEDIA_BD=1"; then
    is_bluray=true
    log "Detected: Blu-ray disc"
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

    DISC_TITLE=$(makemkvcon -r info dev:"$DEVICE" 2>/dev/null | grep "^DRV:0" | cut -d',' -f6 | tr -d '"' | tr ' ' '_' || echo "")
    if [ -z "$DISC_TITLE" ]; then
        DISC_TITLE="$fallback_title"
        log "WARNING: Could not determine disc title, using $DISC_TITLE"
    fi

    log "Scanning disc for titles (minlength=${MIN_TITLE_SECONDS}s)..."
    update_status "ripping" "$disc_type" "$DISC_TITLE" "Scanning titles..." "" "$DISC_TITLE" "[]"

    TITLE_IDS=()
    while IFS= read -r line; do
        title_1idx=$(echo "$line" | grep -oP 'Title #\K[0-9]+')
        if [ -n "$title_1idx" ]; then
            TITLE_IDS+=( $((title_1idx - 1)) )
        fi
    done < <(makemkvcon -r info dev:"$DEVICE" --minlength="$MIN_TITLE_SECONDS" 2>&1 | grep "^MSG:3028")

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

                log "Title $tid ripped: $(basename "$mkv_file") — enqueueing for transcode"
                enqueue_transcode "$DISC_TITLE" "$mkv_file" "$disc_type" "$CURRENT" "$TITLE_COUNT"
                touch "$LOCKFILE"
            else
                log "WARNING: Could not find MKV file for title $tid"
            fi
        else
            log "WARNING: MakeMKV failed for title $tid, continuing with remaining titles"
        fi
    done

    update_status "complete" "$disc_type" "$DISC_TITLE" "$TITLE_COUNT title(s) queued for transcode" "" "$DISC_TITLE" "[]"
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
    update_status "ripping" "Audio CD" "$CD_ALBUM" "Ripping..." "$CD_ARTIST" "$CD_ALBUM" "$CD_TRACKS_JSON"

    if abcde -d "$DEVICE" -N -c /etc/abcde.conf 2>&1; then
        log "Audio CD rip complete"
        update_status "complete" "Audio CD" "$CD_ALBUM" "Done" "$CD_ARTIST" "$CD_ALBUM" "$CD_TRACKS_JSON"
    else
        log "ERROR: abcde failed for Audio CD rip"
        update_status "error" "Audio CD" "$CD_ALBUM" "abcde failed" "$CD_ARTIST" "$CD_ALBUM" "$CD_TRACKS_JSON"
        exit 1
    fi
fi

# ---------- Eject disc when done ----------
log "Ejecting disc..."
sleep 2
eject "$DEVICE" 2>/dev/null || true
update_status "idle"
log "Done."
