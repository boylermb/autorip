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

# ---------- Load helper libraries ----------
# Look in install location first, then alongside this script (dev mode).
_SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
for _libdir in /usr/local/lib/autorip "$_SELF_DIR/lib"; do
    if [ -f "$_libdir/tmdb.sh" ]; then
        # shellcheck source=lib/tmdb.sh
        source "$_libdir/tmdb.sh"
        break
    fi
done

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
#   0 — exact duplicate (same track count + disc ID + bitrate) → caller should skip
#   1 — no match, safe to rip
#   2 — partial/different match → existing dir renamed to (old), safe to rip
check_library_duplicate() {
    local artist="$1"
    local album="$2"
    local disc_track_count="$3"
    local disc_id="${CD_DISC_ID:-}"
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

    # Check disc ID from track metadata — if different, this is a different pressing
    if [ -n "$disc_id" ]; then
        local sample_file
        sample_file=$(find "$library_dir" -maxdepth 1 -type f -name "*.${format}" 2>/dev/null | head -1)
        if [ -n "$sample_file" ]; then
            local existing_disc_id=""
            if command -v ffprobe >/dev/null 2>&1; then
                existing_disc_id=$(ffprobe -v quiet -show_entries format_tags=MusicBrainz\ Disc\ Id -of csv=p=0 "$sample_file" 2>/dev/null || true)
            fi
            if [ -n "$existing_disc_id" ] && [ "$existing_disc_id" != "$disc_id" ]; then
                log "Library has $artist / $album but different disc ID (existing=$existing_disc_id, new=$disc_id) — different pressing, removing old"
                rm -rf "$library_dir"
                return 1
            fi
        fi
    fi

    # Count audio files in the existing library directory
    local existing_count
    existing_count=$(find "$library_dir" -maxdepth 1 -type f -name "*.${format}" 2>/dev/null | wc -l)

    if [ "$existing_count" -eq "$disc_track_count" ] && [ "$disc_track_count" -gt 0 ]; then
        # Same track count — check bitrate of first file
        local sample_file
        sample_file=$(find "$library_dir" -maxdepth 1 -type f -name "*.${format}" 2>/dev/null | head -1)
        if [ -n "$sample_file" ] && command -v ffprobe >/dev/null 2>&1; then
            local bitrate
            bitrate=$(ffprobe -v quiet -select_streams a:0 -show_entries stream=bit_rate -of csv=p=0 "$sample_file" 2>/dev/null)
            # bitrate is in bits/sec; 320kbps = 320000
            if [ -n "$bitrate" ] && [ "$bitrate" -lt 310000 ] 2>/dev/null; then
                log "Library has $artist / $album ($existing_count tracks) but bitrate ${bitrate}bps < 320kbps — removing and re-ripping"
                rm -rf "$library_dir"
                return 1
            fi
        fi
        # Exact match at 320kbps (or can't check) — skip
        log "Library already contains $artist / $album ($existing_count tracks, 320kbps) — skipping rip"
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
# Tries multiple sources: MusicBrainz CAA, CAA release-group, iTunes, Deezer
fetch_album_art() {
    local artist="$1"
    local album="$2"
    local dest="$STATUS_DIR/cover-$(basename "$DEVICE").jpg"
    if command -v python3 >/dev/null 2>&1; then
        python3 -c "
import urllib.request, urllib.parse, json, sys, time

artist, album, dest = sys.argv[1], sys.argv[2], sys.argv[3]
ua = 'autorip/1.0 (https://github.com/boylermb/autorip)'

def download(url, target):
    req = urllib.request.Request(url, headers={'User-Agent': ua})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = resp.read()
        if len(data) < 1000:
            return False  # too small, probably an error page
        with open(target, 'wb') as f:
            f.write(data)
    return True

# --- Method 1: MusicBrainz Cover Art Archive (release) ---
try:
    query = urllib.parse.quote(f'artist:\"{artist}\" AND release:\"{album}\"')
    url = f'https://musicbrainz.org/ws/2/release/?query={query}&fmt=json&limit=5'
    req = urllib.request.Request(url, headers={'User-Agent': ua})
    resp = urllib.request.urlopen(req, timeout=10)
    data = json.loads(resp.read())
    releases = data.get('releases', [])
    for rel in releases:
        mbid = rel['id']
        try:
            if download(f'https://coverartarchive.org/release/{mbid}/front-250', dest):
                print(f'OK: CAA release {mbid}')
                sys.exit(0)
        except Exception:
            continue
except Exception as e:
    print(f'CAA release failed: {e}', file=sys.stderr)

time.sleep(0.5)

# --- Method 2: MusicBrainz Cover Art Archive (release-group) ---
try:
    query = urllib.parse.quote(f'artist:\"{artist}\" AND releasegroup:\"{album}\"')
    url = f'https://musicbrainz.org/ws/2/release-group/?query={query}&fmt=json&limit=3'
    req = urllib.request.Request(url, headers={'User-Agent': ua})
    resp = urllib.request.urlopen(req, timeout=10)
    data = json.loads(resp.read())
    rgs = data.get('release-groups', [])
    for rg in rgs:
        rgid = rg['id']
        try:
            if download(f'https://coverartarchive.org/release-group/{rgid}/front-250', dest):
                print(f'OK: CAA release-group {rgid}')
                sys.exit(0)
        except Exception:
            continue
except Exception as e:
    print(f'CAA release-group failed: {e}', file=sys.stderr)

time.sleep(0.5)

# --- Method 3: iTunes Search API ---
try:
    term = urllib.parse.quote(f'{artist} {album}')
    url = f'https://itunes.apple.com/search?term={term}&media=music&entity=album&limit=5'
    req = urllib.request.Request(url, headers={'User-Agent': ua})
    resp = urllib.request.urlopen(req, timeout=10)
    data = json.loads(resp.read())
    for result in data.get('results', []):
        art_url = result.get('artworkUrl100', '')
        if art_url:
            # Get higher resolution (600x600)
            art_url = art_url.replace('100x100bb', '600x600bb')
            if download(art_url, dest):
                print(f'OK: iTunes ({result.get(\"collectionName\", \"?\")})')
                sys.exit(0)
except Exception as e:
    print(f'iTunes failed: {e}', file=sys.stderr)

time.sleep(0.5)

# --- Method 4: Deezer API ---
try:
    term = urllib.parse.quote(f'{artist} {album}')
    url = f'https://api.deezer.com/search/album?q={term}&limit=5'
    req = urllib.request.Request(url, headers={'User-Agent': ua})
    resp = urllib.request.urlopen(req, timeout=10)
    data = json.loads(resp.read())
    for result in data.get('data', []):
        art_url = result.get('cover_big', '') or result.get('cover_medium', '')
        if art_url:
            if download(art_url, dest):
                print(f'OK: Deezer ({result.get(\"title\", \"?\")})')
                sys.exit(0)
except Exception as e:
    print(f'Deezer failed: {e}', file=sys.stderr)

print('No cover art found from any source', file=sys.stderr)
sys.exit(1)
" "$artist" "$album" "$dest" 2>/dev/null || true
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

    # Human / BDMV form: "Show Name: Season 7: Disc 1" or "Show Name - Season 7 - Disc 1"
    # (Spaces preserved — this is CINFO:2 / bdmt_eng.xml content, never a folder name.)
    if echo "$disc_title" | grep -qiE '[:_ -]+season[ _]*[0-9]+[:_ -]+disc[ _]*[0-9]+( *)$'; then
        TV_SHOW=$(echo "$disc_title" | sed -E 's/[[:space:]]*[:_-]+[[:space:]]*[Ss]eason[[:space:]_]*[0-9]+[[:space:]]*[:_-]+[[:space:]]*[Dd]isc[[:space:]_]*[0-9]+[[:space:]]*$//')
        TV_SEASON=$(echo "$disc_title" | grep -oiE 'season[ _]*[0-9]+' | grep -oE '[0-9]+' | sed 's/^0*//')
        TV_DISC=$(echo "$disc_title" | grep -oiE 'disc[ _]*[0-9]+' | grep -oE '[0-9]+' | sed 's/^0*//')
        log "TV disc detected (human format): show='$TV_SHOW' season=$TV_SEASON disc=$TV_DISC"
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
    local disc_title_human="$5"
    shift 5
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
    "disc_title_human": "$(printf '%s' "$disc_title_human" | sed 's/\\/\\\\/g; s/"/\\"/g')",
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

# Match ID_CDROM_MEDIA_BD as well as ID_CDROM_MEDIA_BD_R / _BD_RE / _BD_R_DL
# (writeable BD variants, e.g. burned BD-R copies).
if echo "$disc_info" | grep -qE 'ID_CDROM_MEDIA_BD(_[A-Z_]+)?=1'; then
    is_bluray=true
    # UHD Blu-rays also report ID_CDROM_MEDIA_BD=1.  We distinguish them
    # later using MakeMKV's disc info (AACS v2 / 4K resolution) once we
    # have the disc title scan results.
    log "Detected: Blu-ray disc (checking for UHD...)"
    update_status "ripping" "Blu-ray" "" "Detecting..."
# Match ID_CDROM_MEDIA_DVD plus all DVD recordable variants:
#   ID_CDROM_MEDIA_DVD_R / _DVD_R_DL / _DVD_RW
#   ID_CDROM_MEDIA_DVD_PLUS_R / _DVD_PLUS_R_DL / _DVD_PLUS_RW
#   ID_CDROM_MEDIA_DVD_RAM
# Burned DVDs (e.g. small-press box sets) often report only the recordable
# subtype, never the bare ID_CDROM_MEDIA_DVD=1, so we accept any of them.
elif echo "$disc_info" | grep -qE 'ID_CDROM_MEDIA_DVD(_[A-Z_]+)?=1'; then
    is_dvd=true
    log "Detected: DVD disc (udev reports DVD media)"
    update_status "ripping" "DVD" "" "Detecting..."
# Filesystem fallback: some no-name DVD+R DL writers don't set any
# ID_CDROM_MEDIA_DVD* property at all. If the disc has a UDF/ISO filesystem
# labelled DVDVIDEO (or a VIDEO_TS path is present) treat it as a DVD.
elif echo "$disc_info" | grep -qE 'ID_FS_LABEL(_ENC)?=DVDVIDEO|ID_FS_LABEL(_ENC)?=DVD_VIDEO'; then
    is_dvd=true
    log "Detected: DVD disc (filesystem label DVDVIDEO; udev didn't flag DVD media)"
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
    log "DEBUG: udev properties: $(echo "$disc_info" | grep -E '^ID_(CDROM_MEDIA|FS_)' | tr '\n' ' ')"
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
    # CINFO:2  = human-friendly title (e.g. "Murder, She Wrote: Season 7: Disc 1"
    #            from BDMV bdmt_eng.xml or DVD VTS metadata) — better signal for
    #            TMDb show resolution.
    # We capture BOTH and propagate the human title via the job JSON so the
    # transcode worker's TV resolver can fall back to it when the volume label
    # is missing or doesn't carry an S{n}D{n} marker.
    DISC_TITLE=$(echo "$MAKEMKV_INFO" | grep '^CINFO:32,' | head -1 | sed 's/^CINFO:32,[^,]*,//; s/^"//; s/"$//' | tr ' ' '_' || echo "")
    DISC_TITLE_HUMAN=$(echo "$MAKEMKV_INFO" | grep '^CINFO:2,' | head -1 | sed 's/^CINFO:2,[^,]*,//; s/^"//; s/"$//' || echo "")
    if [ -z "$DISC_TITLE" ] && [ -n "$DISC_TITLE_HUMAN" ]; then
        DISC_TITLE=$(echo "$DISC_TITLE_HUMAN" | tr ' ' '_')
    fi
    if [ -z "$DISC_TITLE" ]; then
        DISC_TITLE="$fallback_title"
        log "WARNING: Could not determine disc title, using $DISC_TITLE"
    fi
    log "Disc title: $DISC_TITLE${DISC_TITLE_HUMAN:+ (human: $DISC_TITLE_HUMAN)}"

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

    # --- Filter out "play all" consolidated titles ---
    # On multi-episode DVD/Blu-ray box sets, MakeMKV often includes one or more
    # playlists that concatenate every episode into a single giant title.  We
    # apply three independent signals; a title flagged by ANY is treated as a
    # compilation and skipped (provided at least 2 titles remain):
    #
    #   A. sum-of-others   — title duration ≈ sum of all other titles (±15%)
    #   B. segment outlier — MakeMKV segment count (TINFO attr 25) ≥ 2× median
    #                        AND duration ≥ 1.8× median duration
    #   C. duration outlier (legacy) — duration ≥ 2.5× median duration
    #
    # Signal A handles the common 1-playall + N-episodes case down to N=2.
    # Signal B catches playlists that splice many small segments even when their
    # duration is only modestly above the median (e.g. "play all minus intro").
    # Signal C is the original heuristic, kept as a fallback.
    if [ "${#TITLE_IDS[@]}" -ge 2 ]; then
        declare -A title_durations=()
        declare -A title_segments=()
        local _total_dur=0
        for tid in "${TITLE_IDS[@]}"; do
            local dur_str
            dur_str=$(echo "$MAKEMKV_INFO" | grep "^TINFO:${tid},9," | head -1 | sed 's/.*,"//' | tr -d '"' || true)
            if [ -n "$dur_str" ]; then
                local h m s
                IFS=: read -r h m s <<< "$dur_str"
                local _d=$(( 10#$h * 3600 + 10#$m * 60 + 10#$s ))
                title_durations[$tid]=$_d
                _total_dur=$(( _total_dur + _d ))
            fi
            # Segment count (attribute 25) — quoted integer; absent on some discs
            local seg_str
            seg_str=$(echo "$MAKEMKV_INFO" | grep "^TINFO:${tid},25," | head -1 | sed 's/.*,"//' | tr -d '"' || true)
            title_segments[$tid]="${seg_str:-0}"
        done

        declare -A _is_playall=()

        # Signal A — sum-of-others (requires ≥3 titles to avoid false-positives
        # on movie + bonus pairs)
        if [ "${#title_durations[@]}" -ge 3 ]; then
            for tid in "${TITLE_IDS[@]}"; do
                local tdur="${title_durations[$tid]:-0}"
                [ "$tdur" -gt 0 ] || continue
                local others=$(( _total_dur - tdur ))
                [ "$others" -gt 0 ] || continue
                local diff
                if [ "$tdur" -ge "$others" ]; then diff=$(( tdur - others )); else diff=$(( others - tdur )); fi
                if [ $(( diff * 100 )) -le $(( others * 15 )) ]; then
                    _is_playall[$tid]=1
                    log "Play-all detected (sum-of-others): title $tid dur=${tdur}s vs sum-others=${others}s"
                fi
            done
        fi

        # Signals B & C — need a duration median (≥3 titles)
        if [ "${#title_durations[@]}" -ge 3 ]; then
            local -a sorted_durs=()
            while IFS= read -r d; do
                sorted_durs+=("$d")
            done < <(for tid in "${TITLE_IDS[@]}"; do echo "${title_durations[$tid]:-0}"; done | sort -n)
            local median_dur="${sorted_durs[$(( ${#sorted_durs[@]} / 2 ))]}"

            local -a sorted_segs=()
            while IFS= read -r s; do
                sorted_segs+=("$s")
            done < <(for tid in "${TITLE_IDS[@]}"; do echo "${title_segments[$tid]:-0}"; done | sort -n)
            local median_seg="${sorted_segs[$(( ${#sorted_segs[@]} / 2 ))]}"

            if [ "${median_dur:-0}" -gt 0 ] 2>/dev/null; then
                for tid in "${TITLE_IDS[@]}"; do
                    [ -n "${_is_playall[$tid]:-}" ] && continue
                    local tdur="${title_durations[$tid]:-0}"
                    local tseg="${title_segments[$tid]:-0}"

                    # Signal C — duration ≥ 2.5× median
                    if [ "$tdur" -ge $(( median_dur * 25 / 10 )) ] 2>/dev/null; then
                        _is_playall[$tid]=1
                        log "Play-all detected (duration outlier): title $tid dur=${tdur}s vs median ${median_dur}s"
                        continue
                    fi

                    # Signal B — segments ≥ 2× median AND duration ≥ 1.8× median
                    if [ "${median_seg:-0}" -gt 0 ] 2>/dev/null \
                       && [ "$tseg" -ge $(( median_seg * 2 )) ] 2>/dev/null \
                       && [ "$tdur" -ge $(( median_dur * 18 / 10 )) ] 2>/dev/null; then
                        _is_playall[$tid]=1
                        log "Play-all detected (segment outlier): title $tid segs=${tseg} (median ${median_seg}), dur=${tdur}s (median ${median_dur}s)"
                    fi
                done
            fi
        fi

        # Apply removals, but never reduce the list below 2 titles
        if [ "${#_is_playall[@]}" -gt 0 ]; then
            local -a filtered_ids=()
            for tid in "${TITLE_IDS[@]}"; do
                if [ -n "${_is_playall[$tid]:-}" ]; then
                    log "Skipping title $tid — flagged as 'play all' compilation"
                else
                    filtered_ids+=("$tid")
                fi
            done
            if [ "${#filtered_ids[@]}" -ge 2 ]; then
                TITLE_IDS=("${filtered_ids[@]}")
            else
                log "WARN: play-all filter would leave <2 titles; keeping original list"
            fi
        fi
    fi

    TITLE_COUNT=${#TITLE_IDS[@]}
    if [ "$TITLE_COUNT" -eq 0 ]; then
        log "ERROR: No titles found on $disc_type disc"
        update_status "error" "$disc_type" "$DISC_TITLE" "No titles found" "" "$DISC_TITLE" "[]"
        eject "$DEVICE" 2>/dev/null || true
        exit 1
    fi

    log "Found $TITLE_COUNT title(s) to rip: ${TITLE_IDS[*]}"

    # Per-host staging suffix prevents collisions when two different
    # physical discs share the same volume label (e.g. multiple
    # "DVDVIDEO" DVDs ripping simultaneously on different hosts —
    # without the suffix they'd both write to STAGING_DIR/DVDVIDEO/
    # and clobber each other's title files).
    OUTPUT_DIR="$STAGING_DIR/${DISC_TITLE}-$(hostname -s)-$$"
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

            # Locate the freshly-written MKV. Both lookups must tolerate
            # SIGPIPE under `set -o pipefail` — `find | head` can race when
            # find writes more output than head reads (e.g. NFS metadata
            # cache surfaces a stale match), causing the whole script to
            # die between titles. The trailing `|| true` swallows that.
            mkv_file=$(find "$OUTPUT_DIR" -maxdepth 1 -name "*_t$(printf '%02d' "$tid").mkv" -newer "$LOCKFILE" 2>/dev/null | head -1 || true)
            if [ -z "$mkv_file" ]; then
                mkv_file=$(find "$OUTPUT_DIR" -maxdepth 1 -name "*.mkv" -newer "$LOCKFILE" 2>/dev/null | sort -t/ -k2 | tail -1 || true)
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
        enqueue_video_disc "$DISC_TITLE" "$disc_type" "$is_uhd" "$OUTPUT_DIR" "${DISC_TITLE_HUMAN:-}" "${ripped_files[@]}"
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

    # Monitor abcde output for track progress and detect stalls/errors
    tracks_seen=0
    current_track_name=""
    last_progress_time=$(date +%s)
    ABCDE_STALL_TIMEOUT=300  # 5 minutes with no progress = stalled
    abcde_error_detected=""
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
            last_progress_time=$(date +%s)
            update_status "ripping" "Audio CD" "$CD_ALBUM" \
                "Ripping track $new_count/$cd_track_count..." \
                "$CD_ARTIST" "$CD_ALBUM" "$CD_TRACKS_JSON" \
                "$cd_track_count" "$completed" "$current_track_name"
        fi

        # Check for cdparanoia/rip errors in abcde's working directory
        # abcde writes errors to an "errors" file in its temp dir
        abcde_errors_file=$(find /tmp -path "*/abcde.*/errors" -user autorip -newer "$ABCDE_LOG" 2>/dev/null | head -1 || true)
        if [ -z "$abcde_errors_file" ]; then
            abcde_errors_file=$(find /tmp -path "*/abcde.*/errors" -user autorip 2>/dev/null | head -1 || true)
        fi
        if [ -n "$abcde_errors_file" ] && [ -s "$abcde_errors_file" ]; then
            abcde_error_detected=$(cat "$abcde_errors_file" 2>/dev/null)
            log "ERROR: abcde reported rip errors: $abcde_error_detected"
            # Kill abcde and all its children
            kill -- -"$ABCDE_PID" 2>/dev/null || kill "$ABCDE_PID" 2>/dev/null || true
            sleep 2
            # Force kill if still alive
            kill -9 -- -"$ABCDE_PID" 2>/dev/null || kill -9 "$ABCDE_PID" 2>/dev/null || true
            break
        fi

        # Detect stall: no new tracks grabbed for ABCDE_STALL_TIMEOUT seconds
        # and abcde log hasn't grown (encoding still happening = not stalled)
        now=$(date +%s)
        elapsed=$((now - last_progress_time))
        if [ "$elapsed" -ge "$ABCDE_STALL_TIMEOUT" ]; then
            # Check if lame/encoding is still actively running (CPU use)
            encoding_active=$(pgrep -P "$ABCDE_PID" -f "lame|flac|oggenc" 2>/dev/null || true)
            if [ -z "$encoding_active" ]; then
                abcde_error_detected="Rip stalled: no progress for ${elapsed}s and no active encoding"
                log "ERROR: $abcde_error_detected"
                kill -- -"$ABCDE_PID" 2>/dev/null || kill "$ABCDE_PID" 2>/dev/null || true
                sleep 2
                kill -9 -- -"$ABCDE_PID" 2>/dev/null || kill -9 "$ABCDE_PID" 2>/dev/null || true
                break
            else
                # Encoding still running — reset timer, it's just slow
                last_progress_time=$now
            fi
        fi

        sleep 3
    done

    # abcde finished — get its exit code
    wait "$ABCDE_PID" 2>/dev/null
    abcde_rc=$?
    rm -f "$ABCDE_LOG" "$RIP_CONF"

    # If we detected an error during monitoring, treat as failure
    if [ -n "$abcde_error_detected" ]; then
        log "ERROR: CD rip failed for $CD_ARTIST / $CD_ALBUM: $abcde_error_detected"
        rm -rf "$RIP_STAGING"
        update_status "error" "Audio CD" "$CD_ALBUM" "Rip failed: $abcde_error_detected" "$CD_ARTIST" "$CD_ALBUM" "$CD_TRACKS_JSON"
        # Eject the bad disc
        log "Ejecting failed disc..."
        sleep 2
        eject "$DEVICE" 2>/dev/null || true
        date +%s > "$COOLDOWN_FILE"
        update_status "idle"
        exit 1
    fi

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

            # Store disc ID in track metadata (TXXX:MusicBrainz Disc Id)
            if [ -n "$CD_DISC_ID" ] && command -v eyeD3 >/dev/null 2>&1; then
                for mp3 in "$staging_album"/*.mp3; do
                    [ -f "$mp3" ] && eyeD3 --user-text-frame="MusicBrainz Disc Id:$CD_DISC_ID" "$mp3" >/dev/null 2>&1 || true
                done
                log "Wrote disc ID $CD_DISC_ID to track metadata"
            fi

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
