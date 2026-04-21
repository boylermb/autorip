#!/usr/bin/env bash
# =============================================================================
# transcode-worker.sh - Centralized post-processing queue worker
# https://github.com/boylermb/autorip
# =============================================================================
# Watches $OUTPUT_BASE/.autorip-queue/ for job files (JSON) submitted by
# autorip nodes after ripping completes.
#
# Job types:
#   video (default) — Transcode MPEG-2 → H.265 using NVIDIA NVENC on the
#                     GPU node, then rename/move into Jellyfin-compatible paths.
#                     UHD Blu-ray jobs skip transcoding (already H.265 + HDR).
#   audio-cd        — Tag, rename, and move audio files from staging into the
#                     Music library (Artist/Album/NN - Title.ext).
#
# Jobs arrive as titles/discs are ripped, so processing can begin while the
# disc is still being read.
#
# After processing, files are moved to $OUTPUT_BASE/.unreviewed/ (mirroring
# the Media directory structure) along with a metadata.json file.  Staging
# files are deleted immediately after transcoding to free disk space.
#
# Use the separate media-review web app (or the `approve`/`reject`
# subcommands) to review items before they are moved into the official
# Media library.
#
# Usage:
#   transcode-worker.sh          Process pending queue jobs (systemd timer)
#   transcode-worker.sh list     Show items pending review
#   transcode-worker.sh clean    Approve all and move to library
#   transcode-worker.sh approve <path>  Approve a single item
#   transcode-worker.sh reject <path>   Reject a single item
#
# Runs as a systemd oneshot triggered by a 30s timer.
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
    fi
    if [ -f "$_libdir/tv-progress.sh" ]; then
        # shellcheck source=lib/tv-progress.sh
        source "$_libdir/tv-progress.sh"
    fi
    if [ -f "$_libdir/tv-overrides.sh" ]; then
        # shellcheck source=lib/tv-overrides.sh
        source "$_libdir/tv-overrides.sh"
    fi
    if [ -f "$_libdir/tv-runtime-check.sh" ]; then
        # shellcheck source=lib/tv-runtime-check.sh
        source "$_libdir/tv-runtime-check.sh"
    fi
    # Stop after the first directory that had at least one library
    if [ -f "$_libdir/tmdb.sh" ] || [ -f "$_libdir/tv-progress.sh" ] || [ -f "$_libdir/tv-overrides.sh" ]; then
        break
    fi
done

LOGPREFIX="[transcode-worker]"
QUEUE_DIR="$OUTPUT_BASE/.autorip-queue"
STAGING_DIR="$OUTPUT_BASE/.autorip-staging"
MOVIES_DIR="$OUTPUT_BASE/Video/Movies"
TV_DIR="$OUTPUT_BASE/Video/TV"
MUSIC_DIR="$OUTPUT_BASE/Audio/Music"
UNREVIEWED_DIR="$OUTPUT_BASE/.unreviewed"
UNREVIEWED_MOVIES="$UNREVIEWED_DIR/Video/Movies"
UNREVIEWED_TV="$UNREVIEWED_DIR/Video/TV"
UNREVIEWED_MUSIC="$UNREVIEWED_DIR/Audio/Music"
RIP_LOG="$OUTPUT_BASE/.rip-log.json"
RIP_LOG_MD="$OUTPUT_BASE/.rip-log.md"
STATUS_FILE="$OUTPUT_BASE/.autorip-queue/.worker-status.json"
HOSTNAME=$(hostname)

# mnamer settings (movies only) — target unreviewed dir, not final Media dir
MNAMER_MOVIE_DIR="$UNREVIEWED_DIR/Video/Movies"
MNAMER_MOVIE_FORMAT="${MNAMER_MOVIE_FORMAT:-{name} ({year})/{name} ({year}){extension}}"

EPISODES_PER_DISC="${EPISODES_PER_DISC:-4}"
UHD_KEEP_ORIGINAL="${UHD_KEEP_ORIGINAL:-yes}"
NNEDI3_WEIGHTS="${NNEDI3_WEIGHTS:-/usr/share/ffmpeg/nnedi3_weights.bin}"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $LOGPREFIX $*"; }

# Escape a string for safe inclusion in a JSON value (handles \ and ")
json_escape() { printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'; }

# ---------- Rip log ----------
# Append an entry to the shared rip log on the NAS after a successful rip/post-process.
log_rip_entry() {
    local disc_type="$1"
    local artist="$2"
    local album="$3"
    local tracks_json="$4"       # JSON array
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
    "device": "",
    "disc_type": "${disc_type}",
    "artist": "${s_artist}",
    "album": "${s_album}",
    "tracks": ${tracks_json},
    "cover_art": "${s_cover}"
}
ENTRY
)

    (
        flock -w 5 201 || { log "WARNING: Could not acquire rip-log lock"; return; }

        local existing="[]"
        if [ -f "$RIP_LOG" ]; then
            existing=$(cat "$RIP_LOG" 2>/dev/null || echo "[]")
            if ! echo "$existing" | head -c1 | grep -q '\['; then
                existing="[]"
            fi
        fi

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

    log "Rip logged: $disc_type — $artist / $album"

    # Regenerate the markdown version of the rip log
    generate_rip_log_markdown
}

# ---------- Rip log → Markdown ----------
# Regenerate .rip-log.md from .rip-log.json.  Called after every log_rip_entry().
# Image paths use the dashboard's /api/rip-log/art proxy.
generate_rip_log_markdown() {
    [ -f "$RIP_LOG" ] || return
    python3 - "$RIP_LOG" "$RIP_LOG_MD" <<'PYEOF' 2>/dev/null || log "WARNING: Markdown generation failed"
import json, sys, urllib.parse
from datetime import datetime

rip_log_path = sys.argv[1]
md_path = sys.argv[2]

with open(rip_log_path) as f:
    entries = json.load(f)

lines = []
lines.append("# Verified Media Library")
lines.append("")
lines.append(f"*{len(entries)} verified rip{'s' if len(entries) != 1 else ''}*")
lines.append("")

# Group by disc_type
audio = [e for e in entries if e.get("disc_type") == "Audio CD"]
video = [e for e in entries if e.get("disc_type") != "Audio CD"]

if audio:
    lines.append("## 🎵 Audio CDs")
    lines.append("")
    # Sort by artist, then album
    for entry in sorted(audio, key=lambda e: (e.get("artist", "").lower(), e.get("album", "").lower())):
        artist = entry.get("artist", "Unknown Artist")
        album = entry.get("album", "Unknown Album")
        tracks = entry.get("tracks", [])
        cover = entry.get("cover_art", "")
        hostname = entry.get("hostname", "")
        ts = entry.get("timestamp", "")

        # Format date
        date_str = ""
        if ts:
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                date_str = dt.strftime("%b %d, %Y")
            except Exception:
                date_str = ts

        lines.append(f"### {artist} — {album}")
        lines.append("")

        # Album art (uses dashboard proxy)
        if cover:
            art_url = f"/api/rip-log/art?path={urllib.parse.quote(cover)}"
            lines.append(f"![{artist} — {album}]({art_url})")
            lines.append("")

        # Navidrome link
        album_filter = json.dumps({"name": album})
        nav_url = f"https://music.home.lan/app/#/album?filter={urllib.parse.quote(album_filter)}&order=ASC&sort=name"
        meta_parts = []
        if hostname:
            meta_parts.append(f"Ripped on **{hostname}**")
        if date_str:
            meta_parts.append(date_str)
        meta_parts.append(f"[🎧 Listen on Navidrome]({nav_url})")
        lines.append(" · ".join(meta_parts))
        lines.append("")

        # Track listing
        if tracks:
            for i, track in enumerate(tracks, 1):
                lines.append(f"{i}. {track}")
            lines.append("")

        lines.append("---")
        lines.append("")

if video:
    lines.append("## 📀 Video")
    lines.append("")
    for entry in sorted(video, key=lambda e: e.get("album", e.get("title", "")).lower()):
        title = entry.get("album") or entry.get("title") or "Unknown"
        hostname = entry.get("hostname", "")
        ts = entry.get("timestamp", "")
        disc_type = entry.get("disc_type", "Video")

        date_str = ""
        if ts:
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                date_str = dt.strftime("%b %d, %Y")
            except Exception:
                date_str = ts

        lines.append(f"### {title}")
        lines.append("")
        meta_parts = [disc_type]
        if hostname:
            meta_parts.append(f"ripped on **{hostname}**")
        if date_str:
            meta_parts.append(date_str)
        lines.append(" · ".join(meta_parts))
        lines.append("")
        lines.append("---")
        lines.append("")

md_content = "\n".join(lines)
with open(md_path + ".tmp", "w") as f:
    f.write(md_content)
import os
os.replace(md_path + ".tmp", md_path)
PYEOF
}
update_worker_status() {
    local state="$1"
    local job_file="${2:-}"
    local detail="${3:-}"
    local tmpfile
    tmpfile=$(mktemp "$QUEUE_DIR/.worker-status.json.XXXXXX" 2>/dev/null || mktemp /tmp/.worker-status.json.XXXXXX)
    chmod 644 "$tmpfile"
    cat > "$tmpfile" <<EOF
{
    "hostname": "${HOSTNAME}",
    "state": "${state}",
    "job": "$(basename "${job_file}" 2>/dev/null || echo "")",
    "detail": "$(printf '%s' "$detail" | sed 's/\\/\\\\/g; s/"/\\"/g')",
    "updated": "$(date '+%Y-%m-%d %H:%M:%S')"
}
EOF
    mv -f "$tmpfile" "$STATUS_FILE"
}

# ---------- TV disc title parsing (duplicated from autorip.sh) ----------
parse_tv_disc_title() {
    local disc_title="$1"
    TV_SHOW="" ; TV_SEASON="" ; TV_DISC=""

    if echo "$disc_title" | grep -qiE '_S[0-9]+D[0-9]+$'; then
        TV_SHOW=$(echo "$disc_title" | sed -E 's/_[Ss][0-9]+[Dd][0-9]+$//' | tr '_' ' ')
        TV_SHOW=$(echo "$TV_SHOW" | awk '{for(i=1;i<=NF;i++){$i=toupper(substr($i,1,1))tolower(substr($i,2))}}1')
        TV_SEASON=$(echo "$disc_title" | grep -oiE 'S([0-9]+)D' | grep -oE '[0-9]+' | sed 's/^0*//')
        TV_DISC=$(echo "$disc_title" | grep -oiE 'D([0-9]+)$' | grep -oE '[0-9]+' | sed 's/^0*//')
        return 0
    fi

    if echo "$disc_title" | grep -qiE '_SEASON_[0-9]+_DISC_[0-9]+$'; then
        TV_SHOW=$(echo "$disc_title" | sed -E 's/_[Ss][Ee][Aa][Ss][Oo][Nn]_[0-9]+_[Dd][Ii][Ss][Cc]_[0-9]+$//' | tr '_' ' ')
        TV_SHOW=$(echo "$TV_SHOW" | awk '{for(i=1;i<=NF;i++){$i=toupper(substr($i,1,1))tolower(substr($i,2))}}1')
        TV_SEASON=$(echo "$disc_title" | grep -oiE 'SEASON_([0-9]+)' | grep -oE '[0-9]+' | sed 's/^0*//')
        TV_DISC=$(echo "$disc_title" | grep -oiE 'DISC_([0-9]+)' | grep -oE '[0-9]+' | sed 's/^0*//')
        return 0
    fi

    # Human / BDMV form: "Show Name: Season 7: Disc 1" — feeds in via disc_title_human
    # (CINFO:2 from MakeMKV / bdmt_eng.xml). Spaces preserved.
    if echo "$disc_title" | grep -qiE '[:_ -]+season[ _]*[0-9]+[:_ -]+disc[ _]*[0-9]+( *)$'; then
        TV_SHOW=$(echo "$disc_title" | sed -E 's/[[:space:]]*[:_-]+[[:space:]]*[Ss]eason[[:space:]_]*[0-9]+[[:space:]]*[:_-]+[[:space:]]*[Dd]isc[[:space:]_]*[0-9]+[[:space:]]*$//')
        TV_SEASON=$(echo "$disc_title" | grep -oiE 'season[ _]*[0-9]+' | grep -oE '[0-9]+' | sed 's/^0*//')
        TV_DISC=$(echo "$disc_title" | grep -oiE 'disc[ _]*[0-9]+' | grep -oE '[0-9]+' | sed 's/^0*//')
        return 0
    fi

    return 1
}

# ---------- TMDb episode name lookup ----------
# Wrapper around the shared tmdb library that preserves the (show_name, season)
# call signature used by tv_rename_file. Sets TMDB_EP_NAMES[ep#].
tmdb_fetch_season_by_name() {
    local show="$1"
    local season="$2"

    if ! declare -F tmdb_search_show >/dev/null 2>&1; then
        log "TMDb library not loaded — episode titles will not be available"
        return 1
    fi

    tmdb_search_show "$show" || return 1
    tmdb_fetch_season "$TMDB_SHOW_ID" "$season" || return 1
    return 0
}

# ---------- Show-name canonicalization ----------
# Given a raw show name parsed from a disc label (e.g. "Sopranos The",
# "Sp1", "Futurama Vol2"), try to resolve it to a TMDb-known canonical
# title. On success, sets:
#   TV_SHOW_CANONICAL = TMDb's canonical name (e.g. "The Sopranos")
#   TV_SHOW_TMDB_ID   = TMDb show id
#   returns 0
# On failure (no library, no hits on any variant), leaves both empty
# and returns 1. The caller should treat failure as "route to _unmatched/".
#
# Variants tried, in order:
#   1. raw input
#   2. with trailing "Vol N" / "Volume N" / "Disc N" / "Box N" stripped
#   3. with leading "The " moved to the end ("Sopranos The" -> "The Sopranos")
#   4. with the trailing "The" moved to the front
#   5. collapsed-whitespace, single-word inputs >=4 chars only (skip "Sp1")
canonicalize_tv_show() {
    local raw="$1"
    TV_SHOW_CANONICAL=""
    TV_SHOW_TMDB_ID=""

    if ! declare -F tmdb_search_show >/dev/null 2>&1; then
        return 1
    fi

    # Build variants
    local -a variants=()
    variants+=( "$raw" )

    local stripped
    stripped=$(echo "$raw" | sed -E 's/[[:space:]]+(Vol(ume)?|Disc|Box)[[:space:]]*[0-9]+$//I')
    [ "$stripped" != "$raw" ] && variants+=( "$stripped" )

    if echo "$raw" | grep -qiE '^The[[:space:]]'; then
        variants+=( "$(echo "$raw" | sed -E 's/^[Tt]he[[:space:]]+(.*)$/\1, The/')" )
    fi
    if echo "$raw" | grep -qiE '[[:space:]]The$'; then
        variants+=( "The $(echo "$raw" | sed -E 's/[[:space:]]+[Tt]he$//')" )
    fi

    local v
    for v in "${variants[@]}"; do
        # Skip junk: empty, single short word, or all-digits
        [ -z "$v" ] && continue
        local word_count
        word_count=$(echo "$v" | wc -w | tr -d ' ')
        if [ "$word_count" -eq 1 ] && [ "${#v}" -lt 4 ]; then
            continue
        fi
        if echo "$v" | grep -qE '^[0-9]+$'; then
            continue
        fi

        if tmdb_search_show "$v" 2>/dev/null && [ -n "${TMDB_SHOW_ID:-}" ]; then
            TV_SHOW_CANONICAL="${TMDB_SHOW_NAME:-$v}"
            TV_SHOW_TMDB_ID="$TMDB_SHOW_ID"
            return 0
        fi
    done

    return 1
}

# ---------- TV rename (single file) ----------
tv_rename_file() {
    local mkv="$1"
    local title_index="$2"   # 1-indexed

    # Unmatched-show fallback: park files under _unmatched/<raw_disc_label>/
    # with the original basename so a human can identify them later.
    if [ -n "${TV_UNMATCHED:-}" ] && [ -n "${TV_UNMATCHED_LABEL:-}" ]; then
        local unmatched_dir="$UNREVIEWED_TV/_unmatched/$TV_UNMATCHED_LABEL"
        mkdir -p "$unmatched_dir"
        local base
        base=$(basename "$mkv")
        mv -f "$mkv" "$unmatched_dir/$base"
        log "Unmatched TV disc: moved $base → $unmatched_dir/"
        return 0
    fi

    # Per-title classification from the runtime check (if it ran).
    # An "extra" is a title whose runtime falls well outside the
    # season's normal episode-runtime band — bonus features, behind-
    # the-scenes, etc.  Route these to <Show>/Extras/ with their
    # original basename instead of consuming an SxxEyy slot.
    local title_verdict=""
    if declare -F tv_runtime_verdict >/dev/null 2>&1; then
        title_verdict=$(tv_runtime_verdict "$title_index")
    fi
    if [ "$title_verdict" = "extra" ]; then
        local extras_dir="$UNREVIEWED_TV/$TV_SHOW/Extras"
        mkdir -p "$extras_dir"
        local base
        base=$(basename "$mkv")
        mv -f "$mkv" "$extras_dir/$base"
        log "TV extra (runtime outside season band): $(basename "$mkv") → $extras_dir/"
        return 0
    fi

    # Pending-review fallback: TMDb runtimes disagree with actual file
    # durations.  Park under _pending/ with the proposed-name suffix so a
    # human can compare to the episodes-plan.txt sitting alongside.
    if [ -n "${TV_PENDING:-}" ] && [ -n "${TV_PENDING_LABEL:-}" ]; then
        local pending_dir="$UNREVIEWED_TV/_pending/$TV_PENDING_LABEL"
        mkdir -p "$pending_dir"
        local ep_num=$(( ${TV_FIRST_EPISODE:-1} + title_index - 1 ))
        local proposed
        proposed=$(printf "%s - S%02dE%02d.proposed.mkv" "$TV_SHOW" "$TV_SEASON" "$ep_num")
        mv -f "$mkv" "$pending_dir/$proposed"
        log "Pending review: moved $(basename "$mkv") → $pending_dir/$proposed"
        return 0
    fi

    local season_dir
    season_dir=$(printf "Season %02d" "$TV_SEASON")
    local dest_dir="$UNREVIEWED_TV/$TV_SHOW/$season_dir"
    mkdir -p "$dest_dir"

    # Episode number: prefer per-show progress state (TV_FIRST_EPISODE set by
    # the disc handler); fall back to legacy global-config math if unset.
    # Subtract any earlier titles on this disc that were classified as
    # extras so the episode counter stays continuous.
    local extras_before=0
    if declare -F tv_runtime_verdict >/dev/null 2>&1; then
        local _i
        for (( _i=1; _i<title_index; _i++ )); do
            if [ "$(tv_runtime_verdict "$_i")" = "extra" ]; then
                extras_before=$(( extras_before + 1 ))
            fi
        done
    fi
    local effective_index=$(( title_index - extras_before ))

    local ep_num
    if [ -n "${TV_FIRST_EPISODE:-}" ]; then
        ep_num=$(( TV_FIRST_EPISODE + effective_index - 1 ))
    else
        ep_num=$(( (TV_DISC - 1) * EPISODES_PER_DISC + effective_index ))
    fi
    local ep_name

    # Try to get episode title from TMDb
    local ep_title=""
    if tmdb_fetch_season_by_name "$TV_SHOW" "$TV_SEASON" 2>/dev/null; then
        ep_title="${TMDB_EP_NAMES[$ep_num]:-}"
    fi

    if [ -n "$ep_title" ]; then
        # Sanitize episode title for filesystem
        local safe_title
        safe_title=$(echo "$ep_title" | tr -d ':><|*/"?\\!' | sed 's/  */ /g')
        ep_name=$(printf "%s - S%02dE%02d - %s.mkv" "$TV_SHOW" "$TV_SEASON" "$ep_num" "$safe_title")
    else
        ep_name=$(printf "%s - S%02dE%02d.mkv" "$TV_SHOW" "$TV_SEASON" "$ep_num")
    fi
    mv -f "$mkv" "$dest_dir/$ep_name"
    log "Moved $(basename "$mkv") → $dest_dir/$ep_name"
}

# ---------- TV artwork (poster + show.nfo + season-poster) ----------
# Idempotent: skips files that already exist.
# Uses TMDB_SHOW_ID populated by tmdb_search_show; if not loaded, no-ops.
fetch_tv_artwork() {
    local show="$1"
    local season="$2"

    if ! declare -F tmdb_search_show >/dev/null 2>&1; then
        return 0  # library not loaded — silently skip
    fi

    local show_dir="$UNREVIEWED_TV/$show"
    local season_dir
    season_dir=$(printf "Season %02d" "$season")
    local season_path="$show_dir/$season_dir"
    mkdir -p "$season_path"

    local poster="$show_dir/poster.jpg"
    local nfo="$show_dir/tvshow.nfo"
    local season_poster="$season_path/season-poster.jpg"

    # Short-circuit if everything already exists
    if [ -s "$poster" ] && [ -s "$nfo" ] && [ -s "$season_poster" ]; then
        return 0
    fi

    # Resolve TMDb ID
    if ! tmdb_search_show "$show" 2>/dev/null; then
        log "Artwork: could not find '$show' on TMDb; skipping artwork"
        return 1
    fi

    # Show-level poster
    if [ ! -s "$poster" ]; then
        if tmdb_fetch_show_images "$TMDB_SHOW_ID" 2>/dev/null && [ -n "$TMDB_POSTER_URL" ]; then
            if tmdb_download_image "$TMDB_POSTER_URL" "$poster"; then
                log "Artwork: downloaded poster → $poster"
            fi
        fi
    fi

    # tvshow.nfo (Jellyfin/Kodi standard)
    if [ ! -s "$nfo" ] && [ -n "$TMDB_SHOW_ID" ]; then
        cat > "$nfo" <<EOF
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<tvshow>
  <title>$(echo "${TMDB_SHOW_NAME:-$show}" | sed 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g')</title>
  <uniqueid type="tmdb" default="true">${TMDB_SHOW_ID}</uniqueid>
</tvshow>
EOF
        log "Artwork: wrote $nfo"
    fi

    # Season-level poster
    if [ ! -s "$season_poster" ]; then
        if tmdb_fetch_season_images "$TMDB_SHOW_ID" "$season" 2>/dev/null \
           && [ -n "$TMDB_SEASON_POSTER_URL" ]; then
            if tmdb_download_image "$TMDB_SEASON_POSTER_URL" "$season_poster"; then
                log "Artwork: downloaded season poster → $season_poster"
            fi
        fi
    fi

    return 0
}

# ---------- Movie rename (single file, mnamer) ----------
movie_rename_file() {
    local mkv="$1"
    local disc_title="$2"

    if ! command -v mnamer >/dev/null 2>&1; then
        log "mnamer not installed — using fallback"
        movie_fallback_file "$mkv" "$disc_title"
        return
    fi

    # Move to a temp file so mnamer can move it into the unreviewed dir
    local tmp_copy="${mkv%.mkv}.mnamer-copy.mkv"
    mv -f "$mkv" "$tmp_copy"

    log "Running mnamer for movie file: $(basename "$mkv")"
    if mnamer --batch \
        --media=movie \
        --movie-api=tmdb \
        --movie-directory="$MNAMER_MOVIE_DIR" \
        --movie-format="$MNAMER_MOVIE_FORMAT" \
        "$tmp_copy" 2>&1; then
        if [ -f "$tmp_copy" ]; then
            log "mnamer did not match $(basename "$mkv"), using fallback"
            mv -f "$tmp_copy" "$mkv"
            movie_fallback_file "$mkv" "$disc_title"
        else
            log "mnamer matched and moved $(basename "$mkv") to unreviewed"
        fi
    else
        log "mnamer failed, using fallback"
        [ -f "$tmp_copy" ] && mv -f "$tmp_copy" "$mkv"
        movie_fallback_file "$mkv" "$disc_title"
    fi
}

movie_fallback_file() {
    local mkv="$1"
    local disc_title="$2"
    local fallback_dir="$UNREVIEWED_MOVIES/$disc_title"
    mkdir -p "$fallback_dir"
    mv -f "$mkv" "$fallback_dir/"
    log "Moved $(basename "$mkv") to $fallback_dir"
}

# ---------- Process audio CD post-processing job ----------
process_audio_cd_job() {
    local job_file="$1"
    local job_name
    job_name=$(basename "$job_file")

    log "Processing audio CD job: $job_name"
    update_worker_status "post-processing" "$job_file" "Audio CD — reading job..."

    # Parse job JSON
    local artist album staging_dir format source_host tracks_json
    artist=$(grep -oP '"artist"\s*:\s*"\K[^"]+' "$job_file" || echo "Unknown Artist")
    album=$(grep -oP '"album"\s*:\s*"\K[^"]+' "$job_file" || echo "Unknown Album")
    staging_dir=$(grep -oP '"staging_dir"\s*:\s*"\K[^"]+' "$job_file" || echo "")
    format=$(grep -oP '"format"\s*:\s*"\K[^"]+' "$job_file" || echo "mp3")
    source_host=$(grep -oP '"source_host"\s*:\s*"\K[^"]+' "$job_file" || echo "unknown")

    # Extract the full tracks array using python3
    tracks_json=$(python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    job = json.load(f)
print(json.dumps(job.get('tracks', [])))
" "$job_file" 2>/dev/null || echo "[]")

    if [ -z "$staging_dir" ] || [ ! -d "$staging_dir" ]; then
        log "ERROR: Staging directory missing or invalid: $staging_dir"
        mv "$job_file" "${job_file%.json}.error"
        return 1
    fi

    # Mark job as in-progress
    mv "$job_file" "${job_file%.json}.processing"
    local processing_file="${job_file%.json}.processing"

    log "Post-processing: $artist / $album ($format from $source_host)"
    update_worker_status "post-processing" "$processing_file" "$artist — $album"

    # Build final destination: unreviewed Music/Artist/Album/
    local safe_artist safe_album
    safe_artist=$(echo "$artist" | sed -e 's/^\.*//' | tr -d ':><|*/"'"'"'?\\!')
    safe_album=$(echo "$album" | sed -e 's/^\.*//' | tr -d ':><|*/"'"'"'?\\!')
    local final_dir="$UNREVIEWED_MUSIC/$safe_artist/$safe_album"
    mkdir -p "$final_dir"
    # Ensure the artist directory is also accessible
    chown autorip:autorip "$UNREVIEWED_MUSIC/$safe_artist" 2>/dev/null || true
    chmod 777 "$UNREVIEWED_MUSIC/$safe_artist" 2>/dev/null || true

    # Get track names as a bash array
    local -a track_names
    while IFS= read -r name; do
        track_names+=("$name")
    done < <(python3 -c "
import json, sys
tracks = json.loads(sys.argv[1])
for t in tracks:
    print(t)
" "$tracks_json" 2>/dev/null)

    local track_count=${#track_names[@]}
    log "Found $track_count track name(s) from metadata"

    # Process each audio file in the staging directory
    local file_num=0
    local ext="$format"
    while IFS= read -r audio_file; do
        [ -f "$audio_file" ] || continue
        file_num=$((file_num + 1))

        local tracknum
        tracknum=$(printf '%02d' "$file_num")
        local track_name=""
        if [ "$file_num" -le "$track_count" ]; then
            track_name="${track_names[$((file_num - 1))]}"
        fi

        # Tag the file with eyeD3
        if [ "$ext" = "mp3" ] && command -v eyeD3 >/dev/null 2>&1; then
            log "Tagging $tracknum: $track_name"
            local eyeD3_args=()
            eyeD3_args+=(--artist "$artist")
            eyeD3_args+=(--album "$album")
            eyeD3_args+=(--track "$file_num")
            if [ -n "$track_name" ]; then
                eyeD3_args+=(--title "$track_name")
            fi
            if [ -f "$staging_dir/cover.jpg" ]; then
                eyeD3_args+=(--add-image "$staging_dir/cover.jpg:FRONT_COVER")
            fi
            eyeD3 "${eyeD3_args[@]}" "$audio_file" 2>&1 || log "WARNING: eyeD3 tagging failed for track $tracknum"
        elif [ "$ext" = "flac" ] && command -v metaflac >/dev/null 2>&1; then
            log "Tagging $tracknum: $track_name (FLAC)"
            metaflac --remove-all-tags "$audio_file" 2>/dev/null || true
            metaflac --set-tag="ARTIST=$artist" \
                     --set-tag="ALBUM=$album" \
                     --set-tag="TRACKNUMBER=$file_num" \
                     "$audio_file" 2>&1 || true
            if [ -n "$track_name" ]; then
                metaflac --set-tag="TITLE=$track_name" "$audio_file" 2>&1 || true
            fi
            if [ -f "$staging_dir/cover.jpg" ]; then
                metaflac --import-picture-from="$staging_dir/cover.jpg" "$audio_file" 2>&1 || true
            fi
        fi

        # Rename: NN.ext → NN - Track Name.ext
        local new_name
        if [ -n "$track_name" ]; then
            local safe_track
            safe_track=$(echo "$track_name" | sed -e 's/^\.*//' | tr -d ':><|*/"'"'"'?\\!')
            new_name="${tracknum} - ${safe_track}.${ext}"
        else
            new_name="${tracknum}.${ext}"
        fi

        cp -f "$audio_file" "$final_dir/$new_name"
        log "  → $new_name"
    done < <(find "$staging_dir" -maxdepth 1 -name "*.$ext" 2>/dev/null | sort)

    # Copy cover art to final directory
    if [ -f "$staging_dir/cover.jpg" ]; then
        cp -f "$staging_dir/cover.jpg" "$final_dir/cover.jpg"
        log "Copied cover art to $final_dir"
    fi

    # Make library files accessible to NFS clients (Picard, etc.)
    chown -R autorip:autorip "$final_dir" 2>/dev/null || true
    chmod 777 "$final_dir" 2>/dev/null || true
    chmod 666 "$final_dir"/* 2>/dev/null || true

    # Rip log entry is deferred until review is approved.

    # Write metadata.json for the review app
    python3 -c "
import json, sys, datetime
with open(sys.argv[1], 'r') as f:
    data = json.load(f)
data['_unreviewed_dir'] = sys.argv[2]
data['_processed_at'] = datetime.datetime.now().isoformat()
with open(sys.argv[2] + '/metadata.json', 'w') as f:
    json.dump(data, f, indent=2)
" "$processing_file" "$final_dir" 2>/dev/null || true

    # Clean staging — files have been moved to the unreviewed dir
    if [ -n "$staging_dir" ] && [ -d "$staging_dir" ]; then
        local dir_size
        dir_size=$(du -sh "$staging_dir" 2>/dev/null | cut -f1)
        log "Cleaning staging: $staging_dir ($dir_size)"
        rm -rf "$staging_dir"
        local parent_dir
        parent_dir=$(dirname "$staging_dir")
        if [ -d "$parent_dir" ] && [ "$(basename "$parent_dir")" != ".autorip-staging" ]; then
            rmdir "$parent_dir" 2>/dev/null || true
        fi
    fi

    # Remove the processing file — unreviewed dir metadata.json is now the record
    rm -f "$processing_file"
    log "Audio CD job complete (pending review in unreviewed): $artist / $album ($file_num tracks)"
}

# ---------- Transcode a single MKV file ----------
transcode_file() {
    local file_path="$1"
    local is_uhd="$2"
    local ffmpeg_video_opts="$3"

    local basename_mkv
    basename_mkv=$(basename "$file_path")

    # ---------- UHD Blu-ray: skip transcode ----------
    if [ "$is_uhd" = "true" ] && [ "$UHD_KEEP_ORIGINAL" = "yes" ]; then
        log "$basename_mkv is 4K UHD — skipping transcode (UHD_KEEP_ORIGINAL=yes)"
        return 0
    fi

    local video_codec
    video_codec=$(ffprobe -loglevel error -select_streams v:0 \
        -show_entries stream=codec_name -of csv=p=0 "$file_path" 2>/dev/null | tr -d ',' | tr -d ' ' || true)

    if [ "$video_codec" = "mpeg2video" ]; then
        # Detect interlaced / telecined content
        local field_order vf_filters="" needs_deinterlace="false"
        field_order=$(ffprobe -loglevel error -select_streams v:0 \
            -show_entries stream=field_order -of csv=p=0 "$file_path" 2>/dev/null | tr -d ',' | tr -d ' ' || true)

        if [ "$field_order" = "tt" ] || [ "$field_order" = "bb" ] || [ "$field_order" = "tb" ] || [ "$field_order" = "bt" ]; then
            log "Interlaced ($field_order) per stream metadata"
            needs_deinterlace="true"
        else
            # MPEG-2 DVDs sometimes lie about field order — run idet on 2000 frames
            local idet_result
            idet_result=$(ffmpeg -i "$file_path" -vf "idet" -frames:v 2000 -an -f null - 2>&1 | grep "Multi frame" | tail -1 || true)
            local tff bff prog undet
            tff=$(echo "$idet_result" | grep -oP 'TFF:\s*\K[0-9]+' || echo "0")
            bff=$(echo "$idet_result" | grep -oP 'BFF:\s*\K[0-9]+' || echo "0")
            prog=$(echo "$idet_result" | grep -oP 'Progressive:\s*\K[0-9]+' || echo "0")
            undet=$(echo "$idet_result" | grep -oP 'Undetermined:\s*\K[0-9]+' || echo "0")
            local interlaced_frames=$(( tff + bff ))
            local total_frames=$(( interlaced_frames + prog + undet ))
            log "idet results: TFF=$tff BFF=$bff Prog=$prog Undet=$undet"
            if [ "$interlaced_frames" -gt "$prog" ] 2>/dev/null; then
                log "Detected interlaced content via idet"
                needs_deinterlace="true"
            elif [ "$total_frames" -gt 0 ] && [ "$undet" -gt 0 ] 2>/dev/null; then
                # High undetermined count with low progressive = likely telecine (3:2 pulldown)
                local undet_pct=$(( undet * 100 / total_frames ))
                if [ "$undet_pct" -ge 30 ] 2>/dev/null; then
                    log "Detected likely telecine ($undet_pct% undetermined) — will deinterlace"
                    needs_deinterlace="true"
                fi
            fi
        fi

        if [ "$needs_deinterlace" = "true" ]; then
            if [ -f "$NNEDI3_WEIGHTS" ]; then
                log "Applying nnedi deinterlace + denoise"
                vf_filters="-vf nnedi=weights=${NNEDI3_WEIGHTS}:deint=all:field=af,hqdn3d=3:2:3:2"
            else
                log "nnedi weights not found, using bwdif + denoise"
                vf_filters="-vf bwdif=1:0:0,hqdn3d=3:2:3:2"
            fi
        else
            log "Progressive content — applying light denoise only"
            vf_filters="-vf hqdn3d=3:2:3:2"
        fi

        log "Transcoding $basename_mkv (MPEG-2 → H.265)..."
        local transcode_tmp="${file_path%.mkv}.transcoding.mkv"
        if ffmpeg -i "$file_path" \
            -map 0 \
            $vf_filters \
            $ffmpeg_video_opts \
            -c:a copy \
            -c:s copy \
            -movflags +faststart \
            -y "$transcode_tmp" 2>&1; then
            mv -f "$transcode_tmp" "$file_path"
            log "Transcoded $basename_mkv successfully"
        else
            rm -f "$transcode_tmp"
            # If nnedi failed, retry with bwdif fallback
            if echo "$vf_filters" | grep -q "nnedi"; then
                log "nnedi failed — retrying with bwdif deinterlace"
                vf_filters="-vf bwdif=1:0:0,hqdn3d=3:2:3:2"
                if ffmpeg -i "$file_path" \
                    -map 0 \
                    $vf_filters \
                    $ffmpeg_video_opts \
                    -c:a copy \
                    -c:s copy \
                    -movflags +faststart \
                    -y "$transcode_tmp" 2>&1; then
                    mv -f "$transcode_tmp" "$file_path"
                    log "Transcoded $basename_mkv successfully (bwdif fallback)"
                else
                    log "WARNING: Failed to transcode $basename_mkv (bwdif fallback also failed)"
                    rm -f "$transcode_tmp"
                    return 1
                fi
            else
                log "WARNING: Failed to transcode $basename_mkv"
                return 1
            fi
        fi
    else
        log "$basename_mkv already $video_codec, skipping transcode"
    fi
    return 0
}

# ---------- Process a single job ----------
process_job() {
    local job_file="$1"
    local job_name
    job_name=$(basename "$job_file")

    log "Processing job: $job_name"
    update_worker_status "transcoding" "$job_file" "Reading job..."

    # Parse job JSON (minimal — use grep/sed, no jq dependency)
    local disc_title disc_title_human disc_type source_host title_count is_uhd staging_dir
    disc_title=$(grep -oP '"disc_title"\s*:\s*"\K[^"]+' "$job_file" || echo "")
    disc_title_human=$(grep -oP '"disc_title_human"\s*:\s*"\K[^"]*' "$job_file" || echo "")
    disc_type=$(grep -oP '"disc_type"\s*:\s*"\K[^"]+' "$job_file" || echo "DVD")
    source_host=$(grep -oP '"source_host"\s*:\s*"\K[^"]+' "$job_file" || echo "unknown")
    title_count=$(grep -oP '"title_count"\s*:\s*\K[0-9]+' "$job_file" || echo "0")
    is_uhd=$(grep -oP '"is_uhd"\s*:\s*\K(true|false)' "$job_file" || echo "false")
    staging_dir=$(grep -oP '"staging_dir"\s*:\s*"\K[^"]+' "$job_file" || echo "")

    # Detect multi-file disc job (has files[] array) vs legacy single-file job
    local -a file_paths=()
    local -a title_indices=()
    if grep -q '"files"' "$job_file" 2>/dev/null; then
        # Multi-file disc job — extract file_path entries from files array
        while IFS= read -r fpath; do
            [ -n "$fpath" ] && file_paths+=("$fpath")
        done < <(grep -oP '"file_path"\s*:\s*"\K[^"]+' "$job_file")
        while IFS= read -r tidx; do
            [ -n "$tidx" ] && title_indices+=("$tidx")
        done < <(grep -oP '"title_index"\s*:\s*\K[0-9]+' "$job_file")
    else
        # Legacy single-file job
        local file_path title_index
        file_path=$(grep -oP '"file_path"\s*:\s*"\K[^"]+' "$job_file" || echo "")
        title_index=$(grep -oP '"title_index"\s*:\s*\K[0-9]+' "$job_file" || echo "0")
        file_paths=("$file_path")
        title_indices=("$title_index")
        if [ -z "$staging_dir" ] && [ -n "$file_path" ]; then
            staging_dir=$(dirname "$file_path")
        fi
    fi

    if [ -z "$disc_title" ] || [ ${#file_paths[@]} -eq 0 ] || [ -z "${file_paths[0]}" ]; then
        log "ERROR: Invalid job file $job_name (missing disc_title or files)"
        mv "$job_file" "${job_file%.json}.error"
        return 1
    fi

    # Verify at least one file exists
    local any_exist=false
    for fp in "${file_paths[@]}"; do
        [ -f "$fp" ] && any_exist=true && break
    done
    if ! $any_exist; then
        log "ERROR: No source files exist for $disc_title"
        mv "$job_file" "${job_file%.json}.error"
        return 1
    fi

    # Mark job as in-progress
    mv "$job_file" "${job_file%.json}.processing"
    local processing_file="${job_file%.json}.processing"

    log "Processing disc: $disc_title (${#file_paths[@]} title(s), from $source_host)"

    # Per-disc TV setup (canonicalization + artwork + episode-numbering state).
    # Sets TV_FIRST_EPISODE so per-file rename math is consistent across the disc.
    # If the show name can't be matched against TMDb, sets TV_UNMATCHED=1 and
    # tv_rename_file routes files into _unmatched/<raw-label>/ for human review.
    TV_FIRST_EPISODE=""
    TV_PROGRESS_STATUS=""
    TV_UNMATCHED=""
    TV_UNMATCHED_LABEL=""
    TV_PENDING=""
    TV_PENDING_LABEL=""
    TV_RUNTIME_PLAN=""
    IS_TV_DISC=""
    local _tv_parsed=""
    if parse_tv_disc_title "$disc_title"; then
        _tv_parsed=1
    elif [ -n "$disc_title_human" ] && parse_tv_disc_title "$disc_title_human"; then
        _tv_parsed=1
        log "TV: parsed from disc_title_human '$disc_title_human' (volume label '$disc_title' had no S/D markers)"
    fi
    if [ -n "$_tv_parsed" ]; then
        IS_TV_DISC=1
        local _raw_show="$TV_SHOW"

        # Check overrides FIRST. They can either:
        #   - force a specific TMDb id (skips fuzzy search entirely)
        #   - rewrite the show name before canonicalize_tv_show runs
        local _matched=""
        if declare -F tv_apply_overrides >/dev/null 2>&1 \
           && tv_apply_overrides "$_raw_show" "$disc_title"; then
            if [ -n "$TV_OVERRIDE_TMDB_ID" ] && declare -F tmdb_get_show_by_id >/dev/null 2>&1; then
                if tmdb_get_show_by_id "$TV_OVERRIDE_TMDB_ID"; then
                    TV_SHOW="${TV_OVERRIDE_SHOW:-$TMDB_SHOW_NAME}"
                    log "TV: override pinned TMDb id $TV_OVERRIDE_TMDB_ID → '$TV_SHOW'"
                    _matched=1
                else
                    log "TV: override TMDb id $TV_OVERRIDE_TMDB_ID failed to load; falling back to search"
                fi
            fi
            # Name override but no id (or id failed) — feed override name into canonicalize
            if [ -z "$_matched" ] && [ -n "$TV_OVERRIDE_SHOW" ]; then
                if canonicalize_tv_show "$TV_OVERRIDE_SHOW"; then
                    TV_SHOW="$TV_SHOW_CANONICAL"
                    log "TV: override name '$TV_OVERRIDE_SHOW' → canonical '$TV_SHOW' (TMDb id $TV_SHOW_TMDB_ID)"
                    _matched=1
                fi
            fi
        fi

        # No override (or override failed): try canonicalize on the raw name
        if [ -z "$_matched" ]; then
            if canonicalize_tv_show "$_raw_show"; then
                if [ "$TV_SHOW_CANONICAL" != "$_raw_show" ]; then
                    log "TV: canonicalized show name '$_raw_show' → '$TV_SHOW_CANONICAL' (TMDb id $TV_SHOW_TMDB_ID)"
                fi
                TV_SHOW="$TV_SHOW_CANONICAL"
                _matched=1
            fi
        fi

        if [ -n "$_matched" ]; then
            fetch_tv_artwork "$TV_SHOW" "$TV_SEASON" || true

            if declare -F tv_progress_for_disc >/dev/null 2>&1 \
               && tv_progress_for_disc "$TV_SHOW" "$TV_SEASON" "$TV_DISC" "${#file_paths[@]}"; then
                log "TV: $TV_SHOW S${TV_SEASON}D${TV_DISC} → episodes start at E$(printf '%02d' "$TV_FIRST_EPISODE") (status=$TV_PROGRESS_STATUS)"
                if [ "$TV_PROGRESS_STATUS" = "gap" ]; then
                    log "WARNING: out-of-order disc — episode numbering is a best-guess; review before approving"
                fi
            else
                # Fallback to legacy global-config math
                TV_FIRST_EPISODE=$(( (TV_DISC - 1) * EPISODES_PER_DISC + 1 ))
                log "TV: progress lib unavailable, using legacy math (first episode = E$(printf '%02d' "$TV_FIRST_EPISODE"))"
            fi

            # Production-vs-aired runtime sanity check.  Loads the season's
            # episode runtimes from TMDb and compares them to the actual
            # MKV durations.  If 3+ disagree, the disc is parked in
            # _pending/ for human review (probably production-order or
            # extra/missing episodes).
            if declare -F tv_check_runtime_match >/dev/null 2>&1 \
               && [ -n "$TV_FIRST_EPISODE" ] \
               && [ "$TV_PROGRESS_STATUS" != "gap" ]; then
                # Ensure TMDB_EP_RUNTIMES is populated for this season
                tmdb_fetch_season_by_name "$TV_SHOW" "$TV_SEASON" 2>/dev/null || true
                tv_check_runtime_match "$TV_FIRST_EPISODE" "${file_paths[@]}"
                if [ -n "$TV_RUNTIME_MISMATCH" ]; then
                    log "WARNING: runtime mismatch detected — routing disc to _pending/ for review"
                    TV_PENDING=1
                    TV_PENDING_LABEL=$(printf "%s-S%02dD%02d" "$TV_SHOW" "$TV_SEASON" "$TV_DISC")
                    # Drop a plan file in the pending dir alongside the files
                    local _pending_dir="$UNREVIEWED_TV/_pending/$TV_PENDING_LABEL"
                    mkdir -p "$_pending_dir"
                    {
                        echo "Disc:        $disc_title"
                        echo "Show:        $TV_SHOW (TMDb id ${TV_SHOW_TMDB_ID:-?})"
                        echo "Season:      $TV_SEASON"
                        echo "Disc#:       $TV_DISC"
                        echo "First ep:    E$(printf '%02d' "$TV_FIRST_EPISODE") (per progress state)"
                        echo "Generated:   $(date -Iseconds 2>/dev/null || date)"
                        echo
                        echo "Reason: TMDb-reported episode runtimes disagree with actual MKV"
                        echo "durations.  This is the typical signature of a disc pressed in"
                        echo "production order rather than aired order, OR a disc with bonus"
                        echo "content TMDb doesn't list."
                        echo
                        printf '%s' "$TV_RUNTIME_PLAN"
                        echo
                        echo "Action: review file lengths against the planned mapping above,"
                        echo "rename manually, then move into Video/TV/$TV_SHOW/Season $(printf '%02d' "$TV_SEASON")/."
                    } > "$_pending_dir/episodes-plan.txt"
                fi
            fi
        else
            log "TV: could not match show '$_raw_show' on TMDb (no override available) — routing disc to _unmatched/"
            TV_UNMATCHED=1
            TV_UNMATCHED_LABEL="$disc_title"
        fi
    fi

    # Detect GPU
    local ffmpeg_video_opts="-c:v libx265 -crf 24 -preset medium"
    if command -v nvidia-smi >/dev/null 2>&1 && nvidia-smi >/dev/null 2>&1; then
        log "Using NVIDIA NVENC hardware encoding"
        ffmpeg_video_opts="-c:v hevc_nvenc -preset medium -rc constqp -qp 24"
    fi

    local file_num=0
    local total_files=${#file_paths[@]}
    for i in "${!file_paths[@]}"; do
        local fp="${file_paths[$i]}"
        local tidx="${title_indices[$i]:-$((i+1))}"
        file_num=$((file_num + 1))

        if [ ! -f "$fp" ]; then
            log "WARNING: File $fp does not exist, skipping"
            continue
        fi

        local basename_mkv
        basename_mkv=$(basename "$fp")
        update_worker_status "transcoding" "$processing_file" "[$file_num/$total_files] $basename_mkv"

        if ! transcode_file "$fp" "$is_uhd" "$ffmpeg_video_opts"; then
            log "WARNING: Failed to transcode $basename_mkv, continuing"
        fi

        # Rename/move file to final location
        update_worker_status "renaming" "$processing_file" "[$file_num/$total_files] Organizing..."
        if [ -n "$IS_TV_DISC" ]; then
            if [ -n "$TV_UNMATCHED" ]; then
                log "TV disc (unmatched): $disc_title — title $tidx → _unmatched/"
            elif [ -n "$TV_PENDING" ]; then
                log "TV disc (pending review): $TV_SHOW S${TV_SEASON}D${TV_DISC} — title $tidx → _pending/"
            else
                log "TV disc: $TV_SHOW Season $TV_SEASON Disc $TV_DISC — episode from title $tidx"
            fi
            tv_rename_file "$fp" "$tidx"
        else
            log "Movie disc: $disc_title"
            movie_rename_file "$fp" "$disc_title"
        fi
    done

    # If any titles on this TV disc were classified as runtime-outlier
    # extras and routed to <Show>/Extras/, the recorded episode_count
    # for this disc (set earlier by tv_progress_for_disc using the raw
    # title count) overcounts.  Amend it so the next disc starts at the
    # correct episode number.
    if [ -n "$IS_TV_DISC" ] \
       && [ -z "$TV_UNMATCHED" ] && [ -z "$TV_PENDING" ] \
       && [ -n "$TV_FIRST_EPISODE" ] \
       && declare -F tv_runtime_verdict >/dev/null 2>&1 \
       && declare -F tv_progress_amend_disc >/dev/null 2>&1; then
        local _extras=0 _i
        for (( _i=1; _i<=${#file_paths[@]}; _i++ )); do
            if [ "$(tv_runtime_verdict "$_i")" = "extra" ]; then
                _extras=$(( _extras + 1 ))
            fi
        done
        if [ "$_extras" -gt 0 ]; then
            local _real=$(( ${#file_paths[@]} - _extras ))
            log "TV: $_extras title(s) routed to Extras/; amending disc $TV_DISC episode_count → $_real"
            tv_progress_amend_disc "$TV_SHOW" "$TV_SEASON" "$TV_DISC" "$_real" || true
        fi
    fi

    # Determine the unreviewed output directory for metadata.json
    local unreviewed_dest=""
    if [ -n "${TV_UNMATCHED:-}" ] && [ -n "${TV_UNMATCHED_LABEL:-}" ]; then
        unreviewed_dest="$UNREVIEWED_TV/_unmatched/$TV_UNMATCHED_LABEL"
    elif [ -n "${TV_PENDING:-}" ] && [ -n "${TV_PENDING_LABEL:-}" ]; then
        unreviewed_dest="$UNREVIEWED_TV/_pending/$TV_PENDING_LABEL"
    elif [ -n "$IS_TV_DISC" ]; then
        local season_dir
        season_dir=$(printf "Season %02d" "$TV_SEASON")
        unreviewed_dest="$UNREVIEWED_TV/$TV_SHOW/$season_dir"
    else
        # For movies, find where mnamer/fallback placed files
        # Use the last file's parent directory
        for fp_check in "${file_paths[@]}"; do
            # Files were moved by rename functions; scan unreviewed movies for recent entries
            true
        done
        # Fallback: use disc_title as the folder
        unreviewed_dest="$UNREVIEWED_MOVIES/$disc_title"
    fi

    # Write metadata.json for the review app
    if [ -n "$unreviewed_dest" ] && [ -d "$unreviewed_dest" ]; then
        python3 -c "
import json, sys, datetime
with open(sys.argv[1], 'r') as f:
    data = json.load(f)
data['_unreviewed_dir'] = sys.argv[2]
data['_processed_at'] = datetime.datetime.now().isoformat()
with open(sys.argv[2] + '/metadata.json', 'w') as f:
    json.dump(data, f, indent=2)
" "$processing_file" "$unreviewed_dest" 2>/dev/null || true
    fi

    # Clean staging — files have been moved to the unreviewed dir
    if [ -n "$staging_dir" ] && [ -d "$staging_dir" ]; then
        local dir_size
        dir_size=$(du -sh "$staging_dir" 2>/dev/null | cut -f1)
        log "Cleaning staging: $staging_dir ($dir_size)"
        rm -rf "$staging_dir"
        local parent_dir
        parent_dir=$(dirname "$staging_dir")
        if [ -d "$parent_dir" ] && [ "$(basename "$parent_dir")" != ".autorip-staging" ]; then
            rmdir "$parent_dir" 2>/dev/null || true
        fi
    fi

    # Remove the processing file — unreviewed dir metadata.json is now the record
    rm -f "$processing_file"
    log "Job complete (pending review in unreviewed): $disc_title ($total_files title(s))"
}

# ==========================================================================
# Subcommands
# ==========================================================================

# ---------- clean: approve all unreviewed items and move to library ----------
clean_reviewed() {
    local review_count=0

    # Scan the unreviewed directory tree for metadata.json files
    while IFS= read -r meta_file; do
        [ -f "$meta_file" ] || continue
        review_count=$((review_count + 1))

        local item_dir
        item_dir=$(dirname "$meta_file")
        local rel_path="${item_dir#$UNREVIEWED_DIR/}"
        local final_dir="$OUTPUT_BASE/$rel_path"

        # Determine job type from metadata
        local job_type
        job_type=$(grep -oP '"job_type"\s*:\s*"\K[^"]+' "$meta_file" 2>/dev/null || echo "video")

        log "Approving: $rel_path"
        mkdir -p "$final_dir"

        # Move all non-metadata files to the final library location
        find "$item_dir" -maxdepth 1 -type f ! -name "metadata.json" -exec mv -f {} "$final_dir/" \;

        # Make library files accessible
        chown -R autorip:autorip "$final_dir" 2>/dev/null || true
        chmod 777 "$final_dir" 2>/dev/null || true
        chmod 666 "$final_dir"/* 2>/dev/null || true

        # Log audio rips to rip history
        if [ "$job_type" = "audio-cd" ]; then
            local artist album tracks_json cover_rel=""
            artist=$(grep -oP '"artist"\s*:\s*"\K[^"]+' "$meta_file" 2>/dev/null || true)
            album=$(grep -oP '"album"\s*:\s*"\K[^"]+' "$meta_file" 2>/dev/null || true)
            tracks_json=$(python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    job = json.load(f)
print(json.dumps(job.get('tracks', [])))
" "$meta_file" 2>/dev/null || echo "[]")
            if [ -f "$final_dir/cover.jpg" ]; then
                cover_rel="${rel_path}/cover.jpg"
            fi
            if [ -n "$artist" ] && [ -n "$album" ]; then
                log_rip_entry "Audio CD" "$artist" "$album" "$tracks_json" "$cover_rel"
            fi
        fi

        # Remove the unreviewed item directory (now empty except metadata.json)
        rm -rf "$item_dir"
        # Clean empty parent directories
        local parent_dir
        parent_dir=$(dirname "$item_dir")
        while [ "$parent_dir" != "$UNREVIEWED_DIR" ] && [ -d "$parent_dir" ]; do
            rmdir "$parent_dir" 2>/dev/null || break
            parent_dir=$(dirname "$parent_dir")
        done
    done < <(find "$UNREVIEWED_DIR" -name "metadata.json" -type f 2>/dev/null | sort)

    if [ "$review_count" -eq 0 ]; then
        log "No unreviewed items to approve."
    else
        log "Approved $review_count item(s)."
    fi
}

# ---------- list: show unreviewed items ----------
list_reviewed() {
    local count=0
    while IFS= read -r meta_file; do
        [ -f "$meta_file" ] || continue
        count=$((count + 1))

        local item_dir
        item_dir=$(dirname "$meta_file")
        local rel_path="${item_dir#$UNREVIEWED_DIR/}"
        local dir_size
        dir_size=$(du -sh "$item_dir" 2>/dev/null | cut -f1)

        local job_type artist album disc_title
        job_type=$(grep -oP '"job_type"\s*:\s*"\K[^"]+' "$meta_file" 2>/dev/null || echo "video")
        disc_title=$(grep -oP '"disc_title"\s*:\s*"\K[^"]+' "$meta_file" 2>/dev/null || true)
        artist=$(grep -oP '"artist"\s*:\s*"\K[^"]+' "$meta_file" 2>/dev/null || true)
        album=$(grep -oP '"album"\s*:\s*"\K[^"]+' "$meta_file" 2>/dev/null || true)

        if [ "$job_type" = "audio-cd" ]; then
            echo "  [$count] Audio CD: $artist / $album"
        else
            local file_count
            file_count=$(find "$item_dir" -maxdepth 1 -name "*.mkv" 2>/dev/null | wc -l)
            echo "  [$count] Video: ${disc_title:-$rel_path} ($file_count file(s))"
        fi
        echo "       Path: $rel_path ($dir_size)"
    done < <(find "$UNREVIEWED_DIR" -name "metadata.json" -type f 2>/dev/null | sort)

    if [ "$count" -eq 0 ]; then
        echo "No items pending review."
    else
        echo ""
        echo "$count item(s) pending review."
        echo "Run '$0 clean' to approve all and move to library."
    fi
}

# ---------- approve: approve a single unreviewed item ----------
approve_single() {
    local item_path="$1"

    # item_path is relative to UNREVIEWED_DIR, e.g. "Video/Movies/Apollo 13 (1995)"
    local item_dir="$UNREVIEWED_DIR/$item_path"
    local meta_file="$item_dir/metadata.json"

    if [ ! -f "$meta_file" ]; then
        echo "ERROR: No unreviewed item found at: $item_path" >&2
        exit 1
    fi

    local rel_path="$item_path"
    local final_dir="$OUTPUT_BASE/$rel_path"

    log "Approving: $rel_path"
    mkdir -p "$final_dir"

    # Move all non-metadata files to the final library location
    find "$item_dir" -maxdepth 1 -type f ! -name "metadata.json" -exec mv -f {} "$final_dir/" \;

    # Make library files accessible
    chown -R autorip:autorip "$final_dir" 2>/dev/null || true
    chmod 777 "$final_dir" 2>/dev/null || true
    chmod 666 "$final_dir"/* 2>/dev/null || true

    # Log audio rips to rip history
    local job_type
    job_type=$(grep -oP '"job_type"\s*:\s*"\K[^"]+' "$meta_file" 2>/dev/null || echo "video")
    if [ "$job_type" = "audio-cd" ]; then
        local artist album tracks_json cover_rel=""
        artist=$(grep -oP '"artist"\s*:\s*"\K[^"]+' "$meta_file" 2>/dev/null || true)
        album=$(grep -oP '"album"\s*:\s*"\K[^"]+' "$meta_file" 2>/dev/null || true)
        tracks_json=$(python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    job = json.load(f)
print(json.dumps(job.get('tracks', [])))
" "$meta_file" 2>/dev/null || echo "[]")
        if [ -f "$final_dir/cover.jpg" ]; then
            cover_rel="${rel_path}/cover.jpg"
        fi
        if [ -n "$artist" ] && [ -n "$album" ]; then
            log_rip_entry "Audio CD" "$artist" "$album" "$tracks_json" "$cover_rel"
        fi
    fi

    # Remove the unreviewed item directory
    rm -rf "$item_dir"
    # Clean empty parent directories
    local parent_dir
    parent_dir=$(dirname "$item_dir")
    while [ "$parent_dir" != "$UNREVIEWED_DIR" ] && [ -d "$parent_dir" ]; do
        rmdir "$parent_dir" 2>/dev/null || break
        parent_dir=$(dirname "$parent_dir")
    done

    log "Approved: $rel_path"
    echo "OK"
}

# ---------- reject: reject a single unreviewed item ----------
reject_single() {
    local item_path="$1"

    local item_dir="$UNREVIEWED_DIR/$item_path"
    local meta_file="$item_dir/metadata.json"

    if [ ! -f "$meta_file" ]; then
        echo "ERROR: No unreviewed item found at: $item_path" >&2
        exit 1
    fi

    log "Rejecting: $item_path"

    # Remove the entire unreviewed item directory
    rm -rf "$item_dir"
    # Clean empty parent directories
    local parent_dir
    parent_dir=$(dirname "$item_dir")
    while [ "$parent_dir" != "$UNREVIEWED_DIR" ] && [ -d "$parent_dir" ]; do
        rmdir "$parent_dir" 2>/dev/null || break
        parent_dir=$(dirname "$parent_dir")
    done

    log "Rejected and cleaned: $item_path"
    echo "OK"
}

# ---------- Handle subcommands ----------
case "${1:-}" in
    clean)
        mkdir -p "$QUEUE_DIR" "$UNREVIEWED_DIR"
        log "Approving all unreviewed items..."
        clean_reviewed
        exit 0
        ;;
    approve)
        mkdir -p "$QUEUE_DIR" "$UNREVIEWED_DIR"
        if [ -z "${2:-}" ]; then
            echo "Usage: $0 approve <item_path>" >&2
            echo "  item_path is relative to .unreviewed/, e.g. Video/Movies/Apollo 13 (1995)" >&2
            exit 1
        fi
        approve_single "$2"
        exit 0
        ;;
    reject)
        mkdir -p "$QUEUE_DIR" "$UNREVIEWED_DIR"
        if [ -z "${2:-}" ]; then
            echo "Usage: $0 reject <item_path>" >&2
            echo "  item_path is relative to .unreviewed/, e.g. Video/Movies/Apollo 13 (1995)" >&2
            exit 1
        fi
        reject_single "$2"
        exit 0
        ;;
    list|review|status)
        mkdir -p "$QUEUE_DIR" "$UNREVIEWED_DIR"
        echo "=== Items pending review ==="
        list_reviewed
        exit 0
        ;;
    ""|-*)
        # No subcommand or flags — fall through to normal job processing
        ;;
    *)
        echo "Usage: $0 [clean|list|approve <path>|reject <path>]" >&2
        echo "  (no args)  Process pending queue jobs" >&2
        echo "  list       Show items pending review" >&2
        echo "  clean      Approve all and move to library" >&2
        echo "  approve    Approve a single item (path relative to .unreviewed/)" >&2
        echo "  reject     Reject a single item (deletes from unreviewed)" >&2
        exit 1
        ;;
esac

# ==========================================================================
# Main loop — process all pending jobs then exit
# ==========================================================================
mkdir -p "$QUEUE_DIR"
mkdir -p "$UNREVIEWED_DIR" "$UNREVIEWED_MOVIES" "$UNREVIEWED_TV" "$UNREVIEWED_MUSIC"

# Prevent concurrent worker instances (timer may fire while processing)
LOCKFILE="/tmp/transcode-worker.lock"
exec 200>"$LOCKFILE"
if ! flock -n 200; then
    log "Another transcode worker is already running, exiting."
    exit 0
fi

log "Transcode worker starting — checking queue..."
update_worker_status "scanning" "" "Checking for jobs..."

job_count=0
for job_file in "$QUEUE_DIR"/*.json; do
    [ -f "$job_file" ] || continue
    job_count=$((job_count + 1))

    # Dispatch based on job_type
    local_job_type=$(grep -oP '"job_type"\s*:\s*"\K[^"]+' "$job_file" 2>/dev/null || echo "")
    case "$local_job_type" in
        audio-cd)
            process_audio_cd_job "$job_file" || true
            ;;
        *)
            # Default: video transcode job (backwards compatible)
            process_job "$job_file" || true
            ;;
    esac
done

if [ "$job_count" -eq 0 ]; then
    log "No jobs in queue."
else
    log "Processed $job_count job(s)."
fi

update_worker_status "idle" "" ""
log "Worker done."
