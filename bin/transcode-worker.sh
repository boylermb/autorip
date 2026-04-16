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

    return 1
}

# ---------- TV rename (single file) ----------
tv_rename_file() {
    local mkv="$1"
    local title_index="$2"   # 1-indexed
    local season_dir
    season_dir=$(printf "Season %02d" "$TV_SEASON")
    local dest_dir="$UNREVIEWED_TV/$TV_SHOW/$season_dir"
    mkdir -p "$dest_dir"

    # Episode number: (disc - 1) * episodes_per_disc + title_index
    local ep_num=$(( (TV_DISC - 1) * EPISODES_PER_DISC + title_index ))
    local ep_name
    ep_name=$(printf "%s - S%02dE%02d.mkv" "$TV_SHOW" "$TV_SEASON" "$ep_num")
    mv -f "$mkv" "$dest_dir/$ep_name"
    log "Moved $(basename "$mkv") → $dest_dir/$ep_name"
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
        # Detect interlaced content
        local field_order vf_filters=""
        field_order=$(ffprobe -loglevel error -select_streams v:0 \
            -show_entries stream=field_order -of csv=p=0 "$file_path" 2>/dev/null | tr -d ',' | tr -d ' ' || true)

        if [ "$field_order" = "tt" ] || [ "$field_order" = "bb" ] || [ "$field_order" = "tb" ] || [ "$field_order" = "bt" ]; then
            if [ -f "$NNEDI3_WEIGHTS" ]; then
                log "Interlaced ($field_order) — applying nnedi deinterlace + denoise"
                vf_filters="-vf nnedi=weights=${NNEDI3_WEIGHTS}:deint=all:field=af,hqdn3d=3:2:3:2"
            else
                log "Interlaced ($field_order) — nnedi weights not found, using bwdif + denoise"
                vf_filters="-vf bwdif=1:0:0,hqdn3d=3:2:3:2"
            fi
        else
            # Not flagged as interlaced, but MPEG-2 DVDs sometimes lie — run idet
            local idet_result
            idet_result=$(ffmpeg -i "$file_path" -vf "idet" -frames:v 500 -an -f null - 2>&1 | grep "Multi frame" | tail -1 || true)
            local tff bff prog
            tff=$(echo "$idet_result" | grep -oP 'TFF:\s*\K[0-9]+' || echo "0")
            bff=$(echo "$idet_result" | grep -oP 'BFF:\s*\K[0-9]+' || echo "0")
            prog=$(echo "$idet_result" | grep -oP 'Progressive:\s*\K[0-9]+' || echo "0")
            local interlaced_frames=$(( tff + bff ))
            if [ "$interlaced_frames" -gt "$prog" ] 2>/dev/null; then
                if [ -f "$NNEDI3_WEIGHTS" ]; then
                    log "Detected interlaced content (idet: TFF=$tff BFF=$bff Prog=$prog) — applying nnedi + denoise"
                    vf_filters="-vf nnedi=weights=${NNEDI3_WEIGHTS}:deint=all:field=af,hqdn3d=3:2:3:2"
                else
                    log "Detected interlaced (idet) — nnedi weights not found, using bwdif + denoise"
                    vf_filters="-vf bwdif=1:0:0,hqdn3d=3:2:3:2"
                fi
            else
                log "Progressive content — applying light denoise only"
                vf_filters="-vf hqdn3d=3:2:3:2"
            fi
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
            log "WARNING: Failed to transcode $basename_mkv"
            rm -f "$transcode_tmp"
            return 1
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
    local disc_title disc_type source_host title_count is_uhd staging_dir
    disc_title=$(grep -oP '"disc_title"\s*:\s*"\K[^"]+' "$job_file" || echo "")
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
        if parse_tv_disc_title "$disc_title"; then
            log "TV disc: $TV_SHOW Season $TV_SEASON Disc $TV_DISC — episode from title $tidx"
            tv_rename_file "$fp" "$tidx"
        else
            log "Movie disc: $disc_title"
            movie_rename_file "$fp" "$disc_title"
        fi
    done

    # Determine the unreviewed output directory for metadata.json
    local unreviewed_dest=""
    if parse_tv_disc_title "$disc_title"; then
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
