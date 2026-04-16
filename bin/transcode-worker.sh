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
# Staging files are preserved after processing for review. Use the `list`
# and `clean` subcommands to inspect and purge them.
#
# Usage:
#   transcode-worker.sh          Process pending queue jobs (systemd timer)
#   transcode-worker.sh list     Show jobs pending review
#   transcode-worker.sh clean    Purge reviewed staging dirs
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
RIP_LOG="$OUTPUT_BASE/.rip-log.json"
RIP_LOG_MD="$OUTPUT_BASE/.rip-log.md"
STATUS_FILE="$OUTPUT_BASE/.autorip-queue/.worker-status.json"
HOSTNAME=$(hostname)

# mnamer settings (movies only)
MNAMER_MOVIE_DIR="$OUTPUT_BASE/Video/Movies"
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
    local dest_dir="$TV_DIR/$TV_SHOW/$season_dir"
    mkdir -p "$dest_dir"

    # Episode number: (disc - 1) * episodes_per_disc + title_index
    local ep_num=$(( (TV_DISC - 1) * EPISODES_PER_DISC + title_index ))
    local ep_name
    ep_name=$(printf "%s - S%02dE%02d.mkv" "$TV_SHOW" "$TV_SEASON" "$ep_num")
    cp -f "$mkv" "$dest_dir/$ep_name"
    log "Copied $(basename "$mkv") → $ep_name (staging kept for review)"
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

    # Copy to a temp file so mnamer can move it while we keep the staging original
    local tmp_copy="${mkv%.mkv}.mnamer-copy.mkv"
    cp -f "$mkv" "$tmp_copy"

    log "Running mnamer for movie file: $(basename "$mkv")"
    if mnamer --batch \
        --media=movie \
        --movie-api=tmdb \
        --movie-directory="$MNAMER_MOVIE_DIR" \
        --movie-format="$MNAMER_MOVIE_FORMAT" \
        "$tmp_copy" 2>&1; then
        if [ -f "$tmp_copy" ]; then
            log "mnamer did not match $(basename "$mkv"), using fallback"
            rm -f "$tmp_copy"
            movie_fallback_file "$mkv" "$disc_title"
        else
            log "mnamer matched and moved $(basename "$mkv") (staging kept for review)"
        fi
    else
        log "mnamer failed, using fallback"
        rm -f "$tmp_copy"
        movie_fallback_file "$mkv" "$disc_title"
    fi
}

movie_fallback_file() {
    local mkv="$1"
    local disc_title="$2"
    local fallback_dir="$MOVIES_DIR/$disc_title"
    mkdir -p "$fallback_dir"
    cp -f "$mkv" "$fallback_dir/"
    log "Copied $(basename "$mkv") to $fallback_dir (staging kept for review)"
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

    # Build final destination: Music/Artist/Album/
    local safe_artist safe_album
    safe_artist=$(echo "$artist" | sed -e 's/^\.*//' | tr -d ':><|*/"'"'"'?\\!')
    safe_album=$(echo "$album" | sed -e 's/^\.*//' | tr -d ':><|*/"'"'"'?\\!')
    local final_dir="$MUSIC_DIR/$safe_artist/$safe_album"
    mkdir -p "$final_dir"
    # Ensure the artist directory is also accessible
    chown autorip:autorip "$MUSIC_DIR/$safe_artist" 2>/dev/null || true
    chmod 777 "$MUSIC_DIR/$safe_artist" 2>/dev/null || true

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

    # Rip log entry is deferred until review is approved (clean subcommand).

    # Keep staging directory for review — files are copied, not moved.
    # Run `transcode-worker.sh clean` to purge reviewed staging dirs.
    log "Staging kept for review: $staging_dir"

    # Record original library destination so approve can clean it up if metadata is edited
    python3 -c "
import json, sys
with open(sys.argv[1], 'r') as f:
    data = json.load(f)
data['_original_dest'] = sys.argv[2]
with open(sys.argv[1], 'w') as f:
    json.dump(data, f, indent=2)
" "$processing_file" "$final_dir" 2>/dev/null || true

    # Mark job as needing review (staging still present)
    mv "$processing_file" "${processing_file%.processing}.review"
    log "Audio CD job complete (pending review): $artist / $album ($file_num tracks)"
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

    # Keep staging directory for review — files are copied, not moved.
    log "Staging kept for review: $staging_dir"

    # Mark job as needing review (staging still present)
    mv "$processing_file" "${processing_file%.processing}.review"
    log "Job complete (pending review): $disc_title ($total_files title(s))"
}

# ==========================================================================
# Subcommands
# ==========================================================================

# ---------- clean: purge reviewed staging dirs and .review job files ----------
clean_reviewed() {
    local review_count=0
    local bytes_freed=0

    for review_file in "$QUEUE_DIR"/*.review; do
        [ -f "$review_file" ] || continue
        review_count=$((review_count + 1))

        # Try to find the staging dir from the job
        local staging_dir=""
        staging_dir=$(grep -oP '"staging_dir"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || true)
        if [ -z "$staging_dir" ]; then
            # Video jobs store file_path; staging dir is the parent
            local file_path
            file_path=$(grep -oP '"file_path"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || true)
            if [ -n "$file_path" ]; then
                staging_dir=$(dirname "$file_path")
            fi
        fi

        local job_desc
        job_desc=$(basename "$review_file")

        if [ -n "$staging_dir" ] && [ -d "$staging_dir" ]; then
            local dir_size
            dir_size=$(du -sh "$staging_dir" 2>/dev/null | cut -f1)
            log "Cleaning: $job_desc → $staging_dir ($dir_size)"
            rm -rf "$staging_dir"
            # Clean up empty parent (e.g. .autorip-staging/Artist/ or .autorip-staging/DISC_TITLE/)
            local parent_dir
            parent_dir=$(dirname "$staging_dir")
            if [ -d "$parent_dir" ] && [ "$(basename "$parent_dir")" != ".autorip-staging" ]; then
                rmdir "$parent_dir" 2>/dev/null || true
            fi
        else
            log "Cleaning: $job_desc (no staging dir to remove)"
        fi

        # Record in rip history now that review is approved
        local job_type="" artist="" album="" format="" tracks_json=""
        job_type=$(grep -oP '"job_type"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || echo "video")
        artist=$(grep -oP '"artist"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || true)
        album=$(grep -oP '"album"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || true)
        format=$(grep -oP '"format"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || echo "mp3")

        if [ "$job_type" = "audio-cd" ] && [ -n "$artist" ] && [ -n "$album" ]; then
            # Extract tracks JSON array using python3
            tracks_json=$(python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    job = json.load(f)
print(json.dumps(job.get('tracks', [])))
" "$review_file" 2>/dev/null || echo "[]")

            local safe_artist safe_album cover_rel=""
            safe_artist=$(echo "$artist" | sed -e 's/^\.*//' | tr -d ':><|*/"'"'"'?\\!')
            safe_album=$(echo "$album" | sed -e 's/^\.*//' | tr -d ':><|*/"'"'"'?\\!')
            if [ -f "$MUSIC_DIR/$safe_artist/$safe_album/cover.jpg" ]; then
                cover_rel="Audio/Music/$safe_artist/$safe_album/cover.jpg"
            fi
            log_rip_entry "Audio CD" "$artist" "$album" "$tracks_json" "$cover_rel"
        fi

        rm -f "$review_file"
    done

    if [ "$review_count" -eq 0 ]; then
        log "No reviewed jobs to clean up."
    else
        log "Cleaned $review_count reviewed job(s)."
    fi
}

# ---------- list: show pending review jobs ----------
list_reviewed() {
    local count=0
    for review_file in "$QUEUE_DIR"/*.review; do
        [ -f "$review_file" ] || continue
        count=$((count + 1))

        local staging_dir="" file_path="" disc_title="" artist="" album="" job_type=""
        job_type=$(grep -oP '"job_type"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || echo "video")
        disc_title=$(grep -oP '"disc_title"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || true)
        artist=$(grep -oP '"artist"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || true)
        album=$(grep -oP '"album"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || true)
        staging_dir=$(grep -oP '"staging_dir"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || true)
        if [ -z "$staging_dir" ]; then
            file_path=$(grep -oP '"file_path"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || true)
            staging_dir=$(dirname "$file_path" 2>/dev/null || echo "?")
        fi

        local dir_status="MISSING"
        local dir_size=""
        if [ -d "$staging_dir" ]; then
            dir_status="OK"
            dir_size=" ($(du -sh "$staging_dir" 2>/dev/null | cut -f1))"
        fi

        if [ "$job_type" = "audio-cd" ]; then
            echo "  [$count] Audio CD: $artist / $album"
        else
            # Count files in multi-file disc jobs
            local file_count
            file_count=$(grep -c '"file_path"' "$review_file" 2>/dev/null || echo "1")
            echo "  [$count] Video: $disc_title ($file_count title(s))"
        fi
        echo "       Staging: $staging_dir [$dir_status]$dir_size"
        echo "       Job: $(basename "$review_file")"
    done

    if [ "$count" -eq 0 ]; then
        echo "No jobs pending review."
    else
        echo ""
        echo "$count job(s) pending review. Run '$0 clean' to purge staging files."
    fi
}

# ---------- approve: approve a single reviewed job ----------
approve_single() {
    local job_id="$1"
    local review_file=""

    # Find the .review file matching job_id (match by prefix or full name)
    for f in "$QUEUE_DIR"/*.review; do
        [ -f "$f" ] || continue
        local base
        base=$(basename "$f")
        if [ "$base" = "$job_id" ] || [ "$base" = "${job_id}.review" ] || [ "${base%.review}" = "$job_id" ]; then
            review_file="$f"
            break
        fi
    done

    if [ -z "$review_file" ]; then
        echo "ERROR: No review job found matching: $job_id" >&2
        exit 1
    fi

    log "Approving single job: $(basename "$review_file")"

    # Extract job info
    local job_type="" artist="" album="" staging_dir="" file_path="" format=""
    job_type=$(grep -oP '"job_type"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || echo "video")
    artist=$(grep -oP '"artist"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || true)
    album=$(grep -oP '"album"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || true)
    staging_dir=$(grep -oP '"staging_dir"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || true)
    format=$(grep -oP '"format"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || echo "mp3")

    if [ -z "$staging_dir" ]; then
        file_path=$(grep -oP '"file_path"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || true)
        staging_dir=$(dirname "$file_path" 2>/dev/null || echo "")
    fi

    # Re-process from staging with (potentially edited) metadata
    if [ "$job_type" = "audio-cd" ] && [ -n "$artist" ] && [ -n "$album" ] && [ -n "$staging_dir" ] && [ -d "$staging_dir" ]; then
        local tracks_json
        tracks_json=$(python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    job = json.load(f)
print(json.dumps(job.get('tracks', [])))
" "$review_file" 2>/dev/null || echo "[]")

        local ext="$format"
        local safe_artist safe_album
        safe_artist=$(echo "$artist" | sed -e 's/^\.*//' | tr -d ':><|*/"'"'"'?\\!')
        safe_album=$(echo "$album" | sed -e 's/^\.*//' | tr -d ':><|*/"'"'"'?\\!')

        # Remove old library output (in case metadata was edited and dest changed)
        local final_dir="$MUSIC_DIR/$safe_artist/$safe_album"

        # Determine the old library directory to clean up.
        # Primary: _original_dest recorded during process_audio_cd_job.
        # Fallback: compute from _original_artist/_original_album saved on first edit.
        local original_dir=""
        original_dir=$(grep -oP '"_original_dest"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || true)
        if [ -z "$original_dir" ]; then
            # Fallback: compute from original (pre-edit) artist/album if available
            local orig_artist orig_album
            orig_artist=$(grep -oP '"_original_artist"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || true)
            orig_album=$(grep -oP '"_original_album"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || true)
            if [ -n "$orig_artist" ] && [ -n "$orig_album" ]; then
                local safe_orig_artist safe_orig_album
                safe_orig_artist=$(echo "$orig_artist" | sed -e 's/^\.*//' | tr -d ':><|*/"'"'"'?\\!')
                safe_orig_album=$(echo "$orig_album" | sed -e 's/^\.*//' | tr -d ':><|*/"'"'"'?\\!')
                original_dir="$MUSIC_DIR/$safe_orig_artist/$safe_orig_album"
            fi
        fi
        if [ -n "$original_dir" ] && [ -d "$original_dir" ] && [ "$original_dir" != "$final_dir" ]; then
            log "Removing old library dir (metadata was edited): $original_dir"
            rm -rf "$original_dir"
            local old_parent
            old_parent=$(dirname "$original_dir")
            if [ -d "$old_parent" ] && [ "$(basename "$old_parent")" != "Music" ]; then
                rmdir "$old_parent" 2>/dev/null || true
            fi
        fi

        # (Re)create final dir and process from staging
        mkdir -p "$final_dir"
        chown autorip:autorip "$MUSIC_DIR/$safe_artist" 2>/dev/null || true
        chmod 777 "$MUSIC_DIR/$safe_artist" 2>/dev/null || true

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

        # Remove any previously copied files in final_dir before re-processing
        find "$final_dir" -maxdepth 1 -name "*.$ext" -delete 2>/dev/null || true

        local file_num=0
        while IFS= read -r audio_file; do
            [ -f "$audio_file" ] || continue
            file_num=$((file_num + 1))

            local tracknum
            tracknum=$(printf '%02d' "$file_num")
            local track_name=""
            if [ "$file_num" -le "$track_count" ]; then
                track_name="${track_names[$((file_num - 1))]}"
            fi

            # Re-tag the file
            if [ "$ext" = "mp3" ] && command -v eyeD3 >/dev/null 2>&1; then
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
                eyeD3 "${eyeD3_args[@]}" "$audio_file" 2>&1 || true
            elif [ "$ext" = "flac" ] && command -v metaflac >/dev/null 2>&1; then
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

        if [ -f "$staging_dir/cover.jpg" ]; then
            cp -f "$staging_dir/cover.jpg" "$final_dir/cover.jpg"
        fi

        # Make library files accessible to NFS clients (Picard, etc.)
        chown -R autorip:autorip "$final_dir" 2>/dev/null || true
        chmod 777 "$final_dir" 2>/dev/null || true
        chmod 666 "$final_dir"/* 2>/dev/null || true

        log "Re-processed $file_num track(s) to $final_dir"

        # Log to rip history
        local cover_rel=""
        if [ -f "$final_dir/cover.jpg" ]; then
            cover_rel="Audio/Music/$safe_artist/$safe_album/cover.jpg"
        fi
        log_rip_entry "Audio CD" "$artist" "$album" "$tracks_json" "$cover_rel"
    elif [ "$job_type" != "audio-cd" ]; then
        # Video jobs: if disc_title was edited, re-run rename from staging for all files
        local disc_title
        disc_title=$(grep -oP '"disc_title"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || true)

        if [ -n "$disc_title" ]; then
            # Multi-file disc job: iterate files array
            local -a v_file_paths=()
            local -a v_title_indices=()
            if grep -q '"files"' "$review_file" 2>/dev/null; then
                while IFS= read -r fp; do
                    [ -n "$fp" ] && v_file_paths+=("$fp")
                done < <(grep -oP '"file_path"\s*:\s*"\K[^"]+' "$review_file")
                while IFS= read -r tidx; do
                    [ -n "$tidx" ] && v_title_indices+=("$tidx")
                done < <(grep -oP '"title_index"\s*:\s*\K[0-9]+' "$review_file")
            else
                # Legacy single-file job
                local fp
                fp=$(grep -m1 -oP '"file_path"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || true)
                [ -n "$fp" ] && v_file_paths=("$fp")
                local tidx
                tidx=$(grep -m1 -oP '"title_index"\s*:\s*\K[0-9]+' "$review_file" 2>/dev/null || echo "0")
                v_title_indices=("$tidx")
            fi

            for i in "${!v_file_paths[@]}"; do
                local fp="${v_file_paths[$i]}"
                local tidx="${v_title_indices[$i]:-$((i+1))}"
                if [ -f "$fp" ]; then
                    if parse_tv_disc_title "$disc_title"; then
                        tv_rename_file "$fp" "$tidx"
                    else
                        movie_rename_file "$fp" "$disc_title"
                    fi
                fi
            done
        fi
    fi

    # Clean staging directory
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

    rm -f "$review_file"
    log "Approved: $(basename "$review_file")"
    echo "OK"
}

# ---------- reject: reject a single reviewed job (remove from library + staging) ----------
reject_single() {
    local job_id="$1"
    local review_file=""

    for f in "$QUEUE_DIR"/*.review; do
        [ -f "$f" ] || continue
        local base
        base=$(basename "$f")
        if [ "$base" = "$job_id" ] || [ "$base" = "${job_id}.review" ] || [ "${base%.review}" = "$job_id" ]; then
            review_file="$f"
            break
        fi
    done

    if [ -z "$review_file" ]; then
        echo "ERROR: No review job found matching: $job_id" >&2
        exit 1
    fi

    log "Rejecting job: $(basename "$review_file")"

    local job_type="" artist="" album="" staging_dir="" file_path="" format=""
    job_type=$(grep -oP '"job_type"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || echo "video")
    artist=$(grep -oP '"artist"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || true)
    album=$(grep -oP '"album"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || true)
    staging_dir=$(grep -oP '"staging_dir"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || true)
    format=$(grep -oP '"format"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || echo "mp3")

    if [ -z "$staging_dir" ]; then
        file_path=$(grep -oP '"file_path"\s*:\s*"\K[^"]+' "$review_file" 2>/dev/null || true)
        staging_dir=$(dirname "$file_path" 2>/dev/null || echo "")
    fi

    # Remove from final library
    if [ "$job_type" = "audio-cd" ] && [ -n "$artist" ] && [ -n "$album" ]; then
        local safe_artist safe_album
        safe_artist=$(echo "$artist" | sed -e 's/^\.*//' | tr -d ':><|*/"'"'"'?\\!')
        safe_album=$(echo "$album" | sed -e 's/^\.*//' | tr -d ':><|*/"'"'"'?\\!')
        local final_dir="$MUSIC_DIR/$safe_artist/$safe_album"
        if [ -d "$final_dir" ]; then
            log "Removing from library: $final_dir"
            rm -rf "$final_dir"
            # Clean up empty artist dir
            local artist_dir="$MUSIC_DIR/$safe_artist"
            if [ -d "$artist_dir" ]; then
                rmdir "$artist_dir" 2>/dev/null || true
            fi
        fi
    fi

    # Clean staging directory
    if [ -n "$staging_dir" ] && [ -d "$staging_dir" ]; then
        log "Cleaning staging: $staging_dir"
        rm -rf "$staging_dir"
        local parent_dir
        parent_dir=$(dirname "$staging_dir")
        if [ -d "$parent_dir" ] && [ "$(basename "$parent_dir")" != ".autorip-staging" ]; then
            rmdir "$parent_dir" 2>/dev/null || true
        fi
    fi

    rm -f "$review_file"
    log "Rejected and cleaned: $(basename "$review_file")"
    echo "OK"
}

# ---------- Handle subcommands ----------
case "${1:-}" in
    clean)
        mkdir -p "$QUEUE_DIR"
        log "Cleaning reviewed jobs..."
        clean_reviewed
        exit 0
        ;;
    approve)
        mkdir -p "$QUEUE_DIR"
        if [ -z "${2:-}" ]; then
            echo "Usage: $0 approve <job_file>" >&2
            exit 1
        fi
        approve_single "$2"
        exit 0
        ;;
    reject)
        mkdir -p "$QUEUE_DIR"
        if [ -z "${2:-}" ]; then
            echo "Usage: $0 reject <job_file>" >&2
            exit 1
        fi
        reject_single "$2"
        exit 0
        ;;
    list|review|status)
        mkdir -p "$QUEUE_DIR"
        echo "=== Jobs pending review ==="
        list_reviewed
        exit 0
        ;;
    ""|-*)
        # No subcommand or flags — fall through to normal job processing
        ;;
    *)
        echo "Usage: $0 [clean|list|approve <job>|reject <job>]" >&2
        echo "  (no args)  Process pending queue jobs" >&2
        echo "  list       Show jobs pending review" >&2
        echo "  clean      Approve all and purge staging" >&2
        echo "  approve    Approve a single job" >&2
        echo "  reject     Reject a single job (removes from library)" >&2
        exit 1
        ;;
esac

# ==========================================================================
# Main loop — process all pending jobs then exit
# ==========================================================================
mkdir -p "$QUEUE_DIR"

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
