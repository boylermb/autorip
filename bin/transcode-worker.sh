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
STATUS_FILE="$OUTPUT_BASE/.autorip-queue/.worker-status.json"
HOSTNAME=$(hostname)

# mnamer settings (movies only)
MNAMER_MOVIE_DIR="$OUTPUT_BASE/Video/Movies"
MNAMER_MOVIE_FORMAT="${MNAMER_MOVIE_FORMAT:-{name} ({year})/{name} ({year}){extension}}"

EPISODES_PER_DISC="${EPISODES_PER_DISC:-4}"

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
}

# ---------- Worker status (read by dashboard) ----------
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
    for audio_file in $(find "$staging_dir" -maxdepth 1 -name "*.$ext" 2>/dev/null | sort); do
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
    done

    # Copy cover art to final directory
    if [ -f "$staging_dir/cover.jpg" ]; then
        cp -f "$staging_dir/cover.jpg" "$final_dir/cover.jpg"
        log "Copied cover art to $final_dir"
    fi

    # Record in rip log
    local cover_rel=""
    if [ -f "$final_dir/cover.jpg" ]; then
        cover_rel="Audio/Music/$safe_artist/$safe_album/cover.jpg"
    fi
    log_rip_entry "Audio CD" "$artist" "$album" "$tracks_json" "$cover_rel"

    # Keep staging directory for review — files are copied, not moved.
    # Run `transcode-worker.sh clean` to purge reviewed staging dirs.
    log "Staging kept for review: $staging_dir"

    # Mark job as needing review (staging still present)
    mv "$processing_file" "${processing_file%.processing}.review"
    log "Audio CD job complete (pending review): $artist / $album ($file_num tracks)"
}

# ---------- Process a single job ----------
process_job() {
    local job_file="$1"
    local job_name
    job_name=$(basename "$job_file")

    log "Processing job: $job_name"
    update_worker_status "transcoding" "$job_file" "Reading job..."

    # Parse job JSON (minimal — use grep/sed, no jq dependency)
    local disc_title file_path disc_type source_host title_index title_count
    disc_title=$(grep -oP '"disc_title"\s*:\s*"\K[^"]+' "$job_file" || echo "")
    file_path=$(grep -oP '"file_path"\s*:\s*"\K[^"]+' "$job_file" || echo "")
    disc_type=$(grep -oP '"disc_type"\s*:\s*"\K[^"]+' "$job_file" || echo "DVD")
    source_host=$(grep -oP '"source_host"\s*:\s*"\K[^"]+' "$job_file" || echo "unknown")
    title_index=$(grep -oP '"title_index"\s*:\s*\K[0-9]+' "$job_file" || echo "0")
    title_count=$(grep -oP '"title_count"\s*:\s*\K[0-9]+' "$job_file" || echo "0")

    if [ -z "$disc_title" ] || [ -z "$file_path" ]; then
        log "ERROR: Invalid job file $job_name (missing disc_title or file_path)"
        mv "$job_file" "${job_file%.json}.error"
        return 1
    fi

    if [ ! -f "$file_path" ]; then
        log "ERROR: File $file_path does not exist"
        mv "$job_file" "${job_file%.json}.error"
        return 1
    fi

    # Mark job as in-progress
    mv "$job_file" "${job_file%.json}.processing"
    local processing_file="${job_file%.json}.processing"

    local basename_mkv
    basename_mkv=$(basename "$file_path")
    log "Transcoding $basename_mkv [$title_index/$title_count] for $disc_title (from $source_host)"

    # Detect GPU
    local ffmpeg_video_opts="-c:v libx265 -crf 24 -preset medium"
    if command -v nvidia-smi >/dev/null 2>&1 && nvidia-smi >/dev/null 2>&1; then
        log "Using NVIDIA NVENC hardware encoding"
        ffmpeg_video_opts="-c:v hevc_nvenc -preset medium -rc constqp -qp 24"
    fi

    update_worker_status "transcoding" "$processing_file" "[$title_index/$title_count] $basename_mkv"

    # Check if needs transcoding
    local video_codec
    video_codec=$(ffprobe -loglevel error -select_streams v:0 \
        -show_entries stream=codec_name -of csv=p=0 "$file_path" 2>/dev/null | tr -d ',' | tr -d ' ' || true)

    if [ "$video_codec" = "mpeg2video" ]; then
        log "Transcoding $basename_mkv (MPEG-2 → H.265)..."
        local transcode_tmp="${file_path%.mkv}.transcoding.mkv"
        if ffmpeg -i "$file_path" \
            -map 0 \
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
            mv "$processing_file" "${processing_file%.processing}.error"
            return 1
        fi
    else
        log "$basename_mkv already $video_codec, skipping transcode"
    fi

    # Rename/move file to final location
    update_worker_status "renaming" "$processing_file" "[$title_index/$title_count] Organizing..."
    if parse_tv_disc_title "$disc_title"; then
        log "TV disc: $TV_SHOW Season $TV_SEASON Disc $TV_DISC — episode from title $title_index"
        tv_rename_file "$file_path" "$title_index"
    else
        log "Movie disc: $disc_title"
        movie_rename_file "$file_path" "$disc_title"
    fi

    # Keep staging directory for review — files are copied, not moved.
    # Run `transcode-worker.sh clean` to purge reviewed staging dirs.
    log "Staging kept for review: $(dirname "$file_path")"

    # Mark job as needing review (staging still present)
    mv "$processing_file" "${processing_file%.processing}.review"
    log "Job complete (pending review): $disc_title [$title_index/$title_count]"
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
            echo "  [$count] Video: $disc_title"
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

# ---------- Handle subcommands ----------
case "${1:-}" in
    clean)
        mkdir -p "$QUEUE_DIR"
        log "Cleaning reviewed jobs..."
        clean_reviewed
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
        echo "Usage: $0 [clean|list]" >&2
        echo "  (no args)  Process pending queue jobs" >&2
        echo "  list       Show jobs pending review" >&2
        echo "  clean      Purge reviewed staging dirs" >&2
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
