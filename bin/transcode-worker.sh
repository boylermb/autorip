#!/usr/bin/env bash
# =============================================================================
# transcode-worker.sh - Centralized GPU transcode queue processor
# https://github.com/boylermb/autorip
# =============================================================================
# Watches $OUTPUT_BASE/.autorip-queue/ for per-file transcode job files
# (JSON) submitted by autorip nodes as each title is ripped.
# Transcodes MPEG-2 → H.265 using NVIDIA NVENC (hevc_nvenc) on the GPU node,
# then renames/moves the file into Jellyfin-compatible paths.
#
# Each job represents a single MKV file (one title from a disc).  Jobs arrive
# as titles are ripped, so transcoding can begin while the disc is still
# being read.
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
STATUS_FILE="$OUTPUT_BASE/.autorip-queue/.worker-status.json"
HOSTNAME=$(hostname)

# mnamer settings (movies only)
MNAMER_MOVIE_DIR="$OUTPUT_BASE/Video/Movies"
MNAMER_MOVIE_FORMAT="${MNAMER_MOVIE_FORMAT:-{name} ({year})/{name} ({year}){extension}}"

EPISODES_PER_DISC="${EPISODES_PER_DISC:-4}"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $LOGPREFIX $*"; }

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
    mv -f "$mkv" "$dest_dir/$ep_name"
    log "Renamed $(basename "$mkv") → $ep_name"
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

    log "Running mnamer for movie file: $(basename "$mkv")"
    if mnamer --batch \
        --media=movie \
        --movie-api=tmdb \
        --movie-directory="$MNAMER_MOVIE_DIR" \
        --movie-format="$MNAMER_MOVIE_FORMAT" \
        "$mkv" 2>&1; then
        if [ -f "$mkv" ]; then
            log "mnamer did not match $(basename "$mkv"), using fallback"
            movie_fallback_file "$mkv" "$disc_title"
        else
            log "mnamer matched and moved $(basename "$mkv")"
        fi
    else
        log "mnamer failed, using fallback"
        movie_fallback_file "$mkv" "$disc_title"
    fi
}

movie_fallback_file() {
    local mkv="$1"
    local disc_title="$2"
    local fallback_dir="$MOVIES_DIR/$disc_title"
    mkdir -p "$fallback_dir"
    mv -f "$mkv" "$fallback_dir/"
    log "Moved $(basename "$mkv") to $fallback_dir"

    # Enqueue OCR-based identification as a Kubernetes Job.
    # The episode-identify pipeline will attempt to read on-screen text
    # (title cards, credits) and match against TMDb.  If successful it
    # moves the file into the correct Jellyfin folder structure.
    local moved_file="$fallback_dir/$(basename "$mkv")"
    if [ -x /usr/local/bin/enqueue-identify.sh ] && [ -f "$moved_file" ]; then
        log "Enqueueing OCR identification job for $(basename "$mkv")"
        /usr/local/bin/enqueue-identify.sh "$moved_file" || \
            log "WARNING: Failed to enqueue OCR identification job"
    else
        log "enqueue-identify.sh not available, skipping OCR identification"
    fi
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

    # Try to clean up staging directory if empty (last title processed)
    local staging_dir
    staging_dir=$(dirname "$file_path")
    if [ -d "$staging_dir" ] && [ "$(basename "$staging_dir")" != ".autorip-staging" ]; then
        local remaining
        remaining=$(find "$staging_dir" -maxdepth 1 -name "*.mkv" 2>/dev/null | wc -l)
        if [ "$remaining" -eq 0 ]; then
            rmdir "$staging_dir" 2>/dev/null && log "Cleaned up staging dir: $staging_dir" || true
        fi
    fi

    # Mark job complete
    mv "$processing_file" "${processing_file%.processing}.done"
    log "Job complete: $disc_title [$title_index/$title_count]"
}

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
    process_job "$job_file" || true
done

if [ "$job_count" -eq 0 ]; then
    log "No jobs in queue."
else
    log "Processed $job_count job(s)."
fi

update_worker_status "idle" "" ""
log "Worker done."
