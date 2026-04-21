#!/usr/bin/env bash
# backfill-tv-artwork.sh — fetch missing poster.jpg / tvshow.nfo /
# season-poster.jpg for shows that already live in the TV library.
#
# Usage:
#   backfill-tv-artwork.sh [--library DIR] [--show NAME] [--apply]
#                          [--overwrite] [--quiet]
#
# Defaults to dry-run mode: prints what *would* be downloaded.  Pass
# --apply to actually fetch.  Pass --overwrite to refetch even when a
# file already exists.  Reuses bin/lib/tmdb.sh + bin/lib/tv-overrides.sh
# so the same TMDb pinning / name overrides apply.
#
# Idempotent on repeat runs: existing non-empty artwork files are kept
# unless --overwrite is given.

set -euo pipefail

# ---------- locate libs ----------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB_CANDIDATES=(
    "$SCRIPT_DIR/lib"
    "/usr/local/lib/autorip"
)
LIB_DIR=""
for d in "${LIB_CANDIDATES[@]}"; do
    if [ -f "$d/tmdb.sh" ]; then LIB_DIR="$d"; break; fi
done
if [ -z "$LIB_DIR" ]; then
    echo "FATAL: could not find tmdb.sh in any of: ${LIB_CANDIDATES[*]}" >&2
    exit 2
fi

# shellcheck source=lib/tmdb.sh
. "$LIB_DIR/tmdb.sh"
# shellcheck source=lib/tv-overrides.sh
[ -f "$LIB_DIR/tv-overrides.sh" ] && . "$LIB_DIR/tv-overrides.sh"

# ---------- defaults ----------
OUTPUT_BASE="${OUTPUT_BASE:-/srv/library}"
LIBRARY_DIR="${LIBRARY_DIR:-$OUTPUT_BASE/Video/TV}"
ONLY_SHOW=""
APPLY=0
OVERWRITE=0
QUIET=0

# Counters
SHOWS_SCANNED=0
SHOWS_SKIPPED=0
POSTERS_FETCHED=0
POSTERS_SKIPPED=0
NFOS_WRITTEN=0
NFOS_SKIPPED=0
SEASON_POSTERS_FETCHED=0
SEASON_POSTERS_SKIPPED=0
ERRORS=0

# ---------- helpers ----------
log() {
    [ "$QUIET" -eq 1 ] && return 0
    printf '%s\n' "$*"
}
warn() { printf 'WARN: %s\n' "$*" >&2; }
err()  { printf 'ERROR: %s\n' "$*" >&2; ERRORS=$((ERRORS + 1)); }

usage() {
    sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'
    exit "${1:-0}"
}

# ---------- argument parsing ----------
while [ $# -gt 0 ]; do
    case "$1" in
        --library)   LIBRARY_DIR="$2"; shift 2 ;;
        --show)      ONLY_SHOW="$2";   shift 2 ;;
        --apply)     APPLY=1;          shift ;;
        --overwrite) OVERWRITE=1;      shift ;;
        --quiet|-q)  QUIET=1;          shift ;;
        -h|--help)   usage 0 ;;
        *)           echo "Unknown arg: $1" >&2; usage 2 ;;
    esac
done

if [ ! -d "$LIBRARY_DIR" ]; then
    err "library directory not found: $LIBRARY_DIR"
    exit 1
fi

[ "$APPLY" -eq 1 ] && MODE="APPLY" || MODE="DRY-RUN"
log "── TV artwork backfill ($MODE) ──"
log "Library: $LIBRARY_DIR"
[ -n "$ONLY_SHOW" ] && log "Filter:  show='$ONLY_SHOW'"
log ""

# ---------- per-show ----------
# Resolve TMDb id + canonical name for a directory-named show.
# Honours tv-overrides.json when available.  Sets:
#   RESOLVED_TMDB_ID, RESOLVED_NAME (or empty on failure).
resolve_show() {
    local dir_name="$1"
    RESOLVED_TMDB_ID=""
    RESOLVED_NAME=""

    # Try overrides first (pass dir_name as both raw_show + disc_title)
    if declare -F tv_apply_overrides >/dev/null 2>&1; then
        if tv_apply_overrides "$dir_name" "$dir_name" 2>/dev/null; then
            if [ -n "${TV_OVERRIDE_TMDB_ID:-}" ]; then
                if tmdb_get_show_by_id "$TV_OVERRIDE_TMDB_ID" 2>/dev/null; then
                    RESOLVED_TMDB_ID="$TMDB_SHOW_ID"
                    RESOLVED_NAME="${TV_OVERRIDE_SHOW:-$TMDB_SHOW_NAME}"
                    return 0
                fi
            elif [ -n "${TV_OVERRIDE_SHOW:-}" ]; then
                # Override gave us a name but no id; fall through to search
                dir_name="$TV_OVERRIDE_SHOW"
            fi
        fi
    fi

    if tmdb_search_show "$dir_name" 2>/dev/null; then
        RESOLVED_TMDB_ID="$TMDB_SHOW_ID"
        RESOLVED_NAME="$TMDB_SHOW_NAME"
        return 0
    fi
    return 1
}

# Backfill one show directory.
backfill_show() {
    local show_dir="$1"
    local show_name
    show_name="$(basename "$show_dir")"

    case "$show_name" in
        _unmatched|_pending|.*) return 0 ;;
    esac
    if [ -n "$ONLY_SHOW" ] && [ "$show_name" != "$ONLY_SHOW" ]; then
        return 0
    fi

    SHOWS_SCANNED=$((SHOWS_SCANNED + 1))

    local poster="$show_dir/poster.jpg"
    local nfo="$show_dir/tvshow.nfo"

    # Check what's actually missing first (avoid TMDb call if nothing to do)
    local need_poster=0 need_nfo=0
    [ ! -s "$poster" ] || [ "$OVERWRITE" -eq 1 ] && need_poster=1
    [ ! -s "$nfo" ]    || [ "$OVERWRITE" -eq 1 ] && need_nfo=1

    # Inspect seasons
    local season_dir season_poster
    local missing_season_posters=()
    while IFS= read -r -d '' season_dir; do
        season_poster="$season_dir/season-poster.jpg"
        if [ ! -s "$season_poster" ] || [ "$OVERWRITE" -eq 1 ]; then
            missing_season_posters+=("$season_dir")
        fi
    done < <(find "$show_dir" -mindepth 1 -maxdepth 1 -type d -name 'Season *' -print0 2>/dev/null | sort -z)

    if [ "$need_poster" -eq 0 ] && [ "$need_nfo" -eq 0 ] && [ "${#missing_season_posters[@]}" -eq 0 ]; then
        SHOWS_SKIPPED=$((SHOWS_SKIPPED + 1))
        log "✓ $show_name — already complete"
        return 0
    fi

    log "▶ $show_name"

    if ! resolve_show "$show_name"; then
        err "  could not resolve '$show_name' on TMDb — skipping"
        return 0
    fi
    log "  TMDb id=$RESOLVED_TMDB_ID name='$RESOLVED_NAME'"

    # ---- show-level poster ----
    if [ "$need_poster" -eq 1 ]; then
        if tmdb_fetch_show_images "$RESOLVED_TMDB_ID" 2>/dev/null && [ -n "$TMDB_POSTER_URL" ]; then
            if [ "$APPLY" -eq 1 ]; then
                if tmdb_download_image "$TMDB_POSTER_URL" "$poster"; then
                    log "  ↓ poster.jpg ($TMDB_POSTER_URL)"
                    POSTERS_FETCHED=$((POSTERS_FETCHED + 1))
                else
                    err "  failed to download poster"
                fi
            else
                log "  [dry-run] would download poster: $TMDB_POSTER_URL"
                POSTERS_FETCHED=$((POSTERS_FETCHED + 1))
            fi
        else
            warn "  no show-level poster on TMDb for $show_name"
            POSTERS_SKIPPED=$((POSTERS_SKIPPED + 1))
        fi
    else
        POSTERS_SKIPPED=$((POSTERS_SKIPPED + 1))
    fi

    # ---- tvshow.nfo ----
    if [ "$need_nfo" -eq 1 ]; then
        if [ "$APPLY" -eq 1 ]; then
            local title_xml
            title_xml=$(printf '%s' "${RESOLVED_NAME:-$show_name}" \
                | sed 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g')
            cat > "$nfo" <<EOF
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<tvshow>
  <title>$title_xml</title>
  <uniqueid type="tmdb" default="true">$RESOLVED_TMDB_ID</uniqueid>
</tvshow>
EOF
            log "  ✎ tvshow.nfo (tmdb=$RESOLVED_TMDB_ID)"
            NFOS_WRITTEN=$((NFOS_WRITTEN + 1))
        else
            log "  [dry-run] would write tvshow.nfo (tmdb=$RESOLVED_TMDB_ID)"
            NFOS_WRITTEN=$((NFOS_WRITTEN + 1))
        fi
    else
        NFOS_SKIPPED=$((NFOS_SKIPPED + 1))
    fi

    # ---- per-season posters ----
    local sd season_num sp
    for sd in "${missing_season_posters[@]}"; do
        # "Season 03" → 3
        season_num=$(basename "$sd" | sed -E 's/^Season 0*([0-9]+).*/\1/')
        if [ -z "$season_num" ] || ! [[ "$season_num" =~ ^[0-9]+$ ]]; then
            warn "  could not parse season number from '$(basename "$sd")'"
            continue
        fi
        sp="$sd/season-poster.jpg"

        if tmdb_fetch_season_images "$RESOLVED_TMDB_ID" "$season_num" 2>/dev/null \
           && [ -n "$TMDB_SEASON_POSTER_URL" ]; then
            if [ "$APPLY" -eq 1 ]; then
                if tmdb_download_image "$TMDB_SEASON_POSTER_URL" "$sp"; then
                    log "  ↓ Season $season_num/season-poster.jpg"
                    SEASON_POSTERS_FETCHED=$((SEASON_POSTERS_FETCHED + 1))
                else
                    err "  failed to download season $season_num poster"
                fi
            else
                log "  [dry-run] would download Season $season_num/season-poster.jpg ($TMDB_SEASON_POSTER_URL)"
                SEASON_POSTERS_FETCHED=$((SEASON_POSTERS_FETCHED + 1))
            fi
        else
            warn "  no season $season_num poster on TMDb"
            SEASON_POSTERS_SKIPPED=$((SEASON_POSTERS_SKIPPED + 1))
        fi
    done
}

# ---------- walk library ----------
shopt -s nullglob
for show_dir in "$LIBRARY_DIR"/*/; do
    show_dir="${show_dir%/}"
    [ -d "$show_dir" ] || continue
    backfill_show "$show_dir"
done

# ---------- summary ----------
log ""
log "── Summary ($MODE) ──"
log "Shows scanned:    $SHOWS_SCANNED"
log "  already done:   $SHOWS_SKIPPED"
log "Posters:          $POSTERS_FETCHED fetched, $POSTERS_SKIPPED skipped"
log "tvshow.nfo:       $NFOS_WRITTEN written,  $NFOS_SKIPPED skipped"
log "Season posters:   $SEASON_POSTERS_FETCHED fetched, $SEASON_POSTERS_SKIPPED skipped"
[ "$ERRORS" -gt 0 ] && log "Errors:           $ERRORS"
[ "$APPLY" -eq 0 ] && log "(dry-run — re-run with --apply to actually download)"

exit 0
