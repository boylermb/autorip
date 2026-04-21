#!/usr/bin/env bash
# tv-runtime-check.sh — detect production-vs-aired order mismatches.
#
# When TMDb returns episode runtimes that disagree with the actual MKV
# file durations, the disc is probably in a different order than aired
# (production order, foreign cut, etc.) — or has bonus content TMDb
# doesn't know about.  We can't fix the order automatically, but we can
# flag the disc and let a human triage it.
#
# Inputs come from already-loaded state:
#   - TMDB_EP_RUNTIMES[N] (set by tmdb_fetch_season; minutes; may be empty)
#   - TV_FIRST_EPISODE    (set by tv_progress_for_disc)
#
# Public function:
#   tv_check_runtime_match <first_episode> <file1> [file2] ...
#
# Sets:
#   TV_RUNTIME_MISMATCH  = 1 if the disc looks out of order, else empty
#   TV_RUNTIME_PLAN      = multi-line text suitable for episodes-plan.txt:
#                          one row per file with planned ep#, TMDb name,
#                          planned runtime, actual runtime, and verdict.
#
# Returns 0 always (the verdict lives in TV_RUNTIME_MISMATCH).
# If preconditions aren't met (no TMDb runtimes, no ffprobe, etc.) the
# check silently no-ops and TV_RUNTIME_MISMATCH stays empty.

if [ -n "${_TV_RUNTIME_CHECK_LIB_SOURCED:-}" ]; then
    return 0
fi
_TV_RUNTIME_CHECK_LIB_SOURCED=1

# Tolerance: an episode is "mismatched" if |actual - planned| exceeds
# both an absolute and a relative threshold.  Defaults intentionally
# generous to avoid flagging routine ±2-3 min variation that's normal
# for animated and 22-min sitcoms.
TV_RUNTIME_TOL_ABS_MIN="${TV_RUNTIME_TOL_ABS_MIN:-4}"   # minutes
TV_RUNTIME_TOL_REL_PCT="${TV_RUNTIME_TOL_REL_PCT:-25}"  # percent

# Trip the mismatch flag at this many bad episodes (or, if fewer files
# than this, at all-but-one).
TV_RUNTIME_MISMATCH_THRESHOLD="${TV_RUNTIME_MISMATCH_THRESHOLD:-3}"

_tv_runtime_log() {
    if declare -F log >/dev/null 2>&1; then
        log "runtime-check: $*"
    else
        echo "runtime-check: $*" >&2
    fi
}

# Probe a single file's duration in whole minutes (rounded).
# Echoes the integer minutes on stdout, or empty on failure.
_tv_runtime_probe_minutes() {
    local file="$1"
    [ -f "$file" ] || return 1
    command -v ffprobe >/dev/null 2>&1 || return 1
    local secs
    secs=$(ffprobe -v error -show_entries format=duration \
              -of default=noprint_wrappers=1:nokey=1 "$file" 2>/dev/null) || return 1
    [ -n "$secs" ] || return 1
    # secs may be a float like "1320.456000"; round to nearest minute via awk
    awk -v s="$secs" 'BEGIN { printf("%d\n", (s/60.0) + 0.5) }'
}

tv_check_runtime_match() {
    TV_RUNTIME_MISMATCH=""
    TV_RUNTIME_PLAN=""

    local first_ep="$1"; shift
    [ -n "$first_ep" ] || return 0
    [ "$#" -gt 0 ] || return 0

    # Need ffprobe
    if ! command -v ffprobe >/dev/null 2>&1; then
        _tv_runtime_log "ffprobe not available; skipping mismatch check"
        return 0
    fi

    # Need at least some TMDb runtime data
    local have_tmdb=0
    if declare -p TMDB_EP_RUNTIMES >/dev/null 2>&1; then
        local _k
        for _k in "${!TMDB_EP_RUNTIMES[@]}"; do
            if [ -n "${TMDB_EP_RUNTIMES[$_k]}" ]; then
                have_tmdb=1
                break
            fi
        done
    fi
    if [ "$have_tmdb" -eq 0 ]; then
        _tv_runtime_log "no TMDb runtimes loaded; skipping mismatch check"
        return 0
    fi

    local total=$#
    local mismatches=0
    local checked=0
    local plan="ep#  planned-name                                 plan(min) actual(min)  verdict"$'\n'
    plan+="---  -------------------------------------------- --------- -----------  -------"$'\n'

    local i=0
    local f
    for f in "$@"; do
        local ep_num=$(( first_ep + i ))
        i=$(( i + 1 ))

        local actual planned name verdict
        actual=$(_tv_runtime_probe_minutes "$f" || true)
        planned="${TMDB_EP_RUNTIMES[$ep_num]:-}"
        name="${TMDB_EP_NAMES[$ep_num]:-<unknown>}"

        if [ -z "$actual" ]; then
            verdict="probe-failed"
        elif [ -z "$planned" ]; then
            verdict="no-tmdb-runtime"
        else
            checked=$(( checked + 1 ))
            local diff
            if [ "$actual" -ge "$planned" ]; then diff=$(( actual - planned )); else diff=$(( planned - actual )); fi
            local rel_threshold=$(( planned * TV_RUNTIME_TOL_REL_PCT / 100 ))
            local thresh=$rel_threshold
            [ "$TV_RUNTIME_TOL_ABS_MIN" -gt "$thresh" ] && thresh=$TV_RUNTIME_TOL_ABS_MIN
            if [ "$diff" -gt "$thresh" ]; then
                verdict="MISMATCH (Δ${diff}m > ${thresh}m)"
                mismatches=$(( mismatches + 1 ))
            else
                verdict="ok (Δ${diff}m)"
            fi
        fi

        # Truncate name for display
        local display_name="${name:0:44}"
        plan+=$(printf "E%02d  %-44s %9s %11s  %s\n" \
                       "$ep_num" "$display_name" "${planned:--}" "${actual:--}" "$verdict")
        plan+=$'\n'
    done

    TV_RUNTIME_PLAN="$plan"

    # Decide if we should flag.  The threshold is min(MISMATCH_THRESHOLD,
    # checked-1) — i.e. for short discs (1-2 episodes), even a single
    # mismatch is enough; for longer discs, require 3.
    local trigger=$TV_RUNTIME_MISMATCH_THRESHOLD
    if [ "$checked" -gt 0 ] && [ $(( checked - 1 )) -lt "$trigger" ] && [ $(( checked - 1 )) -gt 0 ]; then
        trigger=$(( checked - 1 ))
    fi

    if [ "$checked" -ge 1 ] && [ "$mismatches" -ge "$trigger" ]; then
        TV_RUNTIME_MISMATCH=1
        _tv_runtime_log "flagged: $mismatches/$checked episodes mismatched (trigger=$trigger)"
    else
        _tv_runtime_log "ok: $mismatches/$checked mismatches (trigger=$trigger)"
    fi
    return 0
}
