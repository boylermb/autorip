#!/usr/bin/env bash
# Smoke test for the layered "play all" detection in autorip.sh.
#
# Rather than sourcing autorip.sh (which pulls in systemd / makemkvcon state),
# we re-implement the detection block in-line here against synthetic
# MAKEMKV_INFO strings.  Any logic change in autorip.sh must be mirrored in
# detect_playall() below; the intent is to document expected behaviour and
# catch accidental regressions.
set -euo pipefail

log() { :; }  # silence detection logs during tests

detect_playall() {
    local info="$1"; shift
    local -a title_ids=( "$@" )

    declare -A title_durations=()
    declare -A title_segments=()
    local _total_dur=0
    for tid in "${title_ids[@]}"; do
        local dur_str
        dur_str=$(echo "$info" | grep "^TINFO:${tid},9," | head -1 | sed 's/.*,"//' | tr -d '"' || true)
        if [ -n "$dur_str" ]; then
            local h m s
            IFS=: read -r h m s <<< "$dur_str"
            local _d=$(( 10#$h * 3600 + 10#$m * 60 + 10#$s ))
            title_durations[$tid]=$_d
            _total_dur=$(( _total_dur + _d ))
        fi
        local seg_str
        seg_str=$(echo "$info" | grep "^TINFO:${tid},25," | head -1 | sed 's/.*,"//' | tr -d '"' || true)
        title_segments[$tid]="${seg_str:-0}"
    done

    declare -A _is_playall=()

    if [ "${#title_durations[@]}" -ge 3 ]; then
        for tid in "${title_ids[@]}"; do
            local tdur="${title_durations[$tid]:-0}"
            [ "$tdur" -gt 0 ] || continue
            local others=$(( _total_dur - tdur ))
            [ "$others" -gt 0 ] || continue
            local diff
            if [ "$tdur" -ge "$others" ]; then diff=$(( tdur - others )); else diff=$(( others - tdur )); fi
            if [ $(( diff * 100 )) -le $(( others * 15 )) ]; then
                _is_playall[$tid]=1
            fi
        done
    fi

    if [ "${#title_durations[@]}" -ge 3 ]; then
        local -a sorted_durs=()
        while IFS= read -r d; do sorted_durs+=("$d"); done \
            < <(for tid in "${title_ids[@]}"; do echo "${title_durations[$tid]:-0}"; done | sort -n)
        local median_dur="${sorted_durs[$(( ${#sorted_durs[@]} / 2 ))]}"
        local -a sorted_segs=()
        while IFS= read -r s; do sorted_segs+=("$s"); done \
            < <(for tid in "${title_ids[@]}"; do echo "${title_segments[$tid]:-0}"; done | sort -n)
        local median_seg="${sorted_segs[$(( ${#sorted_segs[@]} / 2 ))]}"

        if [ "${median_dur:-0}" -gt 0 ]; then
            for tid in "${title_ids[@]}"; do
                [ -n "${_is_playall[$tid]:-}" ] && continue
                local tdur="${title_durations[$tid]:-0}"
                local tseg="${title_segments[$tid]:-0}"
                if [ "$tdur" -ge $(( median_dur * 25 / 10 )) ]; then
                    _is_playall[$tid]=1
                    continue
                fi
                if [ "${median_seg:-0}" -gt 0 ] \
                   && [ "$tseg" -ge $(( median_seg * 2 )) ] \
                   && [ "$tdur" -ge $(( median_dur * 18 / 10 )) ]; then
                    _is_playall[$tid]=1
                fi
            done
        fi
    fi

    # Print flagged IDs, sorted
    for tid in "${title_ids[@]}"; do
        [ -n "${_is_playall[$tid]:-}" ] && echo "$tid"
    done | sort -n | paste -sd, -
}

pass=0; fail=0
check() {
    local name="$1" expected="$2" actual="$3"
    if [ "$expected" = "$actual" ]; then
        echo "  ✓ $name"
        pass=$(( pass + 1 ))
    else
        echo "  ✗ $name: expected '$expected' got '$actual'"
        fail=$(( fail + 1 ))
    fi
}

# ----------------------------------------------------------------------
# Case 1: classic box set — 1 play-all (4× episode) + 4 episodes of 22min.
# Signal A (sum-of-others) should fire: playall dur = 88min = sum of 4× 22min.
# ----------------------------------------------------------------------
info1='TINFO:0,9,0,"01:28:00"
TINFO:0,25,0,"20"
TINFO:1,9,0,"00:22:00"
TINFO:1,25,0,"5"
TINFO:2,9,0,"00:22:00"
TINFO:2,25,0,"5"
TINFO:3,9,0,"00:22:00"
TINFO:3,25,0,"5"
TINFO:4,9,0,"00:22:00"
TINFO:4,25,0,"5"'
check "box set: flags the 4× play-all" "0" "$(detect_playall "$info1" 0 1 2 3 4)"

# ----------------------------------------------------------------------
# Case 2: 3 episodes of equal length, no play-all.
# No signals should fire.
# ----------------------------------------------------------------------
info2='TINFO:1,9,0,"00:22:00"
TINFO:1,25,0,"5"
TINFO:2,9,0,"00:22:00"
TINFO:2,25,0,"5"
TINFO:3,9,0,"00:22:00"
TINFO:3,25,0,"5"'
check "3 equal episodes: no flags" "" "$(detect_playall "$info2" 1 2 3)"

# ----------------------------------------------------------------------
# Case 3: segment-outlier — play-all slightly under 2.5× but with 3× segments.
# Signal B should fire.  4 eps of 22min (segs=5) + compilation of 40min (segs=15)
# (40min is 1.8× median 22min, and 15 is 3× median 5).
# NOTE sum-of-others won't fire (40 vs 88 others → 55% diff).
# ----------------------------------------------------------------------
info3='TINFO:0,9,0,"00:40:00"
TINFO:0,25,0,"15"
TINFO:1,9,0,"00:22:00"
TINFO:1,25,0,"5"
TINFO:2,9,0,"00:22:00"
TINFO:2,25,0,"5"
TINFO:3,9,0,"00:22:00"
TINFO:3,25,0,"5"
TINFO:4,9,0,"00:22:00"
TINFO:4,25,0,"5"'
check "segment outlier flagged" "0" "$(detect_playall "$info3" 0 1 2 3 4)"

# ----------------------------------------------------------------------
# Case 4: legacy duration outlier — 1 big title ≥2.5× median, no segment data.
# Signal C (legacy) should fire.
# ----------------------------------------------------------------------
info4='TINFO:0,9,0,"01:00:00"
TINFO:1,9,0,"00:22:00"
TINFO:2,9,0,"00:22:00"
TINFO:3,9,0,"00:22:00"'
check "legacy 2.5× median flagged" "0" "$(detect_playall "$info4" 0 1 2 3)"

# ----------------------------------------------------------------------
# Case 5: 2 titles — movie + bonus feature.  Neither signal should fire
# (sum-of-others requires n>=3; n=2 skips all signals).
# ----------------------------------------------------------------------
info5='TINFO:0,9,0,"01:45:00"
TINFO:0,25,0,"12"
TINFO:1,9,0,"00:15:00"
TINFO:1,25,0,"3"'
check "2 titles (movie+bonus): no flags" "" "$(detect_playall "$info5" 0 1)"

# ----------------------------------------------------------------------
# Case 6: all equal-length titles, none flagged even though each duration
# equals sum of others × 1/(n-1).  With n=3 and equal durs, others = 2·tdur;
# diff% = 50% > 15% → no flag.  Guards against false-positives.
# ----------------------------------------------------------------------
info6='TINFO:1,9,0,"00:45:00"
TINFO:2,9,0,"00:45:00"
TINFO:3,9,0,"00:45:00"'
check "3 equal-length movies: no flags" "" "$(detect_playall "$info6" 1 2 3)"

echo
echo "Results: $pass passed, $fail failed"
exit $fail
