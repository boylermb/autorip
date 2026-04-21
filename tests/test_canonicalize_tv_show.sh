#!/usr/bin/env bash
# Live TMDb test for canonicalize_tv_show.
#
# Hits the real TMDb API.  Requires network.  Uses the tmdb.sh library's
# default key or $TMDB_API_KEY override.
#
# Rather than sourcing transcode-worker.sh (pulls in systemd state),
# we re-declare the function here.  Logic must stay in sync with the
# canonicalize_tv_show() in bin/transcode-worker.sh.
set -euo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SELF_DIR/../bin/lib/tmdb.sh"

log() { echo "  [log] $*" >&2; }

canonicalize_tv_show() {
    local raw="$1"
    TV_SHOW_CANONICAL=""
    TV_SHOW_TMDB_ID=""
    declare -F tmdb_search_show >/dev/null 2>&1 || return 1

    local -a variants=( "$raw" )
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
        [ -z "$v" ] && continue
        local wc; wc=$(echo "$v" | wc -w | tr -d ' ')
        { [ "$wc" -eq 1 ] && [ "${#v}" -lt 4 ]; } && continue
        echo "$v" | grep -qE '^[0-9]+$' && continue

        if tmdb_search_show "$v" 2>/dev/null && [ -n "${TMDB_SHOW_ID:-}" ]; then
            TV_SHOW_CANONICAL="${TMDB_SHOW_NAME:-$v}"
            TV_SHOW_TMDB_ID="$TMDB_SHOW_ID"
            return 0
        fi
    done
    return 1
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

# Clean TMDb per-process cache between tests
reset_tmdb() {
    unset _TMDB_SEARCH_CACHE _TMDB_SEASON_CACHE_KEY _TMDB_SHOW_IMAGES_KEY _TMDB_SEASON_IMAGES_KEY
    declare -gA _TMDB_SEARCH_CACHE=()
    TMDB_SHOW_ID=""
    TMDB_SHOW_NAME=""
}

# ---------- Test 1: well-known show, direct hit
reset_tmdb
canonicalize_tv_show "Futurama" && r=$TV_SHOW_CANONICAL || r="FAIL"
check "direct hit: Futurama" "Futurama" "$r"

# ---------- Test 2: trailing volume stripped
reset_tmdb
canonicalize_tv_show "Futurama Vol 2" && r=$TV_SHOW_CANONICAL || r="FAIL"
# "Futurama Vol 2" itself probably misses; stripped "Futurama" should hit.
check "strip 'Vol 2': Futurama Vol 2 → Futurama" "Futurama" "$r"

# ---------- Test 3: garbage short label → no match
reset_tmdb
if canonicalize_tv_show "Sp1"; then r="MATCHED_UNEXPECTEDLY($TV_SHOW_CANONICAL)"; else r="no match"; fi
check "junk label 'Sp1': rejected" "no match" "$r"

# ---------- Test 4: empty input → no match
reset_tmdb
if canonicalize_tv_show ""; then r="MATCHED_UNEXPECTEDLY"; else r="no match"; fi
check "empty input: rejected" "no match" "$r"

# ---------- Test 5: all-digits → no match
reset_tmdb
if canonicalize_tv_show "1999"; then r="MATCHED_UNEXPECTEDLY($TV_SHOW_CANONICAL)"; else r="no match"; fi
# Note: "1999" is technically valid (there are shows with numeric titles);
# we explicitly skip all-digit variants.
check "all-digits '1999': rejected" "no match" "$r"

echo
echo "Results: $pass passed, $fail failed"
exit $fail
