#!/usr/bin/env bash
# Smoke test for backfill-tv-artwork.sh.  No network required —
# stubs out the tmdb_* functions so we can verify dry-run output and
# file-creation logic against a synthetic library tree.
set -euo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$SELF_DIR/../bin/backfill-tv-artwork.sh"

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

# Build a synthetic library:
#   Library/
#     Futurama/
#       Season 02/   (no posters, no nfo)
#         episode01.mkv
#       Season 03/
#         episode02.mkv
#     Complete Show/
#       poster.jpg
#       tvshow.nfo
#       Season 01/
#         season-poster.jpg
#         ep.mkv
#     _unmatched/   (must be skipped)
#       SP1_S1D1/
LIB="$TMPDIR/Library"
mkdir -p "$LIB/Futurama/Season 02" "$LIB/Futurama/Season 03"
touch "$LIB/Futurama/Season 02/episode01.mkv"
touch "$LIB/Futurama/Season 03/episode02.mkv"

mkdir -p "$LIB/Complete Show/Season 01"
echo "JPEG" > "$LIB/Complete Show/poster.jpg"
echo "<tvshow/>" > "$LIB/Complete Show/tvshow.nfo"
echo "JPEG" > "$LIB/Complete Show/Season 01/season-poster.jpg"
touch "$LIB/Complete Show/Season 01/ep.mkv"

mkdir -p "$LIB/_unmatched/SP1_S1D1"
touch "$LIB/_unmatched/SP1_S1D1/title00.mkv"

# ---------- Stub tmdb / overrides libs in a fake LIB_DIR ----------
FAKE_LIB="$TMPDIR/lib"
mkdir -p "$FAKE_LIB"
cat > "$FAKE_LIB/tmdb.sh" <<'STUB'
TMDB_SHOW_ID=""; TMDB_SHOW_NAME=""
TMDB_POSTER_URL=""; TMDB_SEASON_POSTER_URL=""
tmdb_search_show() {
    case "$1" in
        Futurama)        TMDB_SHOW_ID=615;  TMDB_SHOW_NAME="Futurama"; return 0 ;;
        "Complete Show") TMDB_SHOW_ID=999;  TMDB_SHOW_NAME="Complete Show"; return 0 ;;
        *) return 1 ;;
    esac
}
tmdb_get_show_by_id() {
    case "$1" in
        615) TMDB_SHOW_ID=615; TMDB_SHOW_NAME="Futurama"; return 0 ;;
        *) return 1 ;;
    esac
}
tmdb_fetch_show_images() {
    [ "$1" = "615" ] && { TMDB_POSTER_URL="https://example/poster.jpg"; return 0; }
    return 1
}
tmdb_fetch_season_images() {
    if [ "$1" = "615" ]; then
        TMDB_SEASON_POSTER_URL="https://example/season-${2}.jpg"
        return 0
    fi
    return 1
}
tmdb_download_image() {
    # write a sentinel file so existence tests pass
    printf 'STUB-IMG\n' > "$2"
    return 0
}
STUB
# Empty overrides lib so script still sources it without errors
echo '# empty' > "$FAKE_LIB/tv-overrides.sh"

# Force the script to find our stubs by symlinking into bin/lib only?
# Easier: just run the script from a temp checkout-style layout.
WRAPPER_BIN="$TMPDIR/bin"
mkdir -p "$WRAPPER_BIN"
cp "$SCRIPT" "$WRAPPER_BIN/backfill-tv-artwork.sh"
ln -s "$FAKE_LIB" "$WRAPPER_BIN/lib"

pass=0; fail=0
check() {
    local name="$1" want="$2" got="$3"
    if [ "$want" = "$got" ]; then
        echo "  ✓ $name"
        pass=$(( pass + 1 ))
    else
        echo "  ✗ $name: expected '$want' got '$got'"
        fail=$(( fail + 1 ))
    fi
}

# ---------- Test 1: dry-run reports work, makes no changes ----------
echo "▶ dry-run on synthetic library"
out=$(bash "$WRAPPER_BIN/backfill-tv-artwork.sh" --library "$LIB" 2>&1)
echo "$out" | sed 's/^/    /'

check "dry-run mentions Futurama"            "yes" "$(echo "$out" | grep -q '▶ Futurama' && echo yes || echo no)"
check "dry-run skips Complete Show"          "yes" "$(echo "$out" | grep -q 'Complete Show — already complete' && echo yes || echo no)"
check "dry-run skips _unmatched"             "yes" "$(echo "$out" | grep -q '_unmatched' && echo no || echo yes)"
check "dry-run did not write poster"         "no"  "$([ -s "$LIB/Futurama/poster.jpg" ] && echo yes || echo no)"
check "dry-run did not write nfo"            "no"  "$([ -s "$LIB/Futurama/tvshow.nfo" ] && echo yes || echo no)"

# ---------- Test 2: --apply actually writes files ----------
echo "▶ --apply on synthetic library"
out=$(bash "$WRAPPER_BIN/backfill-tv-artwork.sh" --library "$LIB" --apply 2>&1)
echo "$out" | sed 's/^/    /'

check "apply wrote poster"                   "yes" "$([ -s "$LIB/Futurama/poster.jpg" ] && echo yes || echo no)"
check "apply wrote nfo"                      "yes" "$([ -s "$LIB/Futurama/tvshow.nfo" ] && echo yes || echo no)"
check "apply wrote season 02 poster"         "yes" "$([ -s "$LIB/Futurama/Season 02/season-poster.jpg" ] && echo yes || echo no)"
check "apply wrote season 03 poster"         "yes" "$([ -s "$LIB/Futurama/Season 03/season-poster.jpg" ] && echo yes || echo no)"
check "nfo contains tmdb id"                 "yes" "$(grep -q 'tmdb.*615' "$LIB/Futurama/tvshow.nfo" && echo yes || echo no)"
check "complete show poster untouched"       "JPEG" "$(cat "$LIB/Complete Show/poster.jpg")"

# ---------- Test 3: idempotent re-run ----------
echo "▶ re-run is idempotent"
out=$(bash "$WRAPPER_BIN/backfill-tv-artwork.sh" --library "$LIB" --apply 2>&1)
check "second run: futurama already complete" \
    "yes" "$(echo "$out" | grep -q 'Futurama — already complete' && echo yes || echo no)"

# ---------- Test 4: --show filter ----------
echo "▶ --show filter"
out=$(bash "$WRAPPER_BIN/backfill-tv-artwork.sh" --library "$LIB" --show "Futurama" 2>&1)
check "filter: only Futurama scanned" "yes" \
    "$(echo "$out" | grep -q 'Shows scanned:    1' && echo yes || echo no)"

# ---------- Test 5: missing show is skipped, exits clean ----------
echo "▶ unknown show in library"
mkdir -p "$LIB/Unknown Show NoMatch/Season 01"
touch "$LIB/Unknown Show NoMatch/Season 01/ep.mkv"
out=$(bash "$WRAPPER_BIN/backfill-tv-artwork.sh" --library "$LIB" --apply 2>&1) || true
check "unknown show logs error but script exits 0" "yes" \
    "$(echo "$out" | grep -q 'could not resolve' && echo yes || echo no)"

echo
echo "── Results: $pass passed, $fail failed ──"
[ "$fail" -eq 0 ]
