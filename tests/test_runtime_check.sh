#!/usr/bin/env bash
# Standalone test for tv-runtime-check.sh.  Mocks ffprobe via PATH so no
# real video files are needed.
set -euo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SELF_DIR/../bin/lib/tv-runtime-check.sh"

log() { :; }   # silence runtime-check logs

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

# Create a fake ffprobe that maps file basename -> duration in seconds.
# Encoding: file basename "ep_<minutes>.mkv" -> minutes*60 secs.
mkdir -p "$TMPDIR/bin"
cat > "$TMPDIR/bin/ffprobe" <<'SH'
#!/usr/bin/env bash
# args: -v error -show_entries format=duration -of default=...:nokey=1 <file>
file="${!#}"
base=$(basename "$file")
# Extract integer minutes from name like "ep_22.mkv" or "ep_45.mkv"
mins=$(echo "$base" | sed -E 's/^ep_([0-9]+)\.mkv$/\1/')
[ -z "$mins" ] && exit 1
awk -v m="$mins" 'BEGIN { printf("%.3f\n", m * 60.0) }'
SH
chmod +x "$TMPDIR/bin/ffprobe"
export PATH="$TMPDIR/bin:$PATH"

# Confirm the mock is hit (sanity check the harness, not the lib)
if [ "$(ffprobe -v error -show_entries format=duration -of default=nokey=1 "$TMPDIR/ep_22.mkv" 2>/dev/null | awk '{print int($1)}')" != "1320" ]; then
    echo "ffprobe mock broken; aborting"; exit 2
fi

# Create stub mkv files (just need them to exist for [ -f ] check)
for m in 22 22 22 22 45 90; do
    : > "$TMPDIR/ep_${m}.mkv"
done

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

# Each test resets TMDB_EP_RUNTIMES + TMDB_EP_NAMES from scratch
reset_tmdb() {
    declare -gA TMDB_EP_RUNTIMES=()
    declare -gA TMDB_EP_NAMES=()
}

# ---------- Test 1: 4 episodes, all close to TMDb (22 min each)
reset_tmdb
TMDB_EP_RUNTIMES[1]=22; TMDB_EP_NAMES[1]="Ep One"
TMDB_EP_RUNTIMES[2]=22; TMDB_EP_NAMES[2]="Ep Two"
TMDB_EP_RUNTIMES[3]=22; TMDB_EP_NAMES[3]="Ep Three"
TMDB_EP_RUNTIMES[4]=22; TMDB_EP_NAMES[4]="Ep Four"
TV_RUNTIME_MISMATCH=""; TV_RUNTIME_PLAN=""
tv_check_runtime_match 1 \
    "$TMPDIR/ep_22.mkv" "$TMPDIR/ep_22.mkv" "$TMPDIR/ep_22.mkv" "$TMPDIR/ep_22.mkv"
check "all-match disc: not flagged"  ""  "$TV_RUNTIME_MISMATCH"

# ---------- Test 2: 4 episodes but 3 of them are way off (45 vs 22)
reset_tmdb
TMDB_EP_RUNTIMES[1]=22; TMDB_EP_NAMES[1]="Aired-1"
TMDB_EP_RUNTIMES[2]=22; TMDB_EP_NAMES[2]="Aired-2"
TMDB_EP_RUNTIMES[3]=22; TMDB_EP_NAMES[3]="Aired-3"
TMDB_EP_RUNTIMES[4]=22; TMDB_EP_NAMES[4]="Aired-4"
TV_RUNTIME_MISMATCH=""; TV_RUNTIME_PLAN=""
tv_check_runtime_match 1 \
    "$TMPDIR/ep_22.mkv" "$TMPDIR/ep_45.mkv" "$TMPDIR/ep_45.mkv" "$TMPDIR/ep_45.mkv"
check "3-of-4 mismatched: flagged"   "1"  "$TV_RUNTIME_MISMATCH"

# Plan should mention MISMATCH at least 3 times
mismatch_count=$(echo "$TV_RUNTIME_PLAN" | grep -c "MISMATCH" || true)
check "plan: 3 MISMATCH rows"        "3"  "$mismatch_count"

# ---------- Test 3: only 1 mismatched out of 4 — not flagged (under threshold)
reset_tmdb
TMDB_EP_RUNTIMES[1]=22; TMDB_EP_RUNTIMES[2]=22
TMDB_EP_RUNTIMES[3]=22; TMDB_EP_RUNTIMES[4]=22
TV_RUNTIME_MISMATCH=""; TV_RUNTIME_PLAN=""
tv_check_runtime_match 1 \
    "$TMPDIR/ep_22.mkv" "$TMPDIR/ep_22.mkv" "$TMPDIR/ep_45.mkv" "$TMPDIR/ep_22.mkv"
check "1-of-4 mismatched: not flagged" "" "$TV_RUNTIME_MISMATCH"

# ---------- Test 4: short disc (2 episodes), 1 mismatch — flagged via
# trigger=checked-1=1 logic
reset_tmdb
TMDB_EP_RUNTIMES[5]=22; TMDB_EP_RUNTIMES[6]=22
TV_RUNTIME_MISMATCH=""; TV_RUNTIME_PLAN=""
tv_check_runtime_match 5 "$TMPDIR/ep_22.mkv" "$TMPDIR/ep_90.mkv"
check "2-ep disc, 1 mismatch: flagged" "1" "$TV_RUNTIME_MISMATCH"

# ---------- Test 5: TMDb runtimes empty — silent no-op
reset_tmdb
TV_RUNTIME_MISMATCH=""; TV_RUNTIME_PLAN=""
tv_check_runtime_match 1 "$TMPDIR/ep_22.mkv" "$TMPDIR/ep_22.mkv"
check "no TMDb runtimes: silent skip" "" "$TV_RUNTIME_MISMATCH"
check "no TMDb runtimes: empty plan"  "" "$TV_RUNTIME_PLAN"

# ---------- Test 6: TMDb runtime missing for some eps — only checks ones it has
reset_tmdb
TMDB_EP_RUNTIMES[1]=22                    # ep 1 known
TMDB_EP_NAMES[1]="One"
# ep 2 unknown to TMDb
TMDB_EP_RUNTIMES[3]=22; TMDB_EP_NAMES[3]="Three"
TV_RUNTIME_MISMATCH=""; TV_RUNTIME_PLAN=""
tv_check_runtime_match 1 \
    "$TMPDIR/ep_22.mkv" "$TMPDIR/ep_45.mkv" "$TMPDIR/ep_22.mkv"
# Only 2 actually checked (eps 1 and 3); both match.  Ep 2 row should
# show "no-tmdb-runtime"
check "missing TMDb data: not flagged" "" "$TV_RUNTIME_MISMATCH"
check "missing TMDb data: row marked"  "1" "$(echo "$TV_RUNTIME_PLAN" | grep -c 'no-tmdb-runtime' || true)"

echo
echo "Results: $pass passed, $fail failed"
exit $fail
