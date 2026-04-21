#!/usr/bin/env bash
# Standalone test for tv-overrides.sh.  No network required.
set -euo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SELF_DIR/../bin/lib/tv-overrides.sh"

log() { :; }   # silence override logs

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

OVERRIDES="$TMPDIR/tv-overrides.json"
cat > "$OVERRIDES" <<'JSON'
{
  "shows": {
    "the sopranos": {"tmdb_id": 1398, "name": "The Sopranos"},
    "futurama":     {"tmdb_id": 615},
    "sp1":          {"name": "Star Trek The Original Series"}
  },
  "label_rewrites": [
    {"pattern": "^TOS_",        "show": "Star Trek"},
    {"pattern": "^STARGATE_SG", "show": "Stargate SG-1"}
  ]
}
JSON
export TV_OVERRIDES_FILE="$OVERRIDES"

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

# ---------- Test 1: shows-map hit by lowercase name, both fields set
TV_OVERRIDE_SHOW=""; TV_OVERRIDE_TMDB_ID=""
tv_apply_overrides "The Sopranos" "THE_SOPRANOS_S1D1" && r=ok || r=miss
check "sopranos: applied"          "ok"            "$r"
check "sopranos: name override"    "The Sopranos"  "$TV_OVERRIDE_SHOW"
check "sopranos: forced TMDb id"   "1398"          "$TV_OVERRIDE_TMDB_ID"

# ---------- Test 2: shows-map hit, only tmdb_id (no name override)
TV_OVERRIDE_SHOW=""; TV_OVERRIDE_TMDB_ID=""
tv_apply_overrides "Futurama" "FUTURAMA_S2D1" && r=ok || r=miss
check "futurama: applied"          "ok"            "$r"
check "futurama: id only"          "615"           "$TV_OVERRIDE_TMDB_ID"
check "futurama: name unchanged"   ""              "$TV_OVERRIDE_SHOW"

# ---------- Test 3: shows-map hit, only name (no tmdb_id)
TV_OVERRIDE_SHOW=""; TV_OVERRIDE_TMDB_ID=""
tv_apply_overrides "Sp1" "SP1_S1D1" && r=ok || r=miss
check "sp1: applied"               "ok"                              "$r"
check "sp1: name override"         "Star Trek The Original Series"   "$TV_OVERRIDE_SHOW"
check "sp1: no id"                 ""                                "$TV_OVERRIDE_TMDB_ID"

# ---------- Test 4: label_rewrite — disc title regex match
TV_OVERRIDE_SHOW=""; TV_OVERRIDE_TMDB_ID=""
tv_apply_overrides "Tos" "TOS_DISC_3" && r=ok || r=miss
check "label_rewrite TOS_: applied" "ok"          "$r"
check "label_rewrite TOS_: show"    "Star Trek"   "$TV_OVERRIDE_SHOW"

# ---------- Test 5: no matches at all
TV_OVERRIDE_SHOW=""; TV_OVERRIDE_TMDB_ID=""
if tv_apply_overrides "Some Random Show" "SOME_RANDOM_SHOW_S1D1"; then r=applied; else r=miss; fi
check "no match: returns miss"     "miss"  "$r"
check "no match: name empty"       ""      "$TV_OVERRIDE_SHOW"
check "no match: id empty"         ""      "$TV_OVERRIDE_TMDB_ID"

# ---------- Test 6: missing override file is a clean miss
export TV_OVERRIDES_FILE="$TMPDIR/does-not-exist.json"
TV_OVERRIDE_SHOW=""; TV_OVERRIDE_TMDB_ID=""
if tv_apply_overrides "Anything" "ANYTHING_S1D1"; then r=applied; else r=miss; fi
check "missing file: clean miss"   "miss"  "$r"

# ---------- Test 7: invalid JSON file → clean miss (no crash)
echo "this is not json" > "$TMPDIR/bad.json"
export TV_OVERRIDES_FILE="$TMPDIR/bad.json"
TV_OVERRIDE_SHOW=""; TV_OVERRIDE_TMDB_ID=""
if tv_apply_overrides "Anything" "ANYTHING_S1D1"; then r=applied; else r=miss; fi
check "bad json: clean miss"       "miss"  "$r"

echo
echo "Results: $pass passed, $fail failed"
exit $fail
