# TV Rip Improvements Plan

> **Status:** Proposed, not started. Documented April 21, 2026.
> **Scope:** Improve TV disc handling in `autorip.sh` + `transcode-worker.sh`:
> better cover art, better episode-title matching, better handling of
> "compilation / play-all" titles.
> **Non-goal:** Movie ripping (mnamer flow) is out of scope.

---

## 1. Problems observed

### 1.1 Cover art is missing for TV

Cover art is only fetched for **audio CDs** (Music library). TV shows land
in `Video/TV/<Show>/Season XX/` with no `poster.jpg`, `season-poster.jpg`,
or `show.nfo`. Jellyfin/Plex then either show a placeholder or scrape on
their own (often with the wrong show).

### 1.2 Episode titles are wrong or missing

`tv_rename_file()` in `transcode-worker.sh` already calls TMDb to look up
episode titles, but it goes wrong in several common ways:

| Cause | Symptom |
|---|---|
| **Show name from disc label is wrong** (e.g. `THE_SOPRANOS_S1D1` → "The Sopranos" works; `SP1_S1D1` → "Sp1" fails TMDb search) | All episodes named `Show - S01E01.mkv` (no title) |
| **`EPISODES_PER_DISC` is global, fixed at 4** (config default) | Off-by-N numbering on any show that doesn't have exactly 4 episodes per disc — very common (Futurama=4, Sopranos=3-5/disc, miniseries=1-2/disc, anime=5-7/disc) |
| **Disc title order ≠ broadcast order** | Some box sets present episodes in production order, not aired order. TMDb gives aired-order titles, so episode 3 gets episode 4's title |
| **Multiple seasons share one disc** (anniversary box sets) | `EPISODES_PER_DISC` math breaks immediately |
| **First disc of a season includes "pilot" as a separate title** | All subsequent numbering shifts by one |

### 1.3 "Play-all" / compilation titles slip through

`autorip.sh` filters titles ≥2.5× the median duration when there are ≥3
titles. This catches the common case but misses:

- **2-title discs** (1 episode + 1 play-all): filter never runs (needs ≥3).
- **Play-all that's <2.5× median**: e.g. 4 × 45-min episodes + 1 × 95-min
  play-all (only 2.1× median because the play-all sometimes drops intros).
- **Play-all that's exactly the same length as one episode** because of
  cell repetition tricks DVDs use.
- **Multiple compilation titles** (some discs have "play all + extras",
  "play all without intros", etc.)

When one slips through, the worker treats it as a real episode → numbering
breaks for the rest of the disc *and* a giant duplicate file gets posted
to review.

---

## 2. Target outcomes

After this work:

1. Every TV show directory has at least `poster.jpg` and `show.nfo`; every
   `Season XX/` has `season-poster.jpg`. Jellyfin/Plex pick these up
   automatically.
2. Episode titles match TMDb **most of the time** for common cases; the
   pipeline **never silently mis-numbers** — it either gets it right or
   produces a file flagged for human review.
3. Compilation/play-all titles are dropped reliably regardless of how many
   titles are on the disc.
4. There's a documented **per-show override** mechanism for the cases
   automation can't solve (anthology series, out-of-order box sets,
   miniseries with weird disc layouts).

---

## 3. Strategy

### 3.1 Cover art for TV

Add `fetch_tv_artwork(show, season)` to `transcode-worker.sh`, called
once per disc after `parse_tv_disc_title` succeeds.

Sources, in order:
1. **TMDb** `/tv/{id}/images` → `poster_path`. Already authenticated;
   we already do the show-ID lookup for episode titles, so the ID is in
   hand for free.
2. **TMDb** `/tv/{id}/season/{n}/images` → season poster.
3. **fanart.tv** (optional, requires API key) — has higher-quality posters
   and `clearart`/`logo` assets Jellyfin can use.

Drop files at:
```
$UNREVIEWED_TV/<Show>/poster.jpg
$UNREVIEWED_TV/<Show>/show.nfo                  # tmdbid + tvdbid
$UNREVIEWED_TV/<Show>/Season NN/season-poster.jpg
```

`show.nfo` is a tiny XML file — Jellyfin/Plex use it to short-circuit
their own scraper and pick the *correct* show even when the directory
name is ambiguous.

### 3.2 Episode-title accuracy

Three layered improvements:

#### a) Canonicalize the show name before TMDb lookup

Right now we `tr '_' ' '` and title-case the disc label. Improve with:

1. Strip common DVD-label noise (`DISC_1`, `_REGION_1`, season tags,
   `_BLURAY`, `_DVD`, `_HD`).
2. Search TMDb `/search/tv?query=…` and **require ≥1 result**. If 0 results,
   try removing the last word and re-search (handles abbreviations).
3. If still 0 results, **abort the rename** and route the disc to a new
   `$UNREVIEWED_TV/_unmatched/<disc-label>/` bucket for manual triage —
   never silently generate a wrong-show name.

#### b) Per-disc episode count from MakeMKV, not from config

Instead of `EPISODES_PER_DISC=4` (global config), compute episode count
per disc as **the number of titles that survive the play-all filter**
(see 3.3). Then:

- The first disc starts at E01.
- Subsequent discs start at `previous_disc_last_ep + 1`.

Track this in a per-show state file under `$STATE_DIR/tv-progress/<show>-S<season>.json`:

```json
{
  "show": "Futurama",
  "season": 2,
  "discs": {
    "1": {"episodes": [1, 2, 3, 4], "ripped_at": "..."},
    "2": {"episodes": [5, 6, 7, 8], "ripped_at": "..."}
  },
  "next_episode": 9
}
```

If a disc is re-ripped, look up its existing entry and reuse its episode
range. If discs are ripped out of order (disc 3 before disc 2), warn and
defer naming until disc 2 arrives — leave files in
`$UNREVIEWED_TV/_pending/<show>-S<season>D<n>/`.

#### c) Detect production-vs-aired order mismatches

After fetching episode titles from TMDb, also fetch episode **runtimes**.
Compare to the actual MKV durations. If the order disagrees by more than
a small tolerance (e.g. 3+ episodes' runtimes mismatch), flag the disc:
write the rename plan as `episodes-plan.txt` next to the files in
`_pending/` and let the user confirm or reorder before merging into the
unreviewed tree.

### 3.3 Better play-all/compilation detection

Replace the single 2.5× heuristic with a layered check that runs even
when there are only 2 titles:

1. **Duration outlier** (current): drop any title whose duration is
   ≥2.0× the median (lower threshold) of the other titles, when there
   are ≥3 titles.
2. **Sum-of-others**: drop any title whose duration is within ±5% of
   `sum(other_titles_durations)`. Catches play-all reliably even with 2
   titles (1 episode of 45m + 1 "play-all" of 45m wouldn't trigger, but
   that case is indistinguishable from "really one episode" anyway).
3. **Cell/segment count via MakeMKV TINFO attribute 25** (segment count):
   play-all titles typically have segment counts equal to or very close
   to the sum of segment counts of other titles. This is the most
   reliable signal — drop any title where `segments_in_title ≥
   sum(segments_in_others) * 0.9`.
4. **Filename hint**: MakeMKV often names play-all titles `title00.mkv`
   while episodes start at `title01.mkv`. Use as a tie-breaker only.
5. **Repeated-content fingerprint** (last resort, expensive): if the
   first 30s of a candidate title matches the first 30s of any other
   title (audio fingerprint via `chromaprint`/`fpcalc`), it's a
   compilation. Skip by default; enable via `DETECT_COMPILATIONS_FUZZY=yes`.

When a title is dropped as a compilation, log it with the reason; never
delete the file silently.

### 3.4 Per-show overrides

Some shows will always need manual help (anthologies, re-releases with
bonus episodes, foreign-numbering box sets). Add an overrides file:

```yaml
# /etc/autorip/tv-overrides.yml
"the sopranos":
  tmdb_id: 1398
  episode_order: aired               # default; can be 'production' | 'dvd'
  disc_layouts:
    "S1D1": [1, 2, 3, 4]
    "S1D2": [5, 6, 7]
    "S1D3": [8, 9, 10]

"futurama":
  episodes_per_disc: 4               # explicit, overrides auto-detect

"_unmatched_disc_label_pattern":
  "^SP[0-9]+_": "Star Trek The Original Series"
```

`tv_rename_file` checks this file first; if a key matches, the manual
mapping wins.

### 3.5 Failure mode: `_unmatched/` and `_pending/`

Two new top-level buckets under `$UNREVIEWED_TV`:

- `_unmatched/<disc-label>/` — disc-title parse failed or TMDb search
  returned no results. Files are kept with their MakeMKV names.
- `_pending/<show>-S<season>D<disc>/` — disc rip succeeded but naming
  is uncertain (out-of-order disc, episode-count disagreement). Includes
  `episodes-plan.txt` showing the proposed mapping.

The media-review web app gets two new tabs to handle these. Until reviewed,
nothing reaches `Video/TV/`.

---

## 4. Migration plan

| # | Step | Size | Risk | Status | Done-when |
|---|---|---|---|---|---|
| 1 | Centralize TMDb client into `bin/lib/tmdb.sh` (callable from both `autorip.sh` and `transcode-worker.sh`); add show-images endpoint | S | none (refactor of existing duplicated code) | ✅ done | Both scripts call the helper, identical behaviour |
| 2 | Add `fetch_tv_artwork()` + write `poster.jpg` / `tvshow.nfo` / `season-poster.jpg` | S | low | ✅ done | After a TV rip, the unreviewed directory contains the three artwork files |
| 3 | Replace global `EPISODES_PER_DISC` math with per-show progress state file (`bin/lib/tv-progress.sh`) | M | medium (changes numbering semantics; legacy math kept as fallback) | ✅ done | New rips number correctly with variable episode counts; old rips untouched |
| 4 | Add 3.3 layered play-all detection (sum-of-others + segment-count) | M | low (additive checks; existing 2.5× rule stays as fallback) | ✅ done | 6 unit tests in `tests/test_playall_detection.sh` cover box-set, 2-title, and segment-outlier cases |
| 5 | Add show-name canonicalization + `_unmatched/` routing | S | low | ✅ done | Disc with garbage label lands in `_unmatched/`, not in `Video/TV/Sp1/` |
| 6 | Add `tv-overrides.yml` support | S | low | ✅ done (JSON, not YAML — no PyYAML dep) | Override file with one entry produces the overridden naming |
| 7 | Add production-vs-aired mismatch detection + `_pending/` routing | M | medium (needs runtime-comparison heuristic tuning) | ✅ done | A box set known to be production-order routes to `_pending/` |
| 8 | media-review UI: `_unmatched/` + `_pending/` tabs | M | low | ⏳ next | Web UI exposes both buckets; user can move files into final naming |
| 9 | Backfill: re-process existing TV directories that have no `poster.jpg` | S | none (read-only against TMDb; only writes artwork files) | | All `Video/TV/<Show>/` have `poster.jpg` |

Each step is an independent commit. Steps 1, 2, 4, 5, 6, 9 are the
high-value/low-risk core. Steps 3, 7, 8 are the bigger bets.

---

## 5. Open questions

1. **TMDb API key:** currently hardcoded in `transcode-worker.sh`. Move to
   `autorip.conf`? Use the existing key as fallback if unset?
2. **Multi-rip de-duplication:** if a disc is ripped twice (re-insert),
   do we overwrite or quarantine? Today: overwrite.
3. **Anthology shows** (Black Mirror, Twilight Zone): episodes are
   essentially standalone movies — should they go through mnamer instead?
   Out of scope for this plan but worth noting.
4. **Subtitle ripping:** orthogonal to episode naming but often missing.
   Defer to a separate plan.
5. **Existing in-flight rips** when step 3 ships: do we freeze numbering
   for shows with an active progress state? Probably yes — only new shows
   use the new system until manually reset.

---

## 6. Success criteria

- Pick any TV box set from the shelf, insert disc 1: rip lands in
  `Video/TV/<Correct Show>/Season NN/` with `Show - SnnE01 - Title.mkv`
  through `Show - SnnE0M - Title.mkv` (M = actual episode count).
- `poster.jpg` and `show.nfo` exist; Jellyfin shows the right show metadata
  on first scan.
- Disc 2 of the same set continues numbering correctly.
- A disc with a play-all title rips the correct N episodes, never N+1.
- A disc whose label can't be parsed lands in `_unmatched/`, not silently
  mis-named.
