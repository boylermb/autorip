# autorip

Automatic disc ripping for Linux. Insert a DVD, Blu-ray, 4K UHD Blu-ray, or
Audio CD and it gets ripped, transcoded, and organized automatically.

| Disc Type      | Tool       | Output       | Destination                      |
|----------------|------------|--------------|----------------------------------|
| DVD            | MakeMKV    | MKV (H.265)  | `Video/TV/` or `Video/Movies/`  |
| Blu-ray        | MakeMKV    | MKV (H.265)  | `Video/TV/` or `Video/Movies/`  |
| 4K UHD Blu-ray | MakeMKV    | MKV (original)| `Video/TV/` or `Video/Movies/` |
| Audio CD       | abcde      | MP3/FLAC     | `Audio/Music/Artist/Album/`      |

## Features

- **Zero-touch operation** — insert disc, walk away, disc ejects when done
- **4K UHD Blu-ray support** — rips via MakeMKV with LibreDrive, preserves
  original H.265/HEVC with HDR10/Dolby Vision metadata (no re-encode)
- **Per-title streaming** — each title is enqueued for GPU transcoding as soon
  as it finishes ripping (don't wait for the whole disc)
- **GPU transcode queue** — NVIDIA NVENC (hevc_nvenc) on a dedicated GPU node,
  with CPU fallback (libx265) if no GPU is available
- **TV show detection** — disc titles like `FUTURAMA_S2D1` are automatically
  parsed and placed into `TV/Futurama/Season 02/Futurama - S02E01.mkv`
- **Movie identification** — mnamer (MIT) matches against TMDb for proper
  `Movies/Title (Year)/Title (Year).mkv` naming
- **Multi-node support** — any number of ripping nodes can enqueue to a shared
  NFS queue for a single GPU worker
- **Web API agent** — lightweight Flask API on each node for status, logs,
  remote rip/eject, and queue monitoring
- **Dashboard-ready** — JSON status files for integration with any dashboard

## Quick Start

```bash
# Install on a ripping node
sudo make install

# Install on the GPU transcode node (also a ripping node)
sudo make install-worker

# Edit configuration
sudo vim /etc/autorip/autorip.conf

# Verify
sudo systemctl status autorip-agent
```

## Requirements

- Debian 12+ (or compatible)
- Optical drive(s) — UHD Blu-ray requires a LibreDrive-compatible drive
  (e.g. LG WH16NS40/60, ASUS BW-16D1HT with compatible firmware)
- MakeMKV 1.18+ (installed automatically from source)
- ffmpeg with libx265 (or NVIDIA GPU + NVENC for hardware encoding)
- Python 3.9+

## Configuration

All settings live in `/etc/autorip/autorip.conf`:

```bash
# Root output directory for all ripped media
OUTPUT_BASE="/srv/nas/Media"

# Minimum title length in seconds (skip menus/trailers)
MIN_TITLE_SECONDS=120

# Audio CD output format (mp3, flac, ogg)
CD_FORMAT=mp3

# Parallel encoding processes for abcde
MAX_ENCODE_PROCS=2

# Assumed episodes per disc for TV season numbering
EPISODES_PER_DISC=4

# Keep original video for 4K UHD rips (skip transcode)
UHD_KEEP_ORIGINAL=yes

# mnamer movie format (Jellyfin-compatible)
MNAMER_MOVIE_FORMAT="{name} ({year})/{name} ({year}){extension}"

# API agent port
AGENT_PORT=7800

# MakeMKV version (for building from source)
MAKEMKV_VERSION="1.18.3"

# Paths for systemd sandboxing (space-separated)
WRITABLE_PATHS="/srv/nas /srv/nfs/data"
```

## Directory Structure

```
/etc/autorip/autorip.conf        — Configuration
/usr/local/bin/autorip.sh        — Main rip script
/usr/local/bin/transcode-worker.sh — GPU transcode queue processor
/usr/local/lib/autorip-agent/    — Flask web API
/etc/udev/rules.d/99-autorip.rules — Trigger on disc insert
/etc/systemd/system/autorip@.service
/etc/systemd/system/transcode-worker.service
/etc/systemd/system/transcode-worker.timer
/etc/systemd/system/autorip-agent.service
/etc/abcde.conf                  — Audio CD ripping config
/var/lib/autorip/                — Status files, MakeMKV config
/var/log/autorip/                — Logs
```

## How It Works

1. **Disc insert** → udev rule triggers `autorip@srX.service`
2. **autorip.sh** detects disc type, rips with MakeMKV (video) or abcde (audio)
3. Each ripped title is **immediately enqueued** as a JSON job to `.autorip-queue/`
4. **transcode-worker.sh** (GPU node, 30s timer) picks up jobs:
   - DVD (MPEG-2) → transcode to H.265, rename into Jellyfin-compatible paths
   - Blu-ray (H.264) → skip transcode, rename into Jellyfin-compatible paths
   - UHD Blu-ray (H.265 + HDR) → skip transcode, preserve original quality
5. Disc **ejects** automatically when ripping is complete

## Ansible Integration

If you use Ansible to manage your nodes, see [`ansible/`](ansible/) for a thin
role that clones this repo and runs `make install` with your config.

## License

MIT
