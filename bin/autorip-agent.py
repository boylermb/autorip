#!/usr/bin/env python3
"""
autorip-agent — Lightweight API running on each cluster node.
https://github.com/boylermb/autorip

Provides:
    GET  /status          — Current rip status (JSON)
    GET  /log             — Recent autorip log output
    GET  /art             — Album cover art (JPEG) if available
    POST /rip             — Trigger a rip on the specified device
    POST /eject           — Eject the specified device
    GET  /transcode-queue — Shared GPU transcode queue state
    GET  /review/jobs     — List all items pending review (from .unreviewed/ dir)
    POST /review/edit     — Edit metadata of an unreviewed item
    POST /review/rename   — Rename a media file within an unreviewed item
    GET  /review/stream   — Stream a media file for playback
    POST /review/approve  — Approve a single unreviewed item (moves to library)
    POST /review/reject   — Reject a single unreviewed item (deletes)
    GET  /health          — Health check

Configuration is read from /etc/autorip/autorip.conf (or $AUTORIP_CONF).
"""

import json
import os
import re
import subprocess
import glob
from flask import Flask, jsonify, request, send_file, abort

app = Flask(__name__)

# ---------- Load configuration from shell-style config file ----------
def load_config(path=None):
    """Parse a shell key=value config file into a dict."""
    if path is None:
        path = os.environ.get("AUTORIP_CONF", "/etc/autorip/autorip.conf")
    config = {}
    try:
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, _, value = line.partition("=")
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    config[key] = value
    except FileNotFoundError:
        pass
    return config

_config = load_config()

STATUS_DIR = "/var/lib/autorip"
LOG_DIR = "/var/log/autorip"
ART_DIR = "/var/lib/autorip"
HOSTNAME = os.uname().nodename

DEVICE_RE = re.compile(r"^sr[0-9]+$")

OUTPUT_BASE = _config.get("OUTPUT_BASE", "/mnt/nas")
AGENT_PORT = int(_config.get("AGENT_PORT", "7800"))
QUEUE_DIR = os.path.join(OUTPUT_BASE, ".autorip-queue")
UNREVIEWED_DIR = os.path.join(OUTPUT_BASE, ".unreviewed")


def _valid_job_id(job_id):
    """Validate a job_id: block path traversal while allowing Unicode filenames."""
    if not job_id or "\0" in job_id or "/" in job_id or "\\" in job_id:
        return False
    # Prevent ".." path components
    if ".." in job_id:
        return False
    return True


def read_transcode_queue():
    """Read all transcode job files from the shared queue directory.

    Returns a list of dicts with job info and state (queued/processing/done/error).
    Also includes the worker status if available.
    """
    jobs = []
    worker_status = {}

    if not os.path.isdir(QUEUE_DIR):
        return {"jobs": jobs, "worker": worker_status}

    # Read worker status
    worker_file = os.path.join(QUEUE_DIR, ".worker-status.json")
    try:
        with open(worker_file, "r") as f:
            worker_status = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        worker_status = {"state": "unknown"}

    # Scan for job files in all states
    for entry in sorted(os.listdir(QUEUE_DIR)):
        if entry.startswith("."):
            continue

        filepath = os.path.join(QUEUE_DIR, entry)
        if not os.path.isfile(filepath):
            continue

        # Determine state from extension
        if entry.endswith(".json"):
            state = "queued"
        elif entry.endswith(".processing"):
            state = "processing"
        elif entry.endswith(".done"):
            state = "done"
        elif entry.endswith(".review"):
            state = "review"
        elif entry.endswith(".error"):
            state = "error"
        else:
            continue

        try:
            with open(filepath, "r") as f:
                data = json.load(f)
            data["state"] = state
            data["job_file"] = entry
            # Per-file jobs: check if the source file still exists
            fpath = data.get("file_path", "")
            if fpath:
                data["file_exists"] = os.path.isfile(fpath)
                data["file_transcoding"] = os.path.isfile(
                    fpath.replace(".mkv", ".transcoding.mkv")
                )
            else:
                data["file_exists"] = False
                data["file_transcoding"] = False
            # Multi-file disc jobs: check each file
            for f in data.get("files", []):
                fp = f.get("file_path", "")
                if fp:
                    f["file_exists"] = os.path.isfile(fp)
            # Audio-cd jobs: check staging dir
            sdir = data.get("staging_dir", "")
            if sdir:
                data["staging_exists"] = os.path.isdir(sdir)
            jobs.append(data)
        except (json.JSONDecodeError, OSError):
            jobs.append({"job_file": entry, "state": state, "error": "unreadable"})

    return {"jobs": jobs, "worker": worker_status}


def read_status():
    """Read the local autorip status JSON.

    Checks for per-device status files (status-sr0.json, status-sr1.json, …)
    first, falling back to the legacy single status.json.
    """
    per_device = {}
    for path in sorted(glob.glob(os.path.join(STATUS_DIR, "status-sr*.json"))):
        dev = os.path.basename(path).replace("status-", "").replace(".json", "")
        try:
            with open(path, "r") as f:
                data = json.load(f)
            data["online"] = True
            data["hostname"] = HOSTNAME
            data["device"] = dev
            art_path = os.path.join(ART_DIR, f"cover-{dev}.jpg")
            data["has_art"] = os.path.exists(art_path)
            per_device[dev] = data
        except (FileNotFoundError, json.JSONDecodeError):
            pass

    if per_device:
        return per_device

    # Legacy fallback: single status.json
    status_file = os.path.join(STATUS_DIR, "status.json")
    try:
        with open(status_file, "r") as f:
            data = json.load(f)
        data["online"] = True
        data["hostname"] = HOSTNAME
        art_path = os.path.join(ART_DIR, f"cover-{dev}.jpg")
        data["has_art"] = os.path.exists(art_path)
        dev = data.get("device", "sr0") or "sr0"
        return {dev: data}
    except (FileNotFoundError, json.JSONDecodeError):
        return {
            "sr0": {
                "hostname": HOSTNAME,
                "online": True,
                "status": "idle",
                "device": "sr0",
                "disc_type": "",
                "title": "",
                "progress": "",
                "artist": "",
                "album": "",
                "tracks": [],
                "tracks_total": 0,
                "tracks_completed": 0,
                "current_track": "",
                "has_art": False,
                "updated": "",
            }
        }


def read_log(device="sr0"):
    """Read the autorip log for a device."""
    log_file = os.path.join(LOG_DIR, f"{device}.log")
    try:
        with open(log_file, "r") as f:
            # Return last 200 lines
            lines = f.readlines()
            return "".join(lines[-200:])
    except FileNotFoundError:
        return "No log file found."


def parse_progress(device="sr0"):
    """Parse the autorip log for the latest MakeMKV/ffmpeg progress.

    Returns a dict with:
      - total_percent: overall rip progress (0-100), or -1 if unknown
      - current_percent: current title/file progress (0-100), or -1
      - stage: 'ripping' | 'transcoding' | 'renaming' | 'idle'
      - current_action: the latest "Current action:" string from MakeMKV
    """
    log_file = os.path.join(LOG_DIR, f"{device}.log")
    total_pct = -1
    current_pct = -1
    stage = "idle"
    current_action = ""

    try:
        with open(log_file, "rb") as f:
            # Read last 8KB to avoid scanning huge logs
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - 8192))
            tail = f.read().decode("utf-8", errors="replace")
    except FileNotFoundError:
        return {
            "total_percent": -1,
            "current_percent": -1,
            "stage": "idle",
            "current_action": "",
        }

    # Determine stage from log keywords (scan bottom-up for latest)
    lines = tail.splitlines()
    for line in reversed(lines):
        if "ERROR" in line or "failed" in line.lower():
            stage = "error"
            break
        if "Transcoding" in line or "transcode" in line.lower():
            stage = "transcoding"
            break
        if "mnamer" in line.lower() or "Identifying media" in line:
            stage = "renaming"
            break
        if "MakeMKV" in line or "Ripping" in line or "Total progress" in line:
            stage = "ripping"
            break
        if "Done" in line or "complete" in line.lower():
            stage = "complete"
            break

    # Parse MakeMKV progress: "Total progress - XX%"
    total_re = re.compile(r"Total progress - (\d+)%")
    current_re = re.compile(r"Current progress - (\d+)%")
    action_re = re.compile(r"Current action: (.+)")

    for line in reversed(lines):
        if total_pct == -1:
            m = total_re.search(line)
            if m:
                total_pct = int(m.group(1))
        if current_pct == -1:
            m = current_re.search(line)
            if m:
                current_pct = int(m.group(1))
        if not current_action:
            m = action_re.search(line)
            if m:
                current_action = m.group(1).strip()
        if total_pct >= 0 and current_pct >= 0 and current_action:
            break

    return {
        "total_percent": total_pct,
        "current_percent": current_pct,
        "stage": stage,
        "current_action": current_action,
    }


def get_optical_drives():
    """List optical drives and their state."""
    drives = []
    for dev in sorted(glob.glob("/dev/sr*")):
        name = os.path.basename(dev)
        has_disc = False
        try:
            result = subprocess.run(
                ["udevadm", "info", "--query=property", f"--name={dev}"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            has_disc = "ID_CDROM_MEDIA=1" in result.stdout
        except Exception:
            pass
        drives.append({"device": name, "path": dev, "has_disc": has_disc})
    return drives


@app.route("/status")
def status():
    per_device = read_status()
    drives = get_optical_drives()
    drive_map = {d["device"]: d for d in drives}

    results = []
    # Emit one entry per known drive
    seen = set()
    for dev, data in per_device.items():
        data["drives"] = drives
        data["progress_info"] = parse_progress(dev)
        # Merge drive hardware info
        if dev in drive_map:
            data["has_disc"] = drive_map[dev].get("has_disc", False)
        results.append(data)
        seen.add(dev)

    # Add entries for any drives that have no status file (idle drives)
    for drv in drives:
        if drv["device"] not in seen:
            results.append({
                "hostname": HOSTNAME,
                "online": True,
                "status": "idle",
                "device": drv["device"],
                "disc_type": "",
                "title": "",
                "progress": "",
                "artist": "",
                "album": "",
                "tracks": [],
                "tracks_total": 0,
                "tracks_completed": 0,
                "current_track": "",
                "has_art": False,
                "has_disc": drv.get("has_disc", False),
                "updated": "",
                "drives": drives,
                "progress_info": {"total_percent": -1, "current_percent": -1, "stage": "idle", "current_action": ""},
            })

    return jsonify(results)


@app.route("/log")
def log_endpoint():
    device = request.args.get("device", "sr0")
    # Sanitize device name
    if not DEVICE_RE.match(device):
        return jsonify({"error": "Invalid device"}), 400
    return jsonify({"hostname": HOSTNAME, "log": read_log(device)})


@app.route("/art")
def art():
    device = request.args.get("device", "sr0")
    if not DEVICE_RE.match(device):
        abort(400)
    art_path = os.path.join(ART_DIR, f"cover-{device}.jpg")
    if os.path.exists(art_path):
        return send_file(art_path, mimetype="image/jpeg")
    abort(404)


@app.route("/rip", methods=["POST"])
def rip():
    """Trigger a rip by starting the autorip systemd service."""
    device = request.json.get("device", "sr0") if request.is_json else "sr0"
    # Sanitize
    if not DEVICE_RE.match(device):
        return jsonify({"error": "Invalid device"}), 400

    # Check if already ripping
    result = subprocess.run(
        ["systemctl", "is-active", f"autorip@{device}.service"],
        capture_output=True,
        text=True,
    )
    if result.stdout.strip() in ("active", "activating"):
        return jsonify({"error": f"Rip already in progress on {device}"}), 409

    # Start the service (--no-block so we don't wait for the rip to finish;
    # autorip@.service is Type=oneshot and can run for hours)
    result = subprocess.run(
        ["systemctl", "start", "--no-block", f"autorip@{device}.service"],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        return jsonify({"ok": True, "message": f"Rip started on /dev/{device}"})
    else:
        return jsonify(
            {"error": f"Failed to start rip: {result.stderr.strip()}"}
        ), 500


@app.route("/eject", methods=["POST"])
def eject():
    """Eject the disc from the specified device."""
    device = request.json.get("device", "sr0") if request.is_json else "sr0"
    if not DEVICE_RE.match(device):
        return jsonify({"error": "Invalid device"}), 400

    result = subprocess.run(
        ["eject", f"/dev/{device}"],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        # Clear stale status and cover art for this device
        status_file = os.path.join(STATUS_DIR, f"status-{device}.json")
        art_file = os.path.join(ART_DIR, f"cover-{device}.jpg")
        for f in (status_file, art_file):
            try:
                os.remove(f)
            except FileNotFoundError:
                pass
        return jsonify({"ok": True, "message": f"Ejected /dev/{device}"})
    else:
        return jsonify(
            {"error": f"Failed to eject: {result.stderr.strip()}"}
        ), 500


@app.route("/transcode-queue")
def transcode_queue():
    """Return the current state of the GPU transcode queue."""
    return jsonify(read_transcode_queue())


@app.route("/rip-log")
def rip_log():
    """Return the shared rip log (JSON array of all rips across all nodes).

    Query params:
      ?limit=N   — return only the most recent N entries (default: all)
    """
    rip_log_path = os.path.join(OUTPUT_BASE, ".rip-log.json")
    try:
        with open(rip_log_path, "r") as f:
            entries = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        entries = []

    limit = request.args.get("limit", type=int)
    if limit and limit > 0:
        entries = entries[-limit:]

    return jsonify(entries)


@app.route("/rip-log/art")
def rip_log_art():
    """Serve album cover art referenced in a rip-log entry.

    Query params:
      ?path=Audio/Music/Artist/Album/cover.jpg  — relative to OUTPUT_BASE
    """
    rel_path = request.args.get("path", "")
    if not rel_path:
        abort(400)
    # Prevent directory traversal
    safe = os.path.normpath(rel_path)
    if safe.startswith("..") or safe.startswith("/"):
        abort(400)
    full_path = os.path.join(OUTPUT_BASE, safe)
    if os.path.isfile(full_path):
        return send_file(full_path, mimetype="image/jpeg")
    abort(404)


@app.route("/rip-log/markdown")
def rip_log_markdown():
    """Serve the generated rip-log markdown file."""
    md_path = os.path.join(OUTPUT_BASE, ".rip-log.md")
    if os.path.isfile(md_path):
        return send_file(md_path, mimetype="text/markdown; charset=utf-8")
    abort(404)


WORKER_SCRIPT = os.path.join(
    _config.get("PREFIX", "/usr/local"), "bin", "transcode-worker.sh"
)


@app.route("/review/jobs")
def review_jobs():
    """List all items in the unreviewed directory with metadata."""
    jobs = []
    if not os.path.isdir(UNREVIEWED_DIR):
        return jsonify({"jobs": jobs})

    for root, dirs, files in os.walk(UNREVIEWED_DIR):
        if "metadata.json" not in files:
            continue
        meta_path = os.path.join(root, "metadata.json")
        rel_path = os.path.relpath(root, UNREVIEWED_DIR)
        try:
            with open(meta_path, "r") as f:
                data = json.load(f)
            data["item_path"] = rel_path
            # List media files in the directory
            media_files = []
            for fname in sorted(files):
                if fname == "metadata.json":
                    continue
                fpath = os.path.join(root, fname)
                try:
                    size = os.path.getsize(fpath)
                except OSError:
                    size = 0
                media_files.append({"name": fname, "size": size})
            data["media_files"] = media_files
            # Total size
            total_size = sum(f["size"] for f in media_files)
            data["total_size"] = total_size
            # Check for cover art
            has_art = False
            for art_name in ("cover.jpg", "cover.png", "folder.jpg"):
                if os.path.isfile(os.path.join(root, art_name)):
                    has_art = art_name
                    break
            data["has_art"] = has_art
            jobs.append(data)
        except (json.JSONDecodeError, OSError):
            jobs.append({"item_path": rel_path, "error": "unreadable"})

    return jsonify({"jobs": jobs})


@app.route("/review/edit", methods=["POST"])
def review_edit():
    """Edit metadata fields of an unreviewed item.

    Accepts JSON: { "item_path": "Video/Movies/...", "fields": { "disc_title": "...", ... } }
    Only allows editing known metadata fields in metadata.json.
    """
    if not request.is_json:
        return jsonify({"error": "JSON body required"}), 400
    item_path = request.json.get("item_path", "")
    fields = request.json.get("fields", {})
    if not item_path:
        return jsonify({"error": "Missing item_path"}), 400
    if not fields:
        return jsonify({"error": "No fields to update"}), 400

    # Prevent path traversal
    safe = os.path.normpath(item_path)
    if safe.startswith("..") or safe.startswith("/"):
        return jsonify({"error": "Invalid item_path"}), 400

    # Only allow editing safe metadata fields
    EDITABLE_FIELDS = {"artist", "album", "tracks", "disc_title", "source_type"}
    unknown = set(fields.keys()) - EDITABLE_FIELDS
    if unknown:
        return jsonify({"error": f"Cannot edit fields: {', '.join(unknown)}"}), 400

    meta_file = os.path.join(UNREVIEWED_DIR, safe, "metadata.json")
    if not os.path.isfile(meta_file):
        return jsonify({"error": f"No unreviewed item at: {item_path}"}), 404

    try:
        with open(meta_file, "r") as fh:
            data = json.load(fh)
    except (json.JSONDecodeError, OSError) as exc:
        return jsonify({"error": f"Failed to read metadata: {exc}"}), 500

    # Snapshot original values on first edit
    for key in fields:
        orig_key = f"_original_{key}"
        if orig_key not in data and key in data:
            data[orig_key] = data[key]

    # Apply edits
    for key, value in fields.items():
        data[key] = value

    # Write back atomically
    tmp_path = meta_file + ".tmp"
    try:
        with open(tmp_path, "w") as fh:
            json.dump(data, fh, indent=2)
        os.replace(tmp_path, meta_file)
    except OSError as exc:
        return jsonify({"error": f"Failed to write: {exc}"}), 500

    return jsonify({"ok": True, "message": f"Updated {', '.join(fields.keys())}",
                    "job": data})


@app.route("/review/rename", methods=["POST"])
def review_rename():
    """Rename a media file within an unreviewed item.

    Accepts JSON: { "item_path": "Video/TV/...", "old_name": "title00.mkv", "new_name": "S01E01.mkv" }
    Only renames within the same directory. Preserves file extension.
    """
    if not request.is_json:
        return jsonify({"error": "JSON body required"}), 400
    item_path = request.json.get("item_path", "").strip()
    old_name = request.json.get("old_name", "").strip()
    new_name = request.json.get("new_name", "").strip()

    if not item_path or not old_name or not new_name:
        return jsonify({"error": "Missing item_path, old_name, or new_name"}), 400

    # Prevent path traversal
    safe = os.path.normpath(item_path)
    if safe.startswith("..") or safe.startswith("/"):
        return jsonify({"error": "Invalid item_path"}), 400
    if "/" in old_name or "/" in new_name or ".." in new_name:
        return jsonify({"error": "Invalid filename"}), 400
    if new_name == "metadata.json":
        return jsonify({"error": "Cannot use reserved name"}), 400

    item_dir = os.path.join(UNREVIEWED_DIR, safe)
    old_path = os.path.join(item_dir, old_name)
    new_path = os.path.join(item_dir, new_name)

    if not os.path.isfile(old_path):
        return jsonify({"error": f"File not found: {old_name}"}), 404
    if os.path.exists(new_path):
        return jsonify({"error": f"File already exists: {new_name}"}), 409

    try:
        os.rename(old_path, new_path)
    except OSError as exc:
        return jsonify({"error": f"Rename failed: {exc}"}), 500

    return jsonify({"ok": True, "message": f"Renamed {old_name} → {new_name}"})


@app.route("/review/stream")
def review_stream():
    """Stream a media file from an unreviewed item for playback.

    Query params: ?item_path=Video/TV/...&file=episode.mkv
    Supports HTTP Range requests for seeking.
    """
    item_path = request.args.get("item_path", "").strip()
    filename = request.args.get("file", "").strip()
    if not item_path or not filename:
        return jsonify({"error": "Missing item_path or file param"}), 400

    safe = os.path.normpath(item_path)
    if safe.startswith("..") or safe.startswith("/"):
        abort(400)
    if "/" in filename or ".." in filename:
        abort(400)

    fpath = os.path.join(UNREVIEWED_DIR, safe, filename)
    if not os.path.isfile(fpath):
        abort(404)

    # Determine mimetype
    ext = os.path.splitext(filename)[1].lower()
    mimetypes = {
        ".mkv": "video/x-matroska", ".mp4": "video/mp4",
        ".avi": "video/x-msvideo", ".m4v": "video/mp4",
        ".flac": "audio/flac", ".mp3": "audio/mpeg",
        ".ogg": "audio/ogg", ".wav": "audio/wav",
        ".m4a": "audio/mp4", ".opus": "audio/opus",
    }
    mimetype = mimetypes.get(ext, "application/octet-stream")

    return send_file(fpath, mimetype=mimetype, conditional=True)


@app.route("/review/art")
def review_art():
    """Serve cover art for an unreviewed item.

    Query params: ?item_path=Audio/Music/Artist/Album
    """
    item_path = request.args.get("item_path", "").strip()
    if not item_path:
        abort(400)
    safe = os.path.normpath(item_path)
    if safe.startswith("..") or safe.startswith("/"):
        abort(400)
    item_dir = os.path.join(UNREVIEWED_DIR, safe)
    for art_name in ("cover.jpg", "cover.png", "folder.jpg"):
        art_path = os.path.join(item_dir, art_name)
        if os.path.isfile(art_path):
            return send_file(art_path, conditional=True)
    abort(404)


@app.route("/review/upload-art", methods=["POST"])
def review_upload_art():
    """Upload or replace cover art for an unreviewed item.

    Accepts multipart form: item_path (string) + file (image).
    Also accepts JSON: { "item_path": "...", "url": "https://..." } to fetch from URL.
    """
    if request.content_type and "multipart" in request.content_type:
        item_path = request.form.get("item_path", "").strip()
        if not item_path:
            return jsonify({"error": "Missing item_path"}), 400
        safe = os.path.normpath(item_path)
        if safe.startswith("..") or safe.startswith("/"):
            return jsonify({"error": "Invalid item_path"}), 400
        item_dir = os.path.join(UNREVIEWED_DIR, safe)
        if not os.path.isdir(item_dir):
            return jsonify({"error": f"No unreviewed item at: {item_path}"}), 404
        f = request.files.get("file")
        if not f:
            return jsonify({"error": "No file uploaded"}), 400
        ext = os.path.splitext(f.filename)[1].lower() if f.filename else ".jpg"
        if ext not in (".jpg", ".jpeg", ".png"):
            return jsonify({"error": "Only JPG/PNG allowed"}), 400
        dest = os.path.join(item_dir, "cover" + (".jpg" if ext in (".jpg", ".jpeg") else ".png"))
        f.save(dest)
        return jsonify({"ok": True, "message": "Cover art saved"})

    # JSON mode: fetch from URL
    if not request.is_json:
        return jsonify({"error": "Multipart form or JSON body required"}), 400
    item_path = request.json.get("item_path", "").strip()
    art_url = request.json.get("url", "").strip()
    if not item_path or not art_url:
        return jsonify({"error": "Missing item_path or url"}), 400
    safe = os.path.normpath(item_path)
    if safe.startswith("..") or safe.startswith("/"):
        return jsonify({"error": "Invalid item_path"}), 400
    item_dir = os.path.join(UNREVIEWED_DIR, safe)
    if not os.path.isdir(item_dir):
        return jsonify({"error": f"No unreviewed item at: {item_path}"}), 404

    import urllib.request
    import urllib.error
    req = urllib.request.Request(art_url)
    req.add_header("User-Agent", "autorip/1.0")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            ct = resp.headers.get("Content-Type", "")
            if "png" in ct:
                dest = os.path.join(item_dir, "cover.png")
            else:
                dest = os.path.join(item_dir, "cover.jpg")
            with open(dest, "wb") as out:
                out.write(resp.read())
    except (urllib.error.URLError, OSError) as e:
        return jsonify({"error": f"Failed to fetch art: {e}"}), 502

    return jsonify({"ok": True, "message": "Cover art downloaded"})


@app.route("/review/art-search")
def review_art_search():
    """Search multiple sources for cover art options.

    Query params: ?artist=...&album=...
    Returns JSON array of { source, label, thumbnail, full_url }.
    """
    import urllib.request as ureq
    import urllib.parse
    import urllib.error

    artist = request.args.get("artist", "").strip()
    album = request.args.get("album", "").strip()
    if not artist and not album:
        return jsonify({"error": "Need artist and/or album"}), 400

    ua = "autorip/1.0 (https://github.com/boylermb/autorip)"
    results = []

    def _get_json(url):
        r = ureq.Request(url, headers={"User-Agent": ua})
        with ureq.urlopen(r, timeout=10) as resp:
            return json.loads(resp.read())

    # --- MusicBrainz CAA (release) ---
    try:
        query = urllib.parse.quote(f'artist:"{artist}" AND release:"{album}"')
        data = _get_json(f"https://musicbrainz.org/ws/2/release/?query={query}&fmt=json&limit=5")
        for rel in data.get("releases", []):
            mbid = rel["id"]
            label = rel.get("title", album)
            ar = (rel.get("artist-credit", [{}])[0].get("name", "") if rel.get("artist-credit") else "")
            results.append({
                "source": "MusicBrainz CAA",
                "label": f"{ar} – {label}" if ar else label,
                "thumbnail": f"https://coverartarchive.org/release/{mbid}/front-250",
                "full_url": f"https://coverartarchive.org/release/{mbid}/front",
            })
    except Exception:
        pass

    import time
    time.sleep(0.5)

    # --- MusicBrainz CAA (release-group) ---
    try:
        query = urllib.parse.quote(f'artist:"{artist}" AND releasegroup:"{album}"')
        data = _get_json(f"https://musicbrainz.org/ws/2/release-group/?query={query}&fmt=json&limit=3")
        for rg in data.get("release-groups", []):
            rgid = rg["id"]
            label = rg.get("title", album)
            ar = (rg.get("artist-credit", [{}])[0].get("name", "") if rg.get("artist-credit") else "")
            results.append({
                "source": "MusicBrainz CAA (group)",
                "label": f"{ar} – {label}" if ar else label,
                "thumbnail": f"https://coverartarchive.org/release-group/{rgid}/front-250",
                "full_url": f"https://coverartarchive.org/release-group/{rgid}/front",
            })
    except Exception:
        pass

    time.sleep(0.5)

    # --- iTunes ---
    try:
        term = urllib.parse.quote(f"{artist} {album}")
        data = _get_json(f"https://itunes.apple.com/search?term={term}&media=music&entity=album&limit=5")
        for r in data.get("results", []):
            art100 = r.get("artworkUrl100", "")
            if art100:
                results.append({
                    "source": "iTunes",
                    "label": f"{r.get('artistName', '')} – {r.get('collectionName', '')}",
                    "thumbnail": art100.replace("100x100bb", "250x250bb"),
                    "full_url": art100.replace("100x100bb", "600x600bb"),
                })
    except Exception:
        pass

    time.sleep(0.5)

    # --- Deezer ---
    try:
        term = urllib.parse.quote(f"{artist} {album}")
        data = _get_json(f"https://api.deezer.com/search/album?q={term}&limit=5")
        for r in data.get("data", []):
            thumb = r.get("cover_medium", "") or r.get("cover_small", "")
            full = r.get("cover_big", "") or r.get("cover_xl", "") or thumb
            if thumb:
                results.append({
                    "source": "Deezer",
                    "label": f"{r.get('artist', {}).get('name', '')} – {r.get('title', '')}",
                    "thumbnail": thumb,
                    "full_url": full,
                })
    except Exception:
        pass

    return jsonify({"results": results})


@app.route("/review/lookup", methods=["POST"])
def review_lookup():
    """Look up metadata from a MusicBrainz release URL and update an unreviewed item.

    Accepts JSON: { "item_path": "Audio/Music/...", "musicbrainz_url": "https://musicbrainz.org/release/..." }
    """
    if not request.is_json:
        return jsonify({"error": "JSON body required"}), 400
    item_path = request.json.get("item_path", "").strip()
    mb_url = request.json.get("musicbrainz_url", "").strip()
    if not item_path:
        return jsonify({"error": "Missing item_path"}), 400
    if not mb_url:
        return jsonify({"error": "Missing musicbrainz_url"}), 400

    # Prevent path traversal
    safe = os.path.normpath(item_path)
    if safe.startswith("..") or safe.startswith("/"):
        return jsonify({"error": "Invalid item_path"}), 400

    meta_file = os.path.join(UNREVIEWED_DIR, safe, "metadata.json")
    if not os.path.isfile(meta_file):
        return jsonify({"error": f"No unreviewed item at: {item_path}"}), 404

    # Extract release MBID from the URL
    # Accepts: https://musicbrainz.org/release/<uuid>
    #          https://musicbrainz.org/release/<uuid>#...
    #          bare UUID
    mb_release_re = re.compile(
        r"(?:https?://(?:www\.)?musicbrainz\.org/release/)?"
        r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
        re.IGNORECASE,
    )
    m = mb_release_re.search(mb_url)
    if not m:
        return jsonify({"error": "Could not parse MusicBrainz release ID from URL"}), 400
    release_id = m.group(1).lower()

    # Fetch release metadata from the MusicBrainz JSON API
    import urllib.request
    import urllib.error
    api_url = (
        f"https://musicbrainz.org/ws/2/release/{release_id}"
        "?inc=artists+recordings+discids&fmt=json"
    )
    api_req = urllib.request.Request(api_url)
    api_req.add_header("User-Agent", "autorip/1.0 (https://github.com/boylermb/autorip)")
    try:
        with urllib.request.urlopen(api_req, timeout=15) as resp:
            mb_data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return jsonify({"error": f"MusicBrainz API returned HTTP {e.code}"}), 502
    except (urllib.error.URLError, OSError) as e:
        return jsonify({"error": f"MusicBrainz API unreachable: {e}"}), 502

    artist = mb_data.get("artist-credit-phrase") or ""
    if not artist:
        credits = mb_data.get("artist-credit", [])
        if credits:
            artist = "".join(
                c.get("name", c.get("artist", {}).get("name", ""))
                + c.get("joinphrase", "")
                for c in credits
            ).strip()
    album = mb_data.get("title") or ""
    if not artist or not album:
        return jsonify({"error": "MusicBrainz release missing artist or title"}), 502

    # Extract track names from the first medium (or the medium matching disc_id)
    media = mb_data.get("media", [])
    tracks = []
    disc_total = len(media)
    disc_number = 1

    # Read existing metadata
    try:
        with open(meta_file, "r") as fh:
            job_data = json.load(fh)
    except (json.JSONDecodeError, OSError) as exc:
        return jsonify({"error": f"Failed to read metadata: {exc}"}), 500

    job_disc_id = job_data.get("disc_id", "")
    job_track_count = len(job_data.get("tracks", []))
    matched_medium = None

    # Strategy 1: match by disc_id
    if job_disc_id:
        for medium in media:
            for disc in medium.get("discs", []):
                if disc.get("id") == job_disc_id:
                    matched_medium = medium
                    disc_number = int(medium.get("position", 1))
                    break
            if matched_medium:
                break

    # Strategy 2: match by track count
    if not matched_medium and disc_total > 1 and job_track_count > 0:
        for medium in media:
            if len(medium.get("tracks", [])) == job_track_count:
                matched_medium = medium
                disc_number = int(medium.get("position", 1))
                break

    # Strategy 3: fall back to first medium
    if not matched_medium and media:
        matched_medium = media[0]
        disc_number = int(matched_medium.get("position", 1))

    if matched_medium:
        for track in matched_medium.get("tracks", []):
            rec = track.get("recording", {})
            tracks.append(rec.get("title", f"Track {track.get('number', '?')}"))

    if disc_total > 1:
        album = f"{album} (Disc {disc_number})"

    # Snapshot originals and apply the new metadata
    fields = {"artist": artist, "album": album, "tracks": tracks}
    for key, value in fields.items():
        orig_key = f"_original_{key}"
        if orig_key not in job_data and key in job_data:
            job_data[orig_key] = job_data[key]
        job_data[key] = value

    # Rename track files to match MusicBrainz track names
    item_dir = os.path.join(UNREVIEWED_DIR, safe)
    renamed_files = []
    media_ext = job_data.get("format", "mp3")
    existing_files = sorted(
        f for f in os.listdir(item_dir)
        if f != "metadata.json" and not f.endswith(".tmp")
    )
    for i, track_name in enumerate(tracks):
        if i >= len(existing_files):
            break
        old_name = existing_files[i]
        # Sanitise track name for filesystem
        safe_track = re.sub(r'[<>:"/\\|?*]', '_', track_name)
        track_num = f"{i + 1:02d}"
        ext = os.path.splitext(old_name)[1]  # preserve actual extension
        new_name = f"{track_num} - {safe_track}{ext}"
        if new_name != old_name:
            old_fp = os.path.join(item_dir, old_name)
            new_fp = os.path.join(item_dir, new_name)
            if os.path.isfile(old_fp) and not os.path.exists(new_fp):
                os.rename(old_fp, new_fp)
                renamed_files.append({"old": old_name, "new": new_name})

    # Move directory to Artist/Album structure
    new_rel = os.path.join("Audio", "Music", re.sub(r'[<>:"/\\|?*]', '_', artist),
                           re.sub(r'[<>:"/\\|?*]', '_', album))
    new_item_dir = os.path.join(UNREVIEWED_DIR, new_rel)
    new_item_path = safe  # default: unchanged
    if new_item_dir != item_dir:
        if not os.path.exists(new_item_dir):
            os.makedirs(os.path.dirname(new_item_dir), exist_ok=True)
            os.rename(item_dir, new_item_dir)
            new_item_path = new_rel
            item_dir = new_item_dir
            meta_file = os.path.join(item_dir, "metadata.json")
            job_data["_unreviewed_dir"] = item_dir
            # Clean up empty parent dirs left behind
            old_parent = os.path.join(UNREVIEWED_DIR, safe)
            for _ in range(3):
                old_parent = os.path.dirname(old_parent)
                if old_parent == UNREVIEWED_DIR:
                    break
                try:
                    os.rmdir(old_parent)  # only removes if empty
                except OSError:
                    break

    job_data["item_path"] = new_item_path

    # Fetch cover art from Cover Art Archive if missing
    art_fetched = False
    has_art = any(
        os.path.isfile(os.path.join(item_dir, n))
        for n in ("cover.jpg", "cover.png", "folder.jpg")
    )
    if not has_art:
        caa_url = f"https://coverartarchive.org/release/{release_id}/front-250"
        caa_req = urllib.request.Request(caa_url)
        caa_req.add_header("User-Agent", "autorip/1.0")
        try:
            with urllib.request.urlopen(caa_req, timeout=15) as caa_resp:
                with open(os.path.join(item_dir, "cover.jpg"), "wb") as out:
                    out.write(caa_resp.read())
                art_fetched = True
        except (urllib.error.URLError, OSError):
            pass  # No art available — user can upload manually

    # Write back atomically
    tmp_path = meta_file + ".tmp"
    try:
        with open(tmp_path, "w") as fh:
            json.dump(job_data, fh, indent=2)
        os.replace(tmp_path, meta_file)
    except OSError as exc:
        return jsonify({"error": f"Failed to write: {exc}"}), 500

    return jsonify({
        "ok": True,
        "message": f"Identified: {artist} — {album} ({len(tracks)} tracks, {len(renamed_files)} files renamed{', art fetched' if art_fetched else ''})",
        "artist": artist,
        "album": album,
        "tracks": tracks,
        "item_path": new_item_path,
        "renamed_files": renamed_files,
        "job": job_data,
    })


@app.route("/review/tmdb-lookup", methods=["POST"])
def review_tmdb_lookup():
    """Look up TV episode names from TMDb and rename files in an unreviewed item.

    Accepts JSON: {
        "item_path": "Video/TV/Show/Season 08",
        "tmdb_url": "https://www.themoviedb.org/tv/484-murder-she-wrote/season/8"
    }
    Parses the TMDb show ID and season from the URL, fetches episode names,
    and renames SxxExx files to include the episode title.
    """
    if not request.is_json:
        return jsonify({"error": "JSON body required"}), 400
    item_path = request.json.get("item_path", "").strip()
    tmdb_url = request.json.get("tmdb_url", "").strip()
    if not item_path:
        return jsonify({"error": "Missing item_path"}), 400
    if not tmdb_url:
        return jsonify({"error": "Missing tmdb_url"}), 400

    safe = os.path.normpath(item_path)
    if safe.startswith("..") or safe.startswith("/"):
        return jsonify({"error": "Invalid item_path"}), 400

    item_dir = os.path.join(UNREVIEWED_DIR, safe)
    if not os.path.isdir(item_dir):
        # Also check the approved library
        item_dir_lib = os.path.join(OUTPUT_BASE, safe)
        if os.path.isdir(item_dir_lib):
            item_dir = item_dir_lib
        else:
            return jsonify({"error": f"No directory at: {item_path}"}), 404

    # Parse TMDb URL: /tv/{id}/season/{num} or /tv/{id}-slug/season/{num}
    import urllib.request
    import urllib.error
    tmdb_re = re.compile(
        r"themoviedb\.org/tv/(\d+)(?:-[^/]*)?"
        r"(?:/season/(\d+))?",
        re.IGNORECASE,
    )
    m = tmdb_re.search(tmdb_url)
    if not m:
        return jsonify({"error": "Could not parse TMDb show ID from URL"}), 400
    show_id = m.group(1)
    season_num = m.group(2)

    # If no season in URL, try to infer from item_path
    if not season_num:
        season_re = re.compile(r"[Ss]eason\s*(\d+)", re.IGNORECASE)
        sm = season_re.search(item_path)
        if sm:
            season_num = sm.group(1)
        else:
            return jsonify({"error": "Could not determine season number"}), 400

    # Use configured API key, fall back to mnamer's built-in key
    api_key = _config.get("TMDB_API_KEY") or os.environ.get("TMDB_API_KEY") or "db972a607f2760bb19ff8bb34074b4c7"

    # Fetch season data
    api_url = f"https://api.themoviedb.org/3/tv/{show_id}/season/{season_num}?api_key={api_key}"
    api_req = urllib.request.Request(api_url)
    api_req.add_header("User-Agent", "autorip/1.0")
    try:
        with urllib.request.urlopen(api_req, timeout=15) as resp:
            season_data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return jsonify({"error": f"TMDb API returned HTTP {e.code}"}), 502
    except (urllib.error.URLError, OSError) as e:
        return jsonify({"error": f"TMDb API unreachable: {e}"}), 502

    # Build episode name lookup
    ep_names = {}
    for ep in season_data.get("episodes", []):
        ep_names[ep["episode_number"]] = ep["name"]

    if not ep_names:
        return jsonify({"error": "No episodes found in TMDb season data"}), 404

    # Rename files: match SxxExx pattern and append episode title
    renamed_files = []
    ep_pattern = re.compile(r'^(.*S\d{2}E(\d{2}))((?:\s*-\s*.+)?)(\.mkv)$', re.IGNORECASE)
    for fname in sorted(os.listdir(item_dir)):
        fm = ep_pattern.match(fname)
        if not fm:
            continue
        ep_num = int(fm.group(2))
        ep_title = ep_names.get(ep_num)
        if not ep_title:
            continue
        # Sanitize title for filesystem
        safe_title = re.sub(r'[<>:"/\\|?*]', '_', ep_title).strip()
        new_name = f"{fm.group(1)} - {safe_title}{fm.group(4)}"
        if new_name != fname:
            old_fp = os.path.join(item_dir, fname)
            new_fp = os.path.join(item_dir, new_name)
            if os.path.isfile(old_fp) and not os.path.exists(new_fp):
                os.rename(old_fp, new_fp)
                renamed_files.append({"old": fname, "new": new_name})

    # Update metadata.json if present
    meta_file = os.path.join(item_dir, "metadata.json")
    if os.path.isfile(meta_file):
        try:
            with open(meta_file, "r") as fh:
                job_data = json.load(fh)
            job_data["tmdb_id"] = show_id
            job_data["tmdb_season"] = int(season_num)
            # Update tracks list with episode names
            tracks = []
            for fname in sorted(os.listdir(item_dir)):
                if fname.endswith(".mkv"):
                    tracks.append(os.path.splitext(fname)[0])
            job_data["tracks"] = tracks
            tmp = meta_file + ".tmp"
            with open(tmp, "w") as fh:
                json.dump(job_data, fh, indent=2)
            os.replace(tmp, meta_file)
        except (json.JSONDecodeError, OSError):
            pass

    # Also fetch show poster as cover art if missing
    art_fetched = False
    has_art = any(
        os.path.isfile(os.path.join(item_dir, n))
        for n in ("cover.jpg", "cover.png", "folder.jpg")
    )
    if not has_art:
        poster_path = season_data.get("poster_path")
        if poster_path:
            poster_url = f"https://image.tmdb.org/t/p/w300{poster_path}"
            poster_req = urllib.request.Request(poster_url)
            poster_req.add_header("User-Agent", "autorip/1.0")
            try:
                with urllib.request.urlopen(poster_req, timeout=15) as poster_resp:
                    with open(os.path.join(item_dir, "cover.jpg"), "wb") as out:
                        out.write(poster_resp.read())
                    art_fetched = True
            except (urllib.error.URLError, OSError):
                pass

    return jsonify({
        "ok": True,
        "message": f"TMDb: renamed {len(renamed_files)} file(s){', poster fetched' if art_fetched else ''}",
        "renamed_files": renamed_files,
        "episode_names": ep_names,
    })


@app.route("/review/approve", methods=["POST"])
def review_approve():
    """Approve a single unreviewed item — moves to library."""
    item_path = request.json.get("item_path", "") if request.is_json else ""
    if not item_path:
        return jsonify({"error": "Missing item_path"}), 400
    safe = os.path.normpath(item_path)
    if safe.startswith("..") or safe.startswith("/"):
        return jsonify({"error": "Invalid item_path"}), 400
    result = subprocess.run(
        [WORKER_SCRIPT, "approve", safe],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode == 0:
        return jsonify({"ok": True, "message": f"Approved {item_path}"})
    return jsonify({"error": result.stderr.strip() or "approve failed"}), 500


@app.route("/review/reject", methods=["POST"])
def review_reject():
    """Reject a single unreviewed item — deletes from unreviewed dir."""
    item_path = request.json.get("item_path", "") if request.is_json else ""
    if not item_path:
        return jsonify({"error": "Missing item_path"}), 400
    safe = os.path.normpath(item_path)
    if safe.startswith("..") or safe.startswith("/"):
        return jsonify({"error": "Invalid item_path"}), 400
    result = subprocess.run(
        [WORKER_SCRIPT, "reject", safe],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode == 0:
        return jsonify({"ok": True, "message": f"Rejected {item_path}"})
    return jsonify({"error": result.stderr.strip() or "reject failed"}), 500


@app.route("/health")
def health():
    return jsonify({"status": "ok", "hostname": HOSTNAME})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=AGENT_PORT, debug=False)
