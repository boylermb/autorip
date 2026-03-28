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
            jobs.append(data)
        except (json.JSONDecodeError, OSError):
            jobs.append({"job_file": entry, "state": state, "error": "unreadable"})

    return {"jobs": jobs, "worker": worker_status}


def read_status():
    """Read the local autorip status JSON."""
    status_file = os.path.join(STATUS_DIR, "status.json")
    try:
        with open(status_file, "r") as f:
            data = json.load(f)
        data["online"] = True
        data["hostname"] = HOSTNAME
        art_path = os.path.join(ART_DIR, "cover.jpg")
        data["has_art"] = os.path.exists(art_path)
        return data
    except (FileNotFoundError, json.JSONDecodeError):
        return {
            "hostname": HOSTNAME,
            "online": True,
            "status": "idle",
            "device": "",
            "disc_type": "",
            "title": "",
            "progress": "",
            "artist": "",
            "album": "",
            "tracks": [],
            "has_art": False,
            "updated": "",
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
    data = read_status()
    data["drives"] = get_optical_drives()
    # Attach live progress parsed from the log file
    device = data.get("device") or "sr0"
    data["progress_info"] = parse_progress(device)
    return jsonify(data)


@app.route("/log")
def log_endpoint():
    device = request.args.get("device", "sr0")
    # Sanitize device name
    if not DEVICE_RE.match(device):
        return jsonify({"error": "Invalid device"}), 400
    return jsonify({"hostname": HOSTNAME, "log": read_log(device)})


@app.route("/art")
def art():
    art_path = os.path.join(ART_DIR, "cover.jpg")
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
        return jsonify({"ok": True, "message": f"Ejected /dev/{device}"})
    else:
        return jsonify(
            {"error": f"Failed to eject: {result.stderr.strip()}"}
        ), 500


@app.route("/transcode-queue")
def transcode_queue():
    """Return the current state of the GPU transcode queue."""
    return jsonify(read_transcode_queue())


@app.route("/health")
def health():
    return jsonify({"status": "ok", "hostname": HOSTNAME})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=AGENT_PORT, debug=False)
