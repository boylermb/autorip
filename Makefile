# =============================================================================
# autorip — Automatic disc ripping for Linux
# https://github.com/boylermb/autorip
# =============================================================================
# Targets:
#   make install          — Install everything (scripts, services, configs)
#   make install-worker   — Install only the GPU transcode worker (master node)
#   make install-makemkv  — Build and install MakeMKV from source
#   make install-deps     — Install apt dependencies
#   make configure        — Expand config into installed files (re-run after editing autorip.conf)
#   make uninstall        — Remove all installed files
# =============================================================================

PREFIX       ?= /usr/local
SYSCONFDIR   ?= /etc
SYSTEMDDIR   ?= /etc/systemd/system
CONF_FILE    ?= $(SYSCONFDIR)/autorip/autorip.conf

# MakeMKV version (override: make install-makemkv MAKEMKV_VERSION=1.18.4)
MAKEMKV_VERSION ?= 1.18.3

# ---------- Read config values (used during configure/install) ----------
# These are evaluated lazily so the config file is read at install time
OUTPUT_BASE      = $(shell grep '^OUTPUT_BASE'      "$(CONF_FILE)" 2>/dev/null | head -1 | cut -d= -f2- | tr -d ' "')
MIN_TITLE_SECONDS = $(shell grep '^MIN_TITLE_SECONDS' "$(CONF_FILE)" 2>/dev/null | head -1 | cut -d= -f2- | tr -d ' "')
CD_FORMAT        = $(shell grep '^CD_FORMAT'        "$(CONF_FILE)" 2>/dev/null | head -1 | cut -d= -f2- | tr -d ' "')
MAX_ENCODE_PROCS = $(shell grep '^MAX_ENCODE_PROCS' "$(CONF_FILE)" 2>/dev/null | head -1 | cut -d= -f2- | tr -d ' "')
AGENT_PORT       = $(shell grep '^AGENT_PORT'       "$(CONF_FILE)" 2>/dev/null | head -1 | cut -d= -f2- | tr -d ' "')
WRITABLE_PATHS   = $(shell grep '^WRITABLE_PATHS'   "$(CONF_FILE)" 2>/dev/null | head -1 | cut -d= -f2- | tr -d '"')

.PHONY: install install-worker install-makemkv install-deps configure uninstall help

help:
	@echo "autorip — Automatic disc ripping for Linux"
	@echo ""
	@echo "Usage:"
	@echo "  make install-deps          Install system dependencies (apt)"
	@echo "  make install-makemkv       Build & install MakeMKV from source"
	@echo "  make install               Install autorip (scripts, services, configs)"
	@echo "  make install-worker        Install GPU transcode worker (master node only)"
	@echo "  make configure             Re-apply config to installed files"
	@echo "  make uninstall             Remove all installed autorip files"

# ==========================================================================
# install-deps — System packages (Debian only)
# ==========================================================================
install-deps:
	@echo "==> Installing system dependencies..."
	apt-get update
	apt-get install -y \
		build-essential pkg-config libc6-dev libssl-dev libexpat1-dev \
		libavcodec-dev libgl1-mesa-dev zlib1g-dev wget \
		libdvdcss2 abcde flac lame cdparanoia id3v2 eject at ffmpeg \
		glyrc cd-discid python3 python3-pip python3-venv
	pip3 install --break-system-packages eyed3 mnamer

# ==========================================================================
# install-makemkv — Build from source
# ==========================================================================
install-makemkv:
	@echo "==> Building MakeMKV $(MAKEMKV_VERSION) from source..."
	@if command -v makemkvcon >/dev/null 2>&1; then \
		echo "MakeMKV is already installed:"; \
		makemkvcon --version 2>&1 | head -1 || true; \
		echo "To reinstall, run: make uninstall-makemkv install-makemkv"; \
		exit 0; \
	fi
	mkdir -p /tmp/makemkv-build
	wget -qO /tmp/makemkv-build/makemkv-oss-$(MAKEMKV_VERSION).tar.gz \
		"https://www.makemkv.com/download/makemkv-oss-$(MAKEMKV_VERSION).tar.gz"
	wget -qO /tmp/makemkv-build/makemkv-bin-$(MAKEMKV_VERSION).tar.gz \
		"https://www.makemkv.com/download/makemkv-bin-$(MAKEMKV_VERSION).tar.gz"
	cd /tmp/makemkv-build && tar xzf makemkv-oss-$(MAKEMKV_VERSION).tar.gz
	cd /tmp/makemkv-build && tar xzf makemkv-bin-$(MAKEMKV_VERSION).tar.gz
	cd /tmp/makemkv-build/makemkv-oss-$(MAKEMKV_VERSION) && \
		./configure --disable-gui && make -j$$(nproc) && make install
	cd /tmp/makemkv-build/makemkv-bin-$(MAKEMKV_VERSION) && \
		mkdir -p tmp && echo "accepted" > tmp/eula_accepted && make install
	ldconfig
	rm -rf /tmp/makemkv-build
	@echo "==> MakeMKV $(MAKEMKV_VERSION) installed."

# ==========================================================================
# install — Full installation
# ==========================================================================
install: install-config install-scripts install-services install-agent
	@echo ""
	@echo "==> autorip installed.  Next steps:"
	@echo "    1. Edit $(CONF_FILE) for your environment"
	@echo "    2. Run 'make configure' to apply config"
	@echo "    3. Run 'systemctl daemon-reload'"
	@echo "    4. Insert a disc!"

install-config:
	@echo "==> Installing configuration..."
	mkdir -p $(SYSCONFDIR)/autorip
	@if [ ! -f "$(CONF_FILE)" ]; then \
		cp autorip.conf "$(CONF_FILE)"; \
		echo "    Created $(CONF_FILE) (edit this for your environment)"; \
	else \
		echo "    $(CONF_FILE) already exists — not overwriting"; \
	fi

install-scripts:
	@echo "==> Installing scripts..."
	install -m 0755 bin/autorip.sh $(PREFIX)/bin/autorip.sh
	@echo "    Installed $(PREFIX)/bin/autorip.sh"

install-services: configure-services
	@echo "==> Installing systemd units and udev rules..."
	install -m 0644 etc/99-autorip.rules $(SYSCONFDIR)/udev/rules.d/99-autorip.rules
	install -m 0644 build/autorip@.service $(SYSTEMDDIR)/autorip@.service
	@# Log & state directories
	mkdir -p /var/log/autorip /var/lib/autorip /var/lib/autorip/.MakeMKV
	@# Logrotate
	@echo '/var/log/autorip/*.log {\n  weekly\n  rotate 4\n  compress\n  missingok\n  notifempty\n}' > $(SYSCONFDIR)/logrotate.d/autorip
	@# abcde config
	install -m 0644 build/abcde.conf $(SYSCONFDIR)/abcde.conf
	@# MakeMKV settings
	install -m 0644 build/makemkv-settings.conf /var/lib/autorip/.MakeMKV/settings.conf
	@echo "    Installed udev rules, systemd service, abcde.conf, MakeMKV settings"

install-agent:
	@echo "==> Installing autorip-agent..."
	mkdir -p $(PREFIX)/lib/autorip-agent
	install -m 0644 bin/autorip-agent.py $(PREFIX)/lib/autorip-agent/app.py
	@# Create virtualenv if it doesn't exist
	@if [ ! -d "$(PREFIX)/bin/autorip-agent-venv" ]; then \
		python3 -m venv $(PREFIX)/bin/autorip-agent-venv; \
		$(PREFIX)/bin/autorip-agent-venv/bin/pip install flask gunicorn; \
	fi
	install -m 0644 build/autorip-agent.service $(SYSTEMDDIR)/autorip-agent.service
	@echo "    Installed autorip-agent"

# ==========================================================================
# install-worker — GPU transcode worker (master/GPU node only)
# ==========================================================================
install-worker: configure-services
	@echo "==> Installing GPU transcode worker..."
	install -m 0755 bin/transcode-worker.sh $(PREFIX)/bin/transcode-worker.sh
	install -m 0644 build/transcode-worker.service $(SYSTEMDDIR)/transcode-worker.service
	install -m 0644 systemd/transcode-worker.timer $(SYSTEMDDIR)/transcode-worker.timer
	systemctl daemon-reload
	systemctl enable --now transcode-worker.timer
	@echo "    Installed and enabled transcode-worker"

# ==========================================================================
# configure — Expand @@PLACEHOLDER@@ values in .in files using config
# ==========================================================================
configure: install-config configure-services
	@echo "==> Configuration applied."

configure-services:
	@echo "==> Expanding config into service files..."
	mkdir -p build
	@# systemd units
	sed -e 's|@@WRITABLE_PATHS@@|$(WRITABLE_PATHS)|g' \
		systemd/autorip@.service.in > build/autorip@.service
	sed -e 's|@@WRITABLE_PATHS@@|$(WRITABLE_PATHS)|g' \
		systemd/transcode-worker.service.in > build/transcode-worker.service
	sed -e 's|@@AGENT_PORT@@|$(AGENT_PORT)|g' \
		systemd/autorip-agent.service.in > build/autorip-agent.service
	@# abcde.conf
	sed -e 's|@@CD_FORMAT@@|$(CD_FORMAT)|g' \
	    -e 's|@@OUTPUT_BASE@@|$(OUTPUT_BASE)|g' \
	    -e 's|@@MAX_ENCODE_PROCS@@|$(MAX_ENCODE_PROCS)|g' \
		etc/abcde.conf.in > build/abcde.conf
	@# MakeMKV settings
	sed -e 's|@@MIN_TITLE_SECONDS@@|$(MIN_TITLE_SECONDS)|g' \
		etc/makemkv-settings.conf.in > build/makemkv-settings.conf

# ==========================================================================
# uninstall
# ==========================================================================
uninstall:
	@echo "==> Removing autorip..."
	-systemctl stop autorip-agent transcode-worker.timer 2>/dev/null || true
	-systemctl disable autorip-agent transcode-worker.timer 2>/dev/null || true
	rm -f $(PREFIX)/bin/autorip.sh
	rm -f $(PREFIX)/bin/transcode-worker.sh
	rm -f $(SYSTEMDDIR)/autorip@.service
	rm -f $(SYSTEMDDIR)/transcode-worker.service
	rm -f $(SYSTEMDDIR)/transcode-worker.timer
	rm -f $(SYSTEMDDIR)/autorip-agent.service
	rm -f $(SYSCONFDIR)/udev/rules.d/99-autorip.rules
	rm -f $(SYSCONFDIR)/abcde.conf
	rm -f $(SYSCONFDIR)/logrotate.d/autorip
	rm -rf $(PREFIX)/lib/autorip-agent
	rm -rf $(PREFIX)/bin/autorip-agent-venv
	systemctl daemon-reload
	udevadm control --reload-rules
	@echo "==> autorip removed (config in $(SYSCONFDIR)/autorip/ preserved)."

uninstall-makemkv:
	rm -f /usr/bin/makemkvcon
	rm -f /usr/lib/libmakemkv.so*
	rm -f /usr/lib/libdriveio.so*
	ldconfig

clean:
	rm -rf build/
