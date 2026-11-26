#!/usr/bin/env bash
# ==============================================================================
# Orange Pi RV2 (RISC-V) - All-in-One Server Setup Script
#
# Configures:
#   1. PostgreSQL 16 + pgvector (0.6.0+) on the largest NVMe/disk partition
#   2. Local-only authentication (DuckLake local socket peer + loopback SCRAM-SHA-256, NO network access)
#   3. Dual-Ethernet Bridge (br0) on 10.0.2.1/24 with end0 & end1
#   4. IP Forwarding & NAT masquerade (br0 -> wlan0 Internet uplink)
#   5. Firewall hardening (blocks port 5432 on all non-loopback interfaces)
#   6. Adminer Web UI on port 8080 with Dracula dark theme & auto-routing
#   7. System health cleanup (disables non-functional orangepi-zram-config)
# ==============================================================================

set -euo pipefail

# --- Color definitions ---
C_RESET="\033[0m"
C_BOLD="\033[1m"
C_GREEN="\033[32m"
C_BLUE="\033[34m"
C_YELLOW="\033[33m"
C_RED="\033[31m"
C_CYAN="\033[36m"

log_info()    { echo -e "${C_BLUE}${C_BOLD}[INFO]${C_RESET} $*"; }
log_success() { echo -e "${C_GREEN}${C_BOLD}[SUCCESS]${C_RESET} $*"; }
log_warn()    { echo -e "${C_YELLOW}${C_BOLD}[WARN]${C_RESET} $*"; }
log_error()   { echo -e "${C_RED}${C_BOLD}[ERROR]${C_RESET} $*"; }

# --- Ensure Root Privileges ---
if [ "$(id -u)" -ne 0 ]; then
    log_info "Re-running script with sudo privileges..."
    exec sudo bash "$0" "$@"
fi

# --- Default Configuration ---
DB_USER="${PG_USER:-orangepi}"
DB_NAME="${PG_DB:-orangepi}"
DB_PASSWORD="${PG_PASSWORD:-}"
BRIDGE_NAME="br0"
BRIDGE_IP="10.0.2.1/24"
BRIDGE_PORTS=("end0" "end1")
WAN_IFACE="${WAN_INTERFACE:-wlan0}"
ADMINER_PORT=8080

echo -e "${C_CYAN}${C_BOLD}"
echo "=========================================================="
echo "   Orange Pi RV2 PostgreSQL, Network & Adminer Installer  "
echo "=========================================================="
echo -e "${C_RESET}"

# ==============================================================================
# 1. Detect Largest Partition for Database Storage
# ==============================================================================
log_info "Detecting largest available disk partition..."

# Scan mounted partitions excluding pseudo filesystems and root / if another exists
LARGEST_MOUNT=$(df -B1 --output=target,avail -x tmpfs -x devtmpfs -x efivarfs | \
    awk 'NR>1 && $1 != "/dev" && $1 != "/run" {print $1, $2}' | \
    sort -k2 -nr | \
    awk 'NR==1 {print $1}')

AVAIL_HUMAN=$(df -h "$LARGEST_MOUNT" | awk 'NR==2 {print $4}')
TOTAL_HUMAN=$(df -h "$LARGEST_MOUNT" | awk 'NR==2 {print $2}')

log_success "Selected storage mount: ${C_BOLD}${LARGEST_MOUNT}${C_RESET} (${AVAIL_HUMAN} available of ${TOTAL_HUMAN})"
TARGET_DATA_DIR="${LARGEST_MOUNT}/postgresql/16/main"
log_info "Target PostgreSQL data directory: ${C_BOLD}${TARGET_DATA_DIR}${C_RESET}"

# ==============================================================================
# 2. System Tweaks & Package Installation
# ==============================================================================
log_info "Updating package lists..."
export DEBIAN_FRONTEND=noninteractive
apt update -qq

log_info "Disabling unsupported kernel zram service if present..."
if systemctl list-unit-files | grep -q "orangepi-zram-config.service"; then
    systemctl disable --now orangepi-zram-config.service 2>/dev/null || true
    systemctl reset-failed orangepi-zram-config.service 2>/dev/null || true
fi

log_info "Installing PostgreSQL 16, pgvector, PHP, Adminer, and networking tools..."
apt install -y -qq \
    postgresql-16 \
    postgresql-contrib \
    postgresql-16-pgvector \
    adminer \
    php-cli \
    php-pgsql \
    iptables \
    iptables-persistent \
    netfilter-persistent \
    rsync

log_success "Required packages installed successfully."

# ==============================================================================
# 3. PostgreSQL Storage Migration
# ==============================================================================
CURRENT_DATA_DIR=$(sudo -u postgres psql -t -P format=unaligned -c "SHOW data_directory;" 2>/dev/null || echo "")

if [ "$CURRENT_DATA_DIR" != "$TARGET_DATA_DIR" ]; then
    log_info "Migrating PostgreSQL data directory to ${TARGET_DATA_DIR}..."
    systemctl stop postgresql

    mkdir -p "$(dirname "$TARGET_DATA_DIR")"
    chown -R postgres:postgres "$(dirname "$TARGET_DATA_DIR")"
    chmod 750 "$(dirname "$TARGET_DATA_DIR")"

    if [ -d "$CURRENT_DATA_DIR" ] && [ "$CURRENT_DATA_DIR" != "$TARGET_DATA_DIR" ]; then
        log_info "Syncing existing cluster data from ${CURRENT_DATA_DIR}..."
        rsync -avq "${CURRENT_DATA_DIR}/" "${TARGET_DATA_DIR}/"
        mv "$CURRENT_DATA_DIR" "${CURRENT_DATA_DIR}.bak.$(date +%s)"
    else
        mkdir -p "$TARGET_DATA_DIR"
    fi

    chown -R postgres:postgres "$TARGET_DATA_DIR"
    chmod 700 "$TARGET_DATA_DIR"

    sed -i "s|^data_directory = .*|data_directory = '${TARGET_DATA_DIR}'|" /etc/postgresql/16/main/postgresql.conf
    systemctl start postgresql
    log_success "PostgreSQL storage moved to ${TARGET_DATA_DIR}."
else
    log_info "PostgreSQL is already using ${TARGET_DATA_DIR}."
fi

# ==============================================================================
# 4. PostgreSQL Configuration & Local-Only Access Rules (DuckLake Only)
# ==============================================================================
log_info "Configuring PostgreSQL for local-only access (no network access)..."

PG_CONF="/etc/postgresql/16/main/postgresql.conf"
PG_HBA="/etc/postgresql/16/main/pg_hba.conf"

# Bind ONLY to localhost loopback (disable all external/LAN network listening)
if grep -qE "^#*listen_addresses\s*=" "$PG_CONF"; then
    sed -i "s|^#*listen_addresses\s*=.*|listen_addresses = 'localhost'|" "$PG_CONF"
else
    echo "listen_addresses = 'localhost'" >> "$PG_CONF"
fi

# Backup original pg_hba.conf once and enforce strict local-only access
if [ ! -f "${PG_HBA}.orig" ]; then
    cp "$PG_HBA" "${PG_HBA}.orig"
fi

cat << EOF > "$PG_HBA"
# PostgreSQL Client Authentication (Local-Only / DuckLake Exclusive)
# Network access is disabled. Only local DuckLake Unix socket (${DB_USER} peer)
# and local loopback Adminer (127.0.0.1 / ::1 SCRAM-SHA-256) are allowed.
# TYPE  DATABASE        USER            ADDRESS                 METHOD
local   all             postgres                                peer
local   all             ${DB_USER}                              peer
host    all             ${DB_USER}      127.0.0.1/32            scram-sha-256
host    all             ${DB_USER}      ::1/128                 scram-sha-256
EOF
chmod 640 "$PG_HBA"
chown postgres:postgres "$PG_HBA"

# Restart PostgreSQL so listen_addresses change takes effect immediately
systemctl restart postgresql
log_success "PostgreSQL locked down to local-only access (listen_addresses = 'localhost', no network access)."

# ==============================================================================
# 5. Database, User & pgvector Setup
# ==============================================================================
log_info "Configuring database user '${DB_USER}' and database '${DB_NAME}'..."

if ! sudo -u postgres psql -t -P format=unaligned -c "SELECT 1 FROM pg_roles WHERE rolname='${DB_USER}';" | grep -q 1; then
    sudo -u postgres createuser -s "$DB_USER"
    log_success "Created PostgreSQL role '${DB_USER}'."
fi

if ! sudo -u postgres psql -t -P format=unaligned -c "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}';" | grep -q 1; then
    sudo -u postgres createdb -O "$DB_USER" "$DB_NAME"
    log_success "Created database '${DB_NAME}'."
fi

# Handle password setting if provided or requested
if [ -n "$DB_PASSWORD" ]; then
    sudo -u postgres psql -c "ALTER USER \"${DB_USER}\" WITH PASSWORD '${DB_PASSWORD}';" >/dev/null
    log_success "Set password for user '${DB_USER}'."
else
    HAS_PWD=$(sudo -u postgres psql -t -P format=unaligned -c "SELECT passwd IS NOT NULL FROM pg_shadow WHERE usename='${DB_USER}';" || echo "f")
    if [ "$HAS_PWD" != "t" ]; then
        log_warn "User '${DB_USER}' currently does NOT have a password set."
        log_warn "To set one later, run: psql -c \"ALTER USER ${DB_USER} WITH PASSWORD 'your_password';\""
    fi
fi

# Enable pgvector extension
log_info "Enabling vector extension in '${DB_NAME}'..."
sudo -u postgres psql -d "$DB_NAME" -c "CREATE EXTENSION IF NOT EXISTS vector;" >/dev/null
sudo -u postgres psql -d postgres -c "CREATE EXTENSION IF NOT EXISTS vector;" >/dev/null
log_success "pgvector extension verified active."

# ==============================================================================
# 6. Dual-Ethernet Bridge (br0) & Network Routing
# ==============================================================================
log_info "Configuring dual-Ethernet bridge (${BRIDGE_NAME}) on ${BRIDGE_IP}..."

if nmcli connection show "$BRIDGE_NAME" &>/dev/null; then
    log_info "NetworkManager connection '${BRIDGE_NAME}' already exists."
else
    nmcli connection add type bridge con-name "$BRIDGE_NAME" ifname "$BRIDGE_NAME" \
        ipv4.addresses "$BRIDGE_IP" ipv4.method manual ipv6.method ignore
    log_success "Created bridge connection '${BRIDGE_NAME}'."
fi

# Attach Ethernet ports
for port in "${BRIDGE_PORTS[@]}"; do
    if ip link show "$port" &>/dev/null; then
        PORT_CON="${BRIDGE_NAME}-${port}"
        if ! nmcli connection show "$PORT_CON" &>/dev/null; then
            nmcli connection add type ethernet con-name "$PORT_CON" ifname "$port" master "$BRIDGE_NAME"
            log_success "Attached port '${port}' to bridge '${BRIDGE_NAME}'."
        fi
    else
        log_warn "Interface '${port}' not found on this board. Skipping."
    fi
done

# Remove conflicting default wired profiles if present
nmcli connection delete "Wired connection 1" 2>/dev/null || true
nmcli connection delete "Wired connection 2" 2>/dev/null || true

nmcli connection up "$BRIDGE_NAME" 2>/dev/null || true

# ==============================================================================
# 7. IP Forwarding, NAT & Firewall Hardening
# ==============================================================================
log_info "Enabling persistent kernel IP forwarding..."
echo "net.ipv4.ip_forward = 1" > /etc/sysctl.d/99-ip-forward.conf
sysctl -p /etc/sysctl.d/99-ip-forward.conf >/dev/null

log_info "Configuring iptables NAT masquerade and firewall protection..."

# NAT masquerade out of WAN interface
if ! iptables -t nat -C POSTROUTING -o "$WAN_IFACE" -j MASQUERADE 2>/dev/null; then
    iptables -t nat -A POSTROUTING -o "$WAN_IFACE" -j MASQUERADE
fi

# Forward traffic from bridge out to WAN
if ! iptables -C FORWARD -i "$BRIDGE_NAME" -o "$WAN_IFACE" -j ACCEPT 2>/dev/null; then
    iptables -A FORWARD -i "$BRIDGE_NAME" -o "$WAN_IFACE" -j ACCEPT
fi

# Allow return traffic
if ! iptables -C FORWARD -i "$WAN_IFACE" -o "$BRIDGE_NAME" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT 2>/dev/null; then
    iptables -A FORWARD -i "$WAN_IFACE" -o "$BRIDGE_NAME" -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
fi

# Block PostgreSQL direct TCP connection attempts from ALL non-loopback network interfaces
iptables -D INPUT -i "$WAN_IFACE" -p tcp --dport 5432 -j DROP 2>/dev/null || true
if ! iptables -C INPUT ! -i lo -p tcp --dport 5432 -j DROP 2>/dev/null; then
    iptables -I INPUT ! -i lo -p tcp --dport 5432 -j DROP
fi

# Save iptables rules
netfilter-persistent save >/dev/null
log_success "Firewall & routing rules active and saved."

# ==============================================================================
# 8. Adminer Web UI Setup
# ==============================================================================
log_info "Configuring Adminer Web UI on port ${ADMINER_PORT}..."

mkdir -p /opt/adminer

cat << 'EOF' > /opt/adminer/index.php
<?php

// Auto pre-populate PostgreSQL, loopback IP, and default database
if (empty($_GET) && empty($_POST)) {
    header('Location: ./?pgsql=&server=127.0.0.1&username=orangepi&db=orangepi');
    exit;
}

define('ADMINER_DIR', '/usr/share/adminer');

function adminer_object() {
    include_once ADMINER_DIR . '/plugins/plugin.php';
    foreach (glob(ADMINER_DIR . '/plugins/*.php') as $filename) {
        include_once $filename;
    }

    $plugins = array(
        new AdminerVersionNoverify(),
    );

    class AdminerCustomization extends AdminerPlugin {
        function credentials() {
            list($server, $user, $password) = parent::credentials();
            // Translate empty or 'localhost' to 127.0.0.1 to use TCP SCRAM auth
            // avoiding UNIX domain socket peer authentication mismatch
            if (empty($server) || $server === 'localhost') {
                $server = '127.0.0.1';
            }
            return array($server, $user, $password);
        }
    }

    return new AdminerCustomization($plugins);
}

include ADMINER_DIR . '/adminer.php';
EOF

# Link Dracula dark theme if available
if [ -f /usr/share/adminer/designs/dracula/adminer.css ]; then
    ln -sf /usr/share/adminer/designs/dracula/adminer.css /opt/adminer/adminer.css
fi

chown -R www-data:www-data /opt/adminer
chmod -R 755 /opt/adminer

# Create systemd service for Adminer
cat << EOF > /etc/systemd/system/adminer.service
[Unit]
Description=Adminer PostgreSQL Web UI
After=network.target postgresql.service

[Service]
Type=simple
User=www-data
Group=www-data
WorkingDirectory=/opt/adminer
ExecStart=/usr/bin/php -S 0.0.0.0:${ADMINER_PORT} -t /opt/adminer
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now adminer.service >/dev/null
log_success "Adminer service running on port ${ADMINER_PORT}."

# ==============================================================================
# 9. Verification & Summary
# ==============================================================================
PRIMARY_IP=$(ip -4 addr show "$WAN_IFACE" 2>/dev/null | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | head -n1 || echo "localhost")

echo ""
echo -e "${C_GREEN}${C_BOLD}==========================================================${C_RESET}"
echo -e "${C_GREEN}${C_BOLD}   Orange Pi RV2 Server Setup Completed Successfully!     ${C_RESET}"
echo -e "${C_GREEN}${C_BOLD}==========================================================${C_RESET}"
echo ""
echo -e "  ${C_BOLD}PostgreSQL Version:${C_RESET}   $(sudo -u postgres psql -t -P format=unaligned -c 'SELECT version();' | head -n1 | cut -d' ' -f2)"
echo -e "  ${C_BOLD}pgvector Version:${C_RESET}     $(sudo -u postgres psql -d "$DB_NAME" -t -P format=unaligned -c "SELECT extversion FROM pg_extension WHERE extname='vector';")"
echo -e "  ${C_BOLD}Data Directory:${C_RESET}       ${TARGET_DATA_DIR}"
echo -e "  ${C_BOLD}Bridge Network:${C_RESET}       ${BRIDGE_NAME} (${BRIDGE_IP}) -> Ports: ${BRIDGE_PORTS[*]}"
echo -e "  ${C_BOLD}Internet Uplink:${C_RESET}      ${WAN_IFACE} (NAT Masquerade Enabled)"
echo -e "  ${C_BOLD}Adminer Web URL:${C_RESET}      http://${PRIMARY_IP}:${ADMINER_PORT}/"
echo ""
echo -e "  ${C_BOLD}Connection Rules:${C_RESET}"
echo -e "    • DuckLake Local:      Unix Socket (${C_GREEN}peer${C_RESET}, '${DB_USER}' only)"
echo -e "    • Local Web (Adminer): TCP Loopback 127.0.0.1 (${C_CYAN}SCRAM-SHA-256${C_RESET})"
echo -e "    • Network (${BRIDGE_NAME}/${WAN_IFACE}): Port 5432 is ${C_RED}DISABLED & BLOCKED${C_RESET} (No network access)"
echo ""
echo -e "=========================================================="
