#!/usr/bin/env bash
# ==============================================================================
# Orange Pi RV2 DuckLake & Local AI Server - Automated Installer
# ==============================================================================
# Sets up a complete, production-ready DuckLake Analytical Server and Local AI
# Engine with PostgreSQL metadata store, Parquet lake storage, Gunicorn WSGI,
# and systemd service management.
#
# Supported Architectures: riscv64, aarch64, x86_64
# Supported OS: Ubuntu 24.04+ / Debian 12+
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

# --- Ensure execution directory ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# --- Determine Real Invoking User ---
if [ -n "${SUDO_USER:-}" ] && [ "$SUDO_USER" != "root" ]; then
    APP_USER="$SUDO_USER"
else
    APP_USER="$(id -un)"
fi
APP_GROUP="$(id -gn "$APP_USER")"
APP_HOME="$(eval echo "~$APP_USER")"

# --- Default Settings ---
INSTALL_SERVICE=true
SERVER_PORT=5000
SERVER_HOST="0.0.0.0"
DUCKLAKE_DIR="/srv/DuckLake"
WAREHOUSE_DIR="/srv/Warehouse"
MODELS_DIR="${APP_HOME}/RV2-AI"
DB_USER="$APP_USER"
DB_NAME="$APP_USER"
WITH_ADGUARD=false
ADGUARD_DOMAIN=""
WITH_CA=false
CA_DOMAIN=""

# --- Parse Arguments ---
print_help() {
    cat << EOF
Orange Pi RV2 DuckLake & Local AI Server Installer

Usage: $0 [OPTIONS]

Options:
  --user <name>            Target Linux & Postgres user (default: $APP_USER)
  --db-name <name>         Postgres metadata database name (default: $DB_NAME)
  --port <port>            HTTP server listening port (default: $SERVER_PORT)
  --ducklake-dir <path>    DuckLake Parquet storage directory (default: $DUCKLAKE_DIR)
  --warehouse-dir <path>   Raw warehouse directory (default: $WAREHOUSE_DIR)
  --with-adguard           Also install & configure AdGuard Home (optional DNS blocker/cache)
  --adguard-domain <dom>   Optional home lab domain for AdGuard DNS rewrites (implies --with-adguard)
  --with-ca                Also install & configure CA/TLS reverse proxy (Caddy + acme.sh)
  --ca-domain <dom>        Base lab domain for wildcard certs (implies --with-ca)
  --no-service             Skip creating & starting systemd aisrv.service
  --help                   Display this help message
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --user)
            APP_USER="$2"
            APP_GROUP="$(id -gn "$APP_USER" 2>/dev/null || echo "$APP_USER")"
            shift 2 ;;
        --db-name)
            DB_NAME="$2"
            shift 2 ;;
        --port)
            SERVER_PORT="$2"
            shift 2 ;;
        --ducklake-dir)
            DUCKLAKE_DIR="$2"
            shift 2 ;;
        --warehouse-dir)
            WAREHOUSE_DIR="$2"
            shift 2 ;;
        --with-adguard)
            WITH_ADGUARD=true
            shift ;;
        --adguard-domain)
            WITH_ADGUARD=true
            ADGUARD_DOMAIN="$2"
            shift 2 ;;
        --with-ca)
            WITH_CA=true
            shift ;;
        --ca-domain)
            WITH_CA=true
            CA_DOMAIN="$2"
            shift 2 ;;
        --no-service)
            INSTALL_SERVICE=false
            shift ;;
        -h|--help)
            print_help
            exit 0 ;;
        *)
            log_error "Unknown option: $1"
            print_help
            exit 1 ;;
    esac
done

echo -e "${C_CYAN}${C_BOLD}"
echo "=================================================================="
echo "    🦆 Orange Pi RV2 DuckLake & Local AI Server Installer        "
echo "=================================================================="
echo -e "${C_RESET}"
log_info "Target User     : ${C_BOLD}${APP_USER}:${APP_GROUP}${C_RESET}"
log_info "Project Root    : ${C_BOLD}${SCRIPT_DIR}${C_RESET}"
log_info "DuckLake Path   : ${C_BOLD}${DUCKLAKE_DIR}${C_RESET}"
log_info "Warehouse Path  : ${C_BOLD}${WAREHOUSE_DIR}${C_RESET}"
log_info "Database Target : ${C_BOLD}${DB_USER}@localhost/${DB_NAME}${C_RESET}"
log_info "Service Port    : ${C_BOLD}${SERVER_PORT}${C_RESET}"
echo ""

# --- Self-elevate for root operations if needed ---
if [ "$(id -u)" -ne 0 ]; then
    log_info "Requesting sudo permissions for system package & storage setup..."
    exec sudo bash "$0" \
        --user "$APP_USER" \
        --db-name "$DB_NAME" \
        --port "$SERVER_PORT" \
        --ducklake-dir "$DUCKLAKE_DIR" \
        --warehouse-dir "$WAREHOUSE_DIR" \
        $([ "$WITH_ADGUARD" = true ] && echo "--with-adguard") \
        $([ -n "$ADGUARD_DOMAIN" ] && echo "--adguard-domain" "$ADGUARD_DOMAIN") \
        $([ "$WITH_CA" = true ] && echo "--with-ca") \
        $([ -n "$CA_DOMAIN" ] && echo "--ca-domain" "$CA_DOMAIN") \
        $([ "$INSTALL_SERVICE" = false ] && echo "--no-service")
fi

# ==============================================================================
# Step 1: Detect Architecture & System
# ==============================================================================
ARCH="$(uname -m)"
log_info "Detected system architecture: ${C_BOLD}${ARCH}${C_RESET}"

if [ ! -f /etc/os-release ]; then
    log_error "Unsupported OS. Debian or Ubuntu Linux required."
    exit 1
fi
. /etc/os-release
log_info "Detected OS: ${C_BOLD}${PRETTY_NAME}${C_RESET}"

# ==============================================================================
# Step 2: Install System Packages
# ==============================================================================
log_info "Updating apt package lists..."
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq

log_info "Installing core system packages, PostgreSQL, and build tools..."
apt-get install -y -qq \
    python3 \
    python3-venv \
    python3-dev \
    build-essential \
    postgresql \
    postgresql-contrib \
    libpq-dev \
    curl \
    git \
    jq \
    rsync \
    ca-certificates

log_success "System packages installed successfully."

# ==============================================================================
# Step 3: Setup Storage Directories
# ==============================================================================
log_info "Configuring storage directories..."

mkdir -p "$DUCKLAKE_DIR" "$WAREHOUSE_DIR" "${SCRIPT_DIR}/database"
chown -R "${APP_USER}:${APP_GROUP}" "$DUCKLAKE_DIR" "$WAREHOUSE_DIR" "${SCRIPT_DIR}/database"
chmod 775 "$DUCKLAKE_DIR" "$WAREHOUSE_DIR" "${SCRIPT_DIR}/database"

# Clean up legacy symlink /srv/Wearhouse if present
if [ -L "/srv/Wearhouse" ]; then
    rm -f /srv/Wearhouse
    log_info "Removed legacy /srv/Wearhouse symlink."
fi

log_success "Storage directories initialized: ${DUCKLAKE_DIR} and ${WAREHOUSE_DIR}"

# ==============================================================================
# Step 4: Configure PostgreSQL Metadata Store (Local-Only / DuckLake Exclusive)
# ==============================================================================
log_info "Configuring PostgreSQL service for local-only DuckLake access..."
systemctl start postgresql
systemctl enable postgresql

# Create PostgreSQL database user if missing
if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='${DB_USER}'" | grep -q 1; then
    log_info "Creating PostgreSQL user '${DB_USER}'..."
    sudo -u postgres createuser -s "${DB_USER}"
    log_success "Created PostgreSQL user '${DB_USER}'"
else
    log_info "PostgreSQL user '${DB_USER}' already exists."
fi

# Create PostgreSQL database if missing
if ! sudo -u postgres psql -lqt | cut -d \| -f 1 | grep -qw "${DB_NAME}"; then
    log_info "Creating PostgreSQL database '${DB_NAME}'..."
    sudo -u postgres createdb -O "${DB_USER}" "${DB_NAME}"
    log_success "Created PostgreSQL database '${DB_NAME}'"
else
    log_info "PostgreSQL database '${DB_NAME}' already exists."
fi

# Lock down PostgreSQL to local-only access (no external/LAN network access)
PG_CONF_DIR=$(find /etc/postgresql -maxdepth 2 -type d -name main 2>/dev/null | sort -V | tail -n 1 || true)
if [ -n "$PG_CONF_DIR" ] && [ -f "${PG_CONF_DIR}/postgresql.conf" ] && [ -f "${PG_CONF_DIR}/pg_hba.conf" ]; then
    if grep -qE "^#*listen_addresses\s*=" "${PG_CONF_DIR}/postgresql.conf"; then
        sed -i "s|^#*listen_addresses\s*=.*|listen_addresses = 'localhost'|" "${PG_CONF_DIR}/postgresql.conf"
    else
        echo "listen_addresses = 'localhost'" >> "${PG_CONF_DIR}/postgresql.conf"
    fi

    if [ ! -f "${PG_CONF_DIR}/pg_hba.conf.orig" ]; then
        cp "${PG_CONF_DIR}/pg_hba.conf" "${PG_CONF_DIR}/pg_hba.conf.orig"
    fi

    cat << EOF > "${PG_CONF_DIR}/pg_hba.conf"
# PostgreSQL Client Authentication (Local-Only / DuckLake Exclusive)
# Network access is disabled. Only local DuckLake Unix socket (${DB_USER} peer)
# and local loopback Adminer (127.0.0.1 / ::1 SCRAM-SHA-256) are allowed.
# TYPE  DATABASE        USER            ADDRESS                 METHOD
local   all             postgres                                peer
local   all             ${DB_USER}                              peer
host    all             ${DB_USER}      127.0.0.1/32            scram-sha-256
host    all             ${DB_USER}      ::1/128                 scram-sha-256
EOF
    chmod 640 "${PG_CONF_DIR}/pg_hba.conf"
    chown postgres:postgres "${PG_CONF_DIR}/pg_hba.conf"

    systemctl restart postgresql
    log_success "PostgreSQL restricted to localhost/Unix socket only (${PG_CONF_DIR})."
fi

# Harden firewall if iptables is present (drop port 5432 on all non-loopback interfaces)
if command -v iptables >/dev/null 2>&1; then
    iptables -D INPUT -i wlan0 -p tcp --dport 5432 -j DROP 2>/dev/null || true
    if ! iptables -C INPUT ! -i lo -p tcp --dport 5432 -j DROP 2>/dev/null; then
        iptables -I INPUT ! -i lo -p tcp --dport 5432 -j DROP 2>/dev/null || true
    fi
    if command -v netfilter-persistent >/dev/null 2>&1; then
        netfilter-persistent save >/dev/null 2>&1 || true
    fi
fi

# Verify local peer socket authentication
if sudo -u "${APP_USER}" psql -d "${DB_NAME}" -c "SELECT 1" >/dev/null 2>&1; then
    log_success "PostgreSQL peer authentication verified for user '${APP_USER}'."
else
    log_warn "Direct peer auth test was non-zero; ensuring local socket access..."
fi

# ==============================================================================
# Step 5: Setup Python Virtual Environment & Dependencies
# ==============================================================================
VENV_DIR="${SCRIPT_DIR}/.venv"
log_info "Setting up Python virtual environment in ${C_BOLD}${VENV_DIR}${C_RESET}..."

# Check if uv is available for blazing fast package installation on RISC-V
USE_UV=false
if command -v uv >/dev/null 2>&1; then
    USE_UV=true
elif [ -f "${APP_HOME}/.local/bin/uv" ]; then
    export PATH="${APP_HOME}/.local/bin:${PATH}"
    USE_UV=true
fi

# Create venv if missing
if [ ! -d "$VENV_DIR" ]; then
    if [ "$USE_UV" = true ]; then
        log_info "Creating virtual environment using uv..."
        sudo -u "${APP_USER}" uv venv "$VENV_DIR" --python python3
    else
        log_info "Creating virtual environment using python3-venv..."
        sudo -u "${APP_USER}" python3 -m venv "$VENV_DIR"
    fi
fi

# Install dependencies
log_info "Installing Python dependencies (DuckDB, Flask, Gunicorn, psycopg, pandas)..."
if [ "$USE_UV" = true ]; then
    sudo -u "${APP_USER}" uv pip install --python "${VENV_DIR}/bin/python" -r "${SCRIPT_DIR}/requirements.txt"
else
    sudo -u "${APP_USER}" "${VENV_DIR}/bin/pip" install --upgrade pip -q
    sudo -u "${APP_USER}" "${VENV_DIR}/bin/pip" install -r "${SCRIPT_DIR}/requirements.txt" -q
fi
log_success "Python dependencies installed successfully."

# Check ONNX Runtime GenAI
if sudo -u "${APP_USER}" "${VENV_DIR}/bin/python" -c "import onnxruntime_genai" 2>/dev/null; then
    log_success "onnxruntime-genai is present. Local AI inference enabled."
else
    log_warn "onnxruntime-genai not detected in virtualenv."
    log_info "DuckLake analytical query features are fully operational."
    if [ "$ARCH" = "riscv64" ]; then
        log_info "To enable AI on RISC-V, install pre-built onnxruntime-genai wheel for SpacemiT K1."
    else
        log_info "To enable AI on $ARCH, run: ${VENV_DIR}/bin/pip install onnxruntime-genai"
    fi
fi

# ==============================================================================
# Step 6: Configure server.yaml and .env
# ==============================================================================
log_info "Configuring server settings and environment variables..."

# 1. server.yaml
if [ ! -f "${SCRIPT_DIR}/server.yaml" ]; then
    if [ -f "${SCRIPT_DIR}/server.yaml.example" ]; then
        cp "${SCRIPT_DIR}/server.yaml.example" "${SCRIPT_DIR}/server.yaml"
    else
        cat << 'EOF' > "${SCRIPT_DIR}/server.yaml"
server:
  host: "0.0.0.0"
  port: 5000
  debug: false
  cors_enabled: true

postgres:
  host: ""
  port: 5432
  dbname: "orangepi"
  user: "orangepi"
  password: ""
  connect_timeout: 10
  min_connections: 1
  max_connections: 5

storage:
  ducklake_dir: "/srv/DuckLake"
  warehouse_dir: "/srv/Warehouse"
  models_dir: "/cluster/RV2-AI"
  db_cache_dir: "/cluster/database"
  max_upload_size_mb: 250

ducklake:
  memory_limit: "1GB"
  threads: 4
  max_query_rows: 5000
  auto_scan_on_startup: true
  auto_create_views: true

ai:
  enabled: true
  default_context_length: 2048
  default_max_tokens: 256
  default_temperature: 0.7
  repetition_penalty: 1.1

security:
  allow_remote_ai: false
  ai_allowed_cidrs:
    - "10.0.2.0/24"
    - "127.0.0.1/32"
    - "::1/128"
  trust_forwarded_for: true
EOF
    fi
    # Substitute customized user & database
    sed -i "s/user: \"orangepi\"/user: \"${DB_USER}\"/g" "${SCRIPT_DIR}/server.yaml"
    sed -i "s/dbname: \"orangepi\"/dbname: \"${DB_NAME}\"/g" "${SCRIPT_DIR}/server.yaml"
    sed -i "s|ducklake_dir: \"/srv/DuckLake\"|ducklake_dir: \"${DUCKLAKE_DIR}\"|g" "${SCRIPT_DIR}/server.yaml"
    sed -i "s|port: 5000|port: ${SERVER_PORT}|g" "${SCRIPT_DIR}/server.yaml"
    chown "${APP_USER}:${APP_GROUP}" "${SCRIPT_DIR}/server.yaml"
    log_success "Generated customized server.yaml"
fi

# 2. .env
if [ ! -f "${SCRIPT_DIR}/.env" ]; then
    RANDOM_KEY=$(head -c 32 /dev/urandom | base64 | tr -dc 'a-zA-Z0-9' | head -c 32)
    cat << EOF > "${SCRIPT_DIR}/.env"
# Auto-generated environment secrets
PG_HOST=
PG_PORT=5432
PG_DBNAME=${DB_NAME}
PG_USER=${DB_USER}
PG_PASSWORD=
SERVER_HOST=${SERVER_HOST}
SERVER_PORT=${SERVER_PORT}
SECRET_KEY=${RANDOM_KEY}
CERT_DISTRIBUTION_TOKEN=
DUCKLAKE_MEMORY_LIMIT=1GB
DUCKLAKE_THREADS=4
HF_TOKEN=
EOF
    chown "${APP_USER}:${APP_GROUP}" "${SCRIPT_DIR}/.env"
    chmod 600 "${SCRIPT_DIR}/.env"
    log_success "Generated secure .env file"
fi

# ==============================================================================
# Step 7: Setup Systemd Service (Gunicorn with gthread)
# ==============================================================================
if [ "$INSTALL_SERVICE" = true ]; then
    SERVICE_FILE="/etc/systemd/system/aisrv.service"
    log_info "Installing systemd service unit: ${C_BOLD}${SERVICE_FILE}${C_RESET}..."

    cat << EOF > "$SERVICE_FILE"
[Unit]
Description=Orange Pi RV2 DuckLake & Local AI Server (Gunicorn WSGI)
After=network.target postgresql.service
Wants=postgresql.service

[Service]
Type=simple
User=${APP_USER}
Group=${APP_GROUP}
WorkingDirectory=${SCRIPT_DIR}
Environment=PYTHONUNBUFFERED=1
ExecStart=${VENV_DIR}/bin/gunicorn --workers 1 --threads 4 --worker-class gthread --bind ${SERVER_HOST}:${SERVER_PORT} --timeout 120 --keep-alive 5 --access-logfile - --error-logfile - app:app
Restart=always
RestartSec=3s
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload
    systemctl enable aisrv.service
    systemctl restart aisrv.service
    log_success "aisrv.service installed, enabled, and started."

    # ==============================================================================
    # Step 8: Service Health Check Verification
    # ==============================================================================
    log_info "Verifying service health on http://127.0.0.1:${SERVER_PORT}/status..."
    HEALTHY=false
    for i in {1..12}; do
        sleep 1
        if curl -s "http://127.0.0.1:${SERVER_PORT}/status" | grep -q "online"; then
            HEALTHY=true
            break
        fi
    done

    if [ "$HEALTHY" = true ]; then
        STATUS_JSON=$(curl -s "http://127.0.0.1:${SERVER_PORT}/status")
        DUCKDB_VER=$(echo "$STATUS_JSON" | jq -r '.duckdb_version // "unknown"')
        PG_CONN=$(echo "$STATUS_JSON" | jq -r '.postgres_connected // false')
        TABLE_CNT=$(echo "$STATUS_JSON" | jq -r '.storage.parquet_tables_count // 0')
        log_success "Server is ONLINE! (DuckDB: v${DUCKDB_VER}, Postgres: ${PG_CONN}, Tables: ${TABLE_CNT})"
    else
        log_warn "Health check timed out. Checking recent journal logs:"
        journalctl -u aisrv.service -n 15 --no-pager || true
    fi
fi

# ==============================================================================
# Step 9: Optional AdGuard Home DNS Setup (--with-adguard)
# ==============================================================================
if [ "$WITH_ADGUARD" = true ]; then
    if [ -x "${SCRIPT_DIR}/setup_adguard.sh" ] || [ -f "${SCRIPT_DIR}/setup_adguard.sh" ]; then
        log_info "Running optional AdGuard Home installer (setup_adguard.sh)..."
        ADGUARD_ARGS=(--user "$APP_USER")
        if [ -n "$ADGUARD_DOMAIN" ]; then
            ADGUARD_ARGS+=(--domain "$ADGUARD_DOMAIN")
        fi
        bash "${SCRIPT_DIR}/setup_adguard.sh" "${ADGUARD_ARGS[@]}"
    else
        log_warn "--with-adguard requested, but setup_adguard.sh was not found in ${SCRIPT_DIR}."
    fi
fi

# ==============================================================================
# Step 10: Optional CA / Wildcard HTTPS Reverse Proxy Setup (--with-ca)
# ==============================================================================
if [ "$WITH_CA" = true ]; then
    if [ -f "${SCRIPT_DIR}/setup_ca.sh" ]; then
        log_info "Running optional CA & Caddy installer (setup_ca.sh)..."
        CA_ARGS=(--user "$APP_USER")
        if [ -n "$CA_DOMAIN" ]; then
            CA_ARGS+=(--domain "$CA_DOMAIN")
        fi
        bash "${SCRIPT_DIR}/setup_ca.sh" "${CA_ARGS[@]}"
    else
        log_warn "--with-ca requested, but setup_ca.sh was not found in ${SCRIPT_DIR}."
    fi
fi

# Detect primary IPv4 address
PRIMARY_IP=$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{print $7}' || hostname -I | awk '{print $1}')
PRIMARY_IP="${PRIMARY_IP:-127.0.0.1}"

echo ""
echo -e "${C_GREEN}${C_BOLD}"
echo "=================================================================="
echo "    ✓ DuckLake & Local AI Server Installation Complete!           "
echo "=================================================================="
echo -e "${C_RESET}"
echo -e "Web Analytics Console   : ${C_BOLD}http://${PRIMARY_IP}:${SERVER_PORT}/${C_RESET}"
echo -e "REST API Manifest       : ${C_BOLD}http://${PRIMARY_IP}:${SERVER_PORT}/manifest${C_RESET}"
echo -e "OpenAPI 3.0 JSON Spec   : ${C_BOLD}http://${PRIMARY_IP}:${SERVER_PORT}/openapi.json${C_RESET}"
echo -e "Parquet Lake Directory  : ${C_BOLD}${DUCKLAKE_DIR}${C_RESET}"
echo -e "Raw Warehouse Directory : ${C_BOLD}${WAREHOUSE_DIR}${C_RESET}"
echo ""
echo -e "Service Management Commands:"
echo -e "  sudo systemctl status aisrv"
echo -e "  sudo systemctl restart aisrv"
echo -e "  journalctl -u aisrv -f"
echo ""
echo -e "Example analytical query:"
echo -e "  curl -X POST http://${PRIMARY_IP}:${SERVER_PORT}/ducklake/query \\"
echo -e "    -H 'Content-Type: application/json' -d '{\"query\": \"SHOW TABLES;\"}'"
echo "=================================================================="
