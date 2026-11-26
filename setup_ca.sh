#!/usr/bin/env bash
# ==============================================================================
# Optional CA / Wildcard HTTPS & Reverse Proxy Installer (Caddy + acme.sh)
# ==============================================================================
# Configures publicly trusted Let's Encrypt wildcard certificates via DNS-01
# (acme-dns CNAME delegation), Caddy HTTPS reverse proxy routing, DuckLake
# Web Console CA integration, and secure SSL key distribution tokens.
#
# Usage:
#   sudo ./setup_ca.sh [OPTIONS]
# ==============================================================================

set -euo pipefail

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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -n "${SUDO_USER:-}" ] && [ "$SUDO_USER" != "root" ]; then
    APP_USER="$SUDO_USER"
else
    APP_USER="$(id -un)"
fi
APP_HOME="$(eval echo "~$APP_USER")"

BASE_DOMAIN="${CA_DOMAIN:-}"
ACME_EMAIL="${CA_EMAIL:-}"
SSL_DIR="${CA_SSL_DIR:-/etc/ssl/lab}"
PORTAL_DIR="${CA_PORTAL_DIR:-/var/www/lab}"
CA_SERVER="${CA_SERVER:-letsencrypt}"
SERVICES_SPEC="${CA_SERVICES:-ducklake=5000,adminer=8080,adguard=3000}"
ISSUE_NOW=false

print_help() {
    cat << EOF
Optional CA / Wildcard HTTPS & Reverse Proxy Setup (Caddy + acme.sh)

Usage: $0 [OPTIONS]

Options:
  --user <name>         Linux user running DuckLake & acme.sh (default: $APP_USER)
  --domain <domain>     Base home lab domain (e.g. lab.example.com)
  --email <email>       ACME registration email (default: admin@<domain>)
  --ssl-dir <path>      Certificate install directory (default: $SSL_DIR)
  --portal-dir <path>   Static landing page root (default: $PORTAL_DIR)
  --services <spec>     Comma-separated sub=port mappings (default: $SERVICES_SPEC)
  --ca-server <name>    ACME CA server (letsencrypt or zerossl; default: $CA_SERVER)
  --issue               Attempt DNS-01 issuance immediately after setup
  -h, --help            Display this help message
EOF
}

ORIG_ARGS=("$@")
while [[ $# -gt 0 ]]; do
    case "$1" in
        --user)       APP_USER="$2"; APP_HOME="$(eval echo "~$APP_USER")"; shift 2 ;;
        --domain)     BASE_DOMAIN="$2"; shift 2 ;;
        --email)      ACME_EMAIL="$2"; shift 2 ;;
        --ssl-dir)    SSL_DIR="$2"; shift 2 ;;
        --portal-dir) PORTAL_DIR="$2"; shift 2 ;;
        --services)   SERVICES_SPEC="$2"; shift 2 ;;
        --ca-server)  CA_SERVER="$2"; shift 2 ;;
        --issue)      ISSUE_NOW=true; shift ;;
        -h|--help)    print_help; exit 0 ;;
        *)            log_error "Unknown option: $1"; print_help; exit 1 ;;
    esac
done

if [ "$(id -u)" -ne 0 ]; then
    log_info "Requesting sudo privileges for CA & Caddy installation..."
    exec sudo bash "$0" "${ORIG_ARGS[@]}"
fi

if [ -z "$BASE_DOMAIN" ] && [ -f "${SCRIPT_DIR}/server.yaml" ]; then
    BASE_DOMAIN=$(python3 -c "import yaml; c=yaml.safe_load(open('${SCRIPT_DIR}/server.yaml')) or {}; print((c.get('certificates') or {}).get('base_domain', ''))" 2>/dev/null || echo "")
fi
BASE_DOMAIN="${BASE_DOMAIN:-lab.example.com}"
ACME_EMAIL="${ACME_EMAIL:-admin@${BASE_DOMAIN}}"

echo -e "${C_CYAN}${C_BOLD}"
echo "=================================================================="
echo "    🔐 Home Lab CA, Wildcard HTTPS & Key Distribution Setup       "
echo "=================================================================="
echo -e "${C_RESET}"
log_info "App User     : ${C_BOLD}${APP_USER}${C_RESET}"
log_info "Base Domain  : ${C_BOLD}${BASE_DOMAIN} (*.${BASE_DOMAIN})${C_RESET}"
log_info "SSL Dir      : ${C_BOLD}${SSL_DIR}${C_RESET}"
log_info "Portal Dir   : ${C_BOLD}${PORTAL_DIR}${C_RESET}"
echo ""

# --- 1. Install Required Packages ---
log_info "Installing Caddy, OpenSSL, socat, curl, and dnsutils..."
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq caddy socat curl openssl dnsutils

# --- 2. Install acme.sh for APP_USER ---
ACME_HOME="${APP_HOME}/.acme.sh"
if [ ! -x "${ACME_HOME}/acme.sh" ]; then
    log_info "Installing acme.sh into ${ACME_HOME}..."
    sudo -u "$APP_USER" bash -c "curl -fsSL https://get.acme.sh | sh -s email='${ACME_EMAIL}'"
fi
sudo -u "$APP_USER" "${ACME_HOME}/acme.sh" --home "${ACME_HOME}" --set-default-ca --server "${CA_SERVER}" >/dev/null

# --- 3. Register acme-dns Credentials if Missing ---
ACMEDNS_CONF=$(ls -1 "${ACME_HOME}"/acmedns*.json 2>/dev/null | head -n1 || true)
if [ -z "$ACMEDNS_CONF" ]; then
    ACMEDNS_CONF="${ACME_HOME}/acmedns_lab.json"
    log_info "Registering new acme-dns account at https://auth.acme-dns.io/register..."
    sudo -u "$APP_USER" curl -fsSL -X POST https://auth.acme-dns.io/register -o "$ACMEDNS_CONF"
    chmod 600 "$ACMEDNS_CONF"
    chown "${APP_USER}:$(id -gn "$APP_USER")" "$ACMEDNS_CONF"
    log_success "Saved acme-dns credentials to ${ACMEDNS_CONF}."
else
    log_info "Using existing acme-dns credentials: ${ACMEDNS_CONF}."
fi
ACMEDNS_FULLDOMAIN=$(python3 -c "import json; print(json.load(open('${ACMEDNS_CONF}'))['fulldomain'])")

# --- 4. Prepare SSL, Portal & Sudoers Permissions ---
usermod -aG caddy "$APP_USER"
mkdir -p "$SSL_DIR" "$PORTAL_DIR"
chown "${APP_USER}:caddy" "$SSL_DIR"
chmod 750 "$SSL_DIR"
chown "${APP_USER}:caddy" "$PORTAL_DIR"
chmod 755 "$PORTAL_DIR"

if [ ! -f "${PORTAL_DIR}/index.html" ]; then
    cat << EOF > "${PORTAL_DIR}/index.html"
<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><title>${BASE_DOMAIN} | Home Lab Portal</title></head>
<body style="font-family:sans-serif;background:#0d1117;color:#f0f6fc;display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0;">
  <div style="text-align:center;padding:32px;border:1px solid #30363d;border-radius:12px;background:#161b22;">
    <h1>Home Lab Portal (${BASE_DOMAIN})</h1>
    <p style="color:#8b949e;">Replace <code>${PORTAL_DIR}/index.html</code> with your own landing page.</p>
  </div>
</body>
</html>
EOF
    chown "${APP_USER}:caddy" "${PORTAL_DIR}/index.html"
    chmod 644 "${PORTAL_DIR}/index.html"
fi

# Scoped sudoers permissions so DuckLake app (running as APP_USER) can manage Caddy & key perms cleanly
cat << EOF > /etc/sudoers.d/ducklake-ca
# Allow ${APP_USER} (DuckLake CertManager) to update Caddyfile, key permissions, and reload Caddy
${APP_USER} ALL=(root) NOPASSWD: /usr/bin/tee /etc/caddy/Caddyfile
${APP_USER} ALL=(root) NOPASSWD: /usr/bin/caddy validate --config /etc/caddy/Caddyfile
${APP_USER} ALL=(root) NOPASSWD: /usr/bin/systemctl reload caddy, /usr/bin/systemctl restart caddy, /usr/bin/systemctl reload-or-restart caddy, /usr/bin/systemctl enable --now caddy
${APP_USER} ALL=(root) NOPASSWD: /usr/bin/chgrp caddy ${SSL_DIR}/privkey.pem
EOF
chmod 440 /etc/sudoers.d/ducklake-ca
visudo -cf /etc/sudoers.d/ducklake-ca >/dev/null

# --- 5. Ensure CERT_DISTRIBUTION_TOKEN exists in .env ---
if [ -f "${SCRIPT_DIR}/.env" ]; then
    if ! grep -q "^CERT_DISTRIBUTION_TOKEN=." "${SCRIPT_DIR}/.env"; then
        DIST_TOK=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
        if grep -q "^CERT_DISTRIBUTION_TOKEN=" "${SCRIPT_DIR}/.env"; then
            sed -i "s|^CERT_DISTRIBUTION_TOKEN=.*|CERT_DISTRIBUTION_TOKEN=${DIST_TOK}|" "${SCRIPT_DIR}/.env"
        else
            echo "CERT_DISTRIBUTION_TOKEN=${DIST_TOK}" >> "${SCRIPT_DIR}/.env"
        fi
        log_success "Generated CERT_DISTRIBUTION_TOKEN in ${SCRIPT_DIR}/.env for automated node pulls."
    fi
fi

# --- 6. Update server.yaml certificates configuration ---
if [ -f "${SCRIPT_DIR}/server.yaml" ]; then
    SCRIPT_DIR="$SCRIPT_DIR" BASE_DOMAIN="$BASE_DOMAIN" SSL_DIR="$SSL_DIR" \
    PORTAL_DIR="$PORTAL_DIR" ACME_HOME="$ACME_HOME" ACMEDNS_CONF="$ACMEDNS_CONF" \
    CA_SERVER="$CA_SERVER" SERVICES_SPEC="$SERVICES_SPEC" \
    python3 - << 'PYEOF'
import os, yaml

yaml_path = os.path.join(os.environ["SCRIPT_DIR"], "server.yaml")
data = yaml.safe_load(open(yaml_path, "r", encoding="utf-8")) or {}

services = [{"subdomain": "@", "type": "static", "target": os.environ["PORTAL_DIR"], "enabled": True, "tls": True}]
for item in os.environ["SERVICES_SPEC"].split(","):
    item = item.strip()
    if "=" in item:
        sub, target = item.split("=", 1)
        sub = sub.strip().lower()
        target = target.strip()
        if target.isdigit():
            target = f"127.0.0.1:{target}"
        services.append({"subdomain": sub, "type": "proxy", "target": target, "enabled": True, "tls": True})

data["certificates"] = {
    "enabled": True,
    "base_domain": os.environ["BASE_DOMAIN"],
    "include_wildcard": True,
    "acme_provider": "dns_acmedns",
    "ca_server": os.environ["CA_SERVER"],
    "ssl_dir": os.environ["SSL_DIR"],
    "acme_home": os.environ["ACME_HOME"],
    "acme_dns_conf": os.environ["ACMEDNS_CONF"],
    "caddyfile": "/etc/caddy/Caddyfile",
    "portal_root": os.environ["PORTAL_DIR"],
    "allowed_cidrs": (data.get("certificates") or {}).get("allowed_cidrs") or ["10.0.2.0/24", "127.0.0.1/32", "::1/128"],
    "services": services,
}
with open(yaml_path, "w", encoding="utf-8") as f:
    yaml.dump(data, f, default_flow_style=False, sort_keys=False)
PYEOF
    chown "${APP_USER}:$(id -gn "$APP_USER")" "${SCRIPT_DIR}/server.yaml"
    log_success "Updated ${SCRIPT_DIR}/server.yaml certificates section."
fi

# --- 7. Generate Caddyfile (HTTPS if certs exist, HTTP fallback otherwise) ---
HAS_CERTS=false
if [ -f "${SSL_DIR}/fullchain.pem" ] && [ -f "${SSL_DIR}/privkey.pem" ]; then
    HAS_CERTS=true
fi

{
    echo "# Auto-generated by setup_ca.sh for *.${BASE_DOMAIN}"
    echo ""
    if [ "$HAS_CERTS" = true ]; then
        echo "${BASE_DOMAIN} {"
        echo "	tls ${SSL_DIR}/fullchain.pem ${SSL_DIR}/privkey.pem"
    else
        echo "http://${BASE_DOMAIN} {"
    fi
    echo "	root * ${PORTAL_DIR}"
    echo "	file_server"
    echo "}"
    echo ""
    IFS=',' read -ra SVC_ARR <<< "$SERVICES_SPEC"
    for item in "${SVC_ARR[@]}"; do
        item="${item// /}"
        if [[ "$item" == *"="* ]]; then
            sub="${item%%=*}"
            target="${item#*=}"
            if [[ "$target" =~ ^[0-9]+$ ]]; then target="127.0.0.1:${target}"; fi
            if [ "$HAS_CERTS" = true ]; then
                echo "${sub}.${BASE_DOMAIN} {"
                echo "	tls ${SSL_DIR}/fullchain.pem ${SSL_DIR}/privkey.pem"
            else
                echo "http://${sub}.${BASE_DOMAIN} {"
            fi
            echo "	reverse_proxy ${target}"
            echo "}"
            echo ""
        fi
    done
} > /etc/caddy/Caddyfile

caddy validate --config /etc/caddy/Caddyfile >/dev/null
systemctl enable --now caddy

# --- 8. Optional immediate issuance ---
if [ "$ISSUE_NOW" = true ]; then
    log_info "Checking _acme-challenge.${BASE_DOMAIN} CNAME delegation..."
    RESOLVED=$(dig +short CNAME "_acme-challenge.${BASE_DOMAIN}" @1.1.1.1 | sed 's/\.$//' || true)
    if [ "$RESOLVED" = "$ACMEDNS_FULLDOMAIN" ]; then
        sudo -u "$APP_USER" bash -c "
            export ACMEDNS_BASE_URL='https://auth.acme-dns.io'
            export ACMEDNS_USERNAME=\$(python3 -c \"import json; print(json.load(open('${ACMEDNS_CONF}'))['username'])\")
            export ACMEDNS_PASSWORD=\$(python3 -c \"import json; print(json.load(open('${ACMEDNS_CONF}'))['password'])\")
            export ACMEDNS_SUBDOMAIN=\$(python3 -c \"import json; print(json.load(open('${ACMEDNS_CONF}'))['subdomain'])\")
            '${ACME_HOME}/acme.sh' --home '${ACME_HOME}' --issue --dns dns_acmedns -d '${BASE_DOMAIN}' -d '*.${BASE_DOMAIN}' --server '${CA_SERVER}'
            '${ACME_HOME}/acme.sh' --home '${ACME_HOME}' --install-cert -d '${BASE_DOMAIN}' \
                --fullchain-file '${SSL_DIR}/fullchain.pem' \
                --key-file '${SSL_DIR}/privkey.pem' \
                --reloadcmd 'chmod 640 ${SSL_DIR}/privkey.pem && sudo chgrp caddy ${SSL_DIR}/privkey.pem && sudo systemctl reload-or-restart caddy'
        "
        log_success "Certificate issued! Re-run setup_ca.sh or click 'Apply to Caddy' in Settings to enable HTTPS blocks."
    else
        log_warn "CNAME not propagated yet (expected ${ACMEDNS_FULLDOMAIN}, got '${RESOLVED:-none}')."
    fi
fi

echo ""
echo -e "${C_GREEN}${C_BOLD}==================================================================${C_RESET}"
echo -e "${C_GREEN}${C_BOLD}   ✓ CA & Reverse Proxy Setup Complete!                           ${C_RESET}"
echo -e "${C_GREEN}${C_BOLD}==================================================================${C_RESET}"
echo -e "  Required Public DNS CNAME:"
echo -e "    Host   : ${C_BOLD}_acme-challenge.${BASE_DOMAIN}${C_RESET}"
echo -e "    Target : ${C_BOLD}${ACMEDNS_FULLDOMAIN}${C_RESET}"
echo -e "  Next:"
echo -e "    Add the CNAME above, then click 'Generate Certificate' in the"
echo -e "    DuckLake Settings tab or re-run: sudo $0 --domain ${BASE_DOMAIN} --issue"
echo "=================================================================="
