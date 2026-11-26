#!/usr/bin/env bash
# ==============================================================================
# Optional AdGuard Home Installer & Maximum Blocking Configurator
# ==============================================================================
# Installs AdGuard Home cleanly into /opt/AdGuardHome (riscv64/arm64/amd64/armv7),
# frees port 53 from systemd-resolved if active, enables encrypted DoH/DoQ
# upstreams with optimistic DNS caching, configures 9 maximum-protection
# ad/tracker/malware/phishing/telemetry blocklists, and optionally configures
# local wildcard DNS rewrites for your home lab domain.
#
# Usage:
#   sudo ./setup_adguard.sh [OPTIONS]
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

# --- Defaults ---
AG_DIR="/opt/AdGuardHome"
AG_USER="${ADGUARD_USER:-${SUDO_USER:-admin}}"
if [ "$AG_USER" = "root" ]; then AG_USER="admin"; fi
AG_PASS="${ADGUARD_PASSWORD:-}"
AG_WEB_PORT="${ADGUARD_WEB_PORT:-3000}"
AG_DNS_PORT="${ADGUARD_DNS_PORT:-53}"
LAB_DOMAIN="${ADGUARD_DOMAIN:-}"
REWRITE_IP="${ADGUARD_REWRITE_IP:-}"
STRICT_MODE=false
INSTALL_ONLY=false

print_help() {
    cat << EOF
Optional AdGuard Home Setup & Maximum Blocklist Configurator

Usage: $0 [OPTIONS]

Options:
  --user <name>         AdGuard admin username (default: $AG_USER)
  --password <pass>     AdGuard admin password (auto-generated on fresh install)
  --web-port <port>     AdGuard Web UI port (default: $AG_WEB_PORT)
  --dns-port <port>     AdGuard DNS listening port (default: $AG_DNS_PORT)
  --domain <domain>     Optional home lab domain for local DNS rewrites (e.g. lab.example.com)
  --rewrite-ip <ip>     Target LAN IP for DNS rewrites (default: primary server IP)
  --strict              Replace HaGeZi PRO++ with HaGeZi ULTIMATE (strictest blocking)
  --install-only        Install binary & free port 53, leave web wizard unconfigured
  -h, --help            Show this help message
EOF
}

ORIG_ARGS=("$@")
while [[ $# -gt 0 ]]; do
    case "$1" in
        --user)       AG_USER="$2"; shift 2 ;;
        --password)   AG_PASS="$2"; shift 2 ;;
        --web-port)   AG_WEB_PORT="$2"; shift 2 ;;
        --dns-port)   AG_DNS_PORT="$2"; shift 2 ;;
        --domain)     LAB_DOMAIN="$2"; shift 2 ;;
        --rewrite-ip) REWRITE_IP="$2"; shift 2 ;;
        --strict)     STRICT_MODE=true; shift ;;
        --install-only) INSTALL_ONLY=true; shift ;;
        -h|--help)    print_help; exit 0 ;;
        *)            log_error "Unknown option: $1"; print_help; exit 1 ;;
    esac
done

if [ "$(id -u)" -ne 0 ]; then
    log_info "Requesting sudo privileges for AdGuard Home installation..."
    exec sudo bash "$0" "${ORIG_ARGS[@]}"
fi

echo -e "${C_CYAN}${C_BOLD}"
echo "=================================================================="
echo "    🛡️  AdGuard Home DNS Cache & Ad/Threat Blocker Setup          "
echo "=================================================================="
echo -e "${C_RESET}"

# --- 1. Detect Architecture & Install Binary ---
ARCH="$(uname -m)"
case "$ARCH" in
    riscv64)        AG_ARCH="riscv64" ;;
    aarch64|arm64)  AG_ARCH="arm64" ;;
    x86_64|amd64)   AG_ARCH="amd64" ;;
    armv7l|armv7)   AG_ARCH="armv7" ;;
    *)
        log_error "Unsupported CPU architecture for AdGuard Home: $ARCH"
        exit 1 ;;
esac

if [ ! -x "${AG_DIR}/AdGuardHome" ]; then
    log_info "Downloading AdGuard Home (${AG_ARCH})..."
    TMP_TGZ="$(mktemp /tmp/adguardhome.XXXXXX.tar.gz)"
    curl -fsSL "https://static.adtidy.org/adguardhome/release/AdGuardHome_linux_${AG_ARCH}.tar.gz" -o "$TMP_TGZ"
    mkdir -p /opt
    tar -xzf "$TMP_TGZ" -C /opt
    rm -f "$TMP_TGZ"
    log_success "Installed AdGuard Home binary into ${AG_DIR}."
else
    log_info "AdGuard Home binary already present at ${AG_DIR}/AdGuardHome."
fi

# --- 2. Free Port 53 from systemd-resolved if active ---
if systemctl is-active --quiet systemd-resolved; then
    log_info "Disabling systemd-resolved DNS stub listener to free port ${AG_DNS_PORT}..."
    mkdir -p /etc/systemd/resolved.conf.d
    cat << 'EOF' > /etc/systemd/resolved.conf.d/adguardhome.conf
[Resolve]
DNS=127.0.0.1
DNSStubListener=no
EOF
    systemctl restart systemd-resolved
    log_success "systemd-resolved stub disabled."
fi

# --- 3. Install & Start Systemd Service ---
if [ ! -f /etc/systemd/system/AdGuardHome.service ]; then
    log_info "Installing AdGuardHome systemd service..."
    "${AG_DIR}/AdGuardHome" -s install
else
    systemctl enable --now AdGuardHome.service >/dev/null 2>&1 || systemctl restart AdGuardHome.service
fi

# Wait for Web UI listener
log_info "Waiting for AdGuard Home web service on port ${AG_WEB_PORT}..."
for _ in {1..15}; do
    if curl -s -o /dev/null "http://127.0.0.1:${AG_WEB_PORT}/"; then
        break
    fi
    sleep 1
done

if [ "$INSTALL_ONLY" = true ]; then
    log_success "AdGuard Home installed (--install-only). Open http://<server-ip>:${AG_WEB_PORT}/ to complete setup."
    exit 0
fi

# --- 4. Run Initial Configuration if AdGuardHome.yaml does not exist ---
GENERATED_PASS=""
FRESH_INSTALL=false
if [ ! -f "${AG_DIR}/AdGuardHome.yaml" ]; then
    FRESH_INSTALL=true
    if [ -z "$AG_PASS" ]; then
        AG_PASS=$(python3 -c 'import secrets, string; a=string.ascii_letters+string.digits; print("".join(secrets.choice(a) for _ in range(18)))')
        GENERATED_PASS="$AG_PASS"
    fi

    log_info "Completing initial AdGuard Home configuration (DNS :${AG_DNS_PORT}, Web :${AG_WEB_PORT})..."
    curl -fsS -X POST "http://127.0.0.1:${AG_WEB_PORT}/control/install/configure" \
        -H "Content-Type: application/json" \
        -d "{
            \"web\": {\"ip\": \"0.0.0.0\", \"port\": ${AG_WEB_PORT}},
            \"dns\": {\"ip\": \"0.0.0.0\", \"port\": ${AG_DNS_PORT}},
            \"username\": \"${AG_USER}\",
            \"password\": \"${AG_PASS}\"
        }" >/dev/null
    sleep 2
    log_success "AdGuard Home initialized for user '${AG_USER}'."
fi

if [ -z "$AG_PASS" ]; then
    log_warn "AdGuardHome.yaml already exists and no --password was provided."
    log_info "Re-run with '--user <name> --password <pass>' to apply/update blocklists and DNS rewrites via API."
    exit 0
fi

# Detect rewrite target IP if --domain provided without --rewrite-ip
if [ -n "$LAB_DOMAIN" ] && [ -z "$REWRITE_IP" ]; then
    REWRITE_IP=$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{print $7}' | head -n1 || hostname -I | awk '{print $1}')
    REWRITE_IP="${REWRITE_IP:-127.0.0.1}"
fi

# --- 5. Apply Maximum Blocking, Encrypted Upstreams, Caching & Optional Rewrites ---
log_info "Applying encrypted DoH/DoQ upstreams, optimistic DNS cache, and blocklists..."

AG_USER="$AG_USER" AG_PASS="$AG_PASS" AG_WEB_PORT="$AG_WEB_PORT" \
LAB_DOMAIN="$LAB_DOMAIN" REWRITE_IP="$REWRITE_IP" STRICT_MODE="$STRICT_MODE" \
python3 - << 'PYEOF'
import base64
import json
import os
import urllib.request

user = os.environ["AG_USER"]
pw = os.environ["AG_PASS"]
port = os.environ["AG_WEB_PORT"]
lab_domain = os.environ.get("LAB_DOMAIN", "").strip()
rewrite_ip = os.environ.get("REWRITE_IP", "").strip()
strict_mode = os.environ.get("STRICT_MODE", "false").lower() == "true"

auth_hdr = "Basic " + base64.b64encode(f"{user}:{pw}".encode()).decode()

def api(method, path, body=None, timeout=90):
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Authorization": auth_hdr}
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode()
        return json.loads(raw) if raw.strip().startswith(("{", "[")) else raw

# 1. Encrypted upstreams + Optimistic Cache + DNSSEC
api("POST", "/control/dns_config", {
    "upstream_dns": [
        "https://dns.cloudflare.com/dns-query",
        "https://dns.quad9.net/dns-query",
        "quic://dns.adguard-dns.com",
    ],
    "bootstrap_dns": ["1.1.1.1", "9.9.9.9", "1.0.0.1"],
    "upstream_mode": "parallel",
    "fastest_timeout": "1s",
    "ratelimit": 0,
    "blocking_mode": "default",
    "edns_cs_enabled": False,
    "dnssec_enabled": True,
    "disable_ipv6": False,
    "cache_size": 16777216,
    "cache_ttl_min": 300,
    "cache_ttl_max": 86400,
    "cache_optimistic": True,
})

# 2. Safe Browsing & Filtering refresh interval
api("POST", "/control/safebrowsing/enable", {})
api("POST", "/control/filtering/config", {"enabled": True, "interval": 12})

hagezi_url = (
    "https://cdn.jsdelivr.net/gh/hagezi/dns-blocklists@latest/adblock/ultimate.txt"
    if strict_mode
    else "https://cdn.jsdelivr.net/gh/hagezi/dns-blocklists@latest/adblock/pro.plus.txt"
)
hagezi_name = "HaGeZi Multi ULTIMATE" if strict_mode else "HaGeZi Multi PRO++"

desired_filters = [
    ("AdGuard DNS filter", "https://adguardteam.github.io/HostlistsRegistry/assets/filter_1.txt"),
    (hagezi_name, hagezi_url),
    ("OISD Big", "https://big.oisd.nl"),
    ("StevenBlack Unified Hosts", "https://raw.githubusercontent.com/StevenBlack/hosts/master/hosts"),
    ("AdGuard DNS Popup Hosts filter", "https://adguardteam.github.io/HostlistsRegistry/assets/filter_59.txt"),
    ("HaGeZi Threat Intelligence Feeds", "https://adguardteam.github.io/HostlistsRegistry/assets/filter_44.txt"),
    ("Malicious URL Blocklist (URLHaus)", "https://adguardteam.github.io/HostlistsRegistry/assets/filter_11.txt"),
    ("Dandelion Sprout Anti-Malware List", "https://adguardteam.github.io/HostlistsRegistry/assets/filter_12.txt"),
    ("Phishing URL Blocklist (PhishTank+OpenPhish)", "https://adguardteam.github.io/HostlistsRegistry/assets/filter_30.txt"),
]

status = api("GET", "/control/filtering/status")
existing_urls = {f["url"] for f in (status.get("filters") or [])}

for name, url in desired_filters:
    if url not in existing_urls:
        try:
            api("POST", "/control/filtering/add_url", {"name": name, "url": url, "whitelist": False})
            print(f"[INFO] Added filter: {name}")
        except Exception as exc:
            print(f"[WARN] Failed adding {name}: {exc}")

api("POST", "/control/filtering/refresh", {"whitelist": False}, timeout=120)

# 3. Optional DNS Rewrites for home lab domain
if lab_domain and rewrite_ip:
    rewrites = api("GET", "/control/rewrite/list") or []
    existing_rw = {(r["domain"], r["answer"]) for r in rewrites}
    for dom in (f"*.{lab_domain}", lab_domain):
        if (dom, rewrite_ip) not in existing_rw:
            api("POST", "/control/rewrite/add", {"domain": dom, "answer": rewrite_ip})
            print(f"[INFO] Added DNS rewrite: {dom} -> {rewrite_ip}")

final_status = api("GET", "/control/filtering/status")
enabled = [f for f in (final_status.get("filters") or []) if f.get("enabled")]
total_rules = sum(f.get("rules_count", 0) for f in enabled)
print(f"[SUCCESS] Active blocklists: {len(enabled)} | Total rules: {total_rules:,}")
PYEOF

PRIMARY_IP=$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{print $7}' | head -n1 || hostname -I | awk '{print $1}')
PRIMARY_IP="${PRIMARY_IP:-127.0.0.1}"

echo ""
echo -e "${C_GREEN}${C_BOLD}==================================================================${C_RESET}"
echo -e "${C_GREEN}${C_BOLD}   ✓ AdGuard Home Setup Complete!                                 ${C_RESET}"
echo -e "${C_GREEN}${C_BOLD}==================================================================${C_RESET}"
echo -e "  Web Portal   : ${C_BOLD}http://${PRIMARY_IP}:${AG_WEB_PORT}/${C_RESET}"
echo -e "  DNS Server   : ${C_BOLD}${PRIMARY_IP}:${AG_DNS_PORT}${C_RESET} (DoH/DoQ upstream, 16 MB optimistic cache)"
echo -e "  Username     : ${C_BOLD}${AG_USER}${C_RESET}"
if [ -n "$GENERATED_PASS" ]; then
    echo -e "  Password     : ${C_BOLD}${GENERATED_PASS}${C_RESET}  (save this now!)"
fi
if [ -n "$LAB_DOMAIN" ]; then
    echo -e "  DNS Rewrites : ${C_BOLD}*.${LAB_DOMAIN}, ${LAB_DOMAIN} -> ${REWRITE_IP}${C_RESET}"
fi
echo "=================================================================="
