# Orange Pi RV2 HTTPS, Wildcard Certificates & CA Architecture (`CA.md`)

This document describes the TLS certificate authority workflow, DNS-01 delegation, Caddy HTTPS reverse proxy, and local home lab portal configured on the Orange Pi RV2 (`riscv64`) for **`*.lab.holz.one`**.

For LAN routing, bridge (`br0`), firewall hardening, and AdGuard Home DNS setup, refer to [NETWORK.md](NETWORK.md).

---

## 1. Architecture Overview (Publicly Trusted Let's Encrypt vs. Private CA)

To eliminate browser security warnings across all home lab devices without installing a custom self-signed Root CA certificate on every client—and without exposing home lab services to the public internet—the RV2 uses **Let's Encrypt (`ECDSA`) certificates via ACME DNS-01 validation** paired with **Caddy** as the local HTTPS reverse proxy:

```
                  Squarespace Domains (holz.one)
                  * @   A     -> 185.199.108-111.153 (GitHub Pages)
                  * *   CNAME -> salamcast.github.io (GitHub Pages)
                  * _acme-challenge.lab CNAME -> <uuid>.auth.acme-dns.io
                                                     │
                                     DNS-01 TXT updates (acme.sh)
                                                     ▼
                                     Let's Encrypt Public ACME CA
                                                     │
                                  Wildcard Cert: lab.holz.one, *.lab.holz.one
                                                     ▼
  LAN Client ──(HTTPS :443)──► Caddy Reverse Proxy (Orange Pi RV2)
   (AdGuard DNS rewrite:       ├── lab.holz.one       ──► /var/www/lab.holz.one (Static Landing Page)
    *.lab.holz.one -> RV2)     ├── ducklake.lab.holz.one ──► 127.0.0.1:5000 (DuckLake & AI Server)
                               ├── adminer.lab.holz.one  ──► 127.0.0.1:8080 (Adminer PostgreSQL UI)
                               └── adguard.lab.holz.one  ──► 127.0.0.1:3000 (AdGuard Home Web UI)
```

### Why `acme-dns` CNAME Delegation?
Squarespace Domains does not provide a public DNS API for automated ACME `TXT` record updates. Delegating only `_acme-challenge.lab.holz.one` via a static `CNAME` to `auth.acme-dns.io` allows `acme.sh` to issue and renew wildcard certificates fully automatically while keeping `holz.one` and `www.holz.one` DNS management on Squarespace (and GitHub Pages) untouched.

---

## 2. Squarespace DNS Delegation

In **Squarespace Domains** (`holz.one` → DNS Settings), a single static `CNAME` record delegates ACME challenge validation for `lab.holz.one` and `*.lab.holz.one`:

| Host | Type | Target |
| :--- | :--- | :--- |
| `_acme-challenge.lab` | `CNAME` | `1422f4f4-644f-4b4c-9274-80722cf460bc.auth.acme-dns.io` |

Because an explicit DNS `CNAME` overrides a higher-level wildcard (`*.holz.one`), only `_acme-challenge.lab.holz.one` routes to `acme-dns`, while `holz.one` and `www.holz.one` continue resolving to GitHub Pages.

Verify public delegation at any time:
```bash
dig +short CNAME _acme-challenge.lab.holz.one @1.1.1.1
```

---

## 3. Certificate Management & Auto-Renewal (`acme.sh`)

Certificates are managed under user `orangepi` using **`acme.sh`** (`letsencrypt` CA, ECC key):

| Component | Path on RV2 | Purpose |
| :--- | :--- | :--- |
| **ACME Client** | `/home/orangepi/.acme.sh/acme.sh` | Automated ACME DNS-01 issuance & renewal |
| **acme-dns Credentials** | `/home/orangepi/.acme.sh/acmedns_holz_one.json` | API credentials (`0600`) for `auth.acme-dns.io` |
| **Issuance Helper Script** | `/home/orangepi/issue-lab-cert.sh` | Verifies CNAME propagation, issues cert, installs to `/etc/ssl/lab.holz.one/` |
| **Full Chain Certificate** | `/etc/ssl/lab.holz.one/fullchain.pem` | Public cert + Let's Encrypt intermediate chain |
| **Private Key** | `/etc/ssl/lab.holz.one/privkey.pem` | ECC private key (`orangepi:caddy`, mode `0640`) |

### Renewal Schedule & Reload Hook
`acme.sh` runs a scheduled cron job as `orangepi`:
```cron
15 0,6,12,18 * * * "/home/orangepi/.acme.sh"/acme.sh --cron --home "/home/orangepi/.acme.sh" > /dev/null
```

When the certificate renews (automatically inside the ARI / ~60-day renewal window), `acme.sh` installs the updated files into `/etc/ssl/lab.holz.one/` and runs the saved reload hook:
```bash
chmod 640 /etc/ssl/lab.holz.one/privkey.pem && sudo chgrp caddy /etc/ssl/lab.holz.one/privkey.pem && sudo systemctl reload-or-restart caddy
```

---

## 4. Caddy HTTPS Reverse Proxy (`/etc/caddy/Caddyfile`)

**Caddy** (`caddy.service`) listens on ports `80` (automatic HTTP → HTTPS redirects) and `443` (HTTP/1.1, HTTP/2, and HTTP/3):

```caddyfile
lab.holz.one {
	tls /etc/ssl/lab.holz.one/fullchain.pem /etc/ssl/lab.holz.one/privkey.pem
	root * /var/www/lab.holz.one
	file_server
}

ducklake.lab.holz.one {
	tls /etc/ssl/lab.holz.one/fullchain.pem /etc/ssl/lab.holz.one/privkey.pem
	reverse_proxy 127.0.0.1:5000
}

adminer.lab.holz.one {
	tls /etc/ssl/lab.holz.one/fullchain.pem /etc/ssl/lab.holz.one/privkey.pem
	reverse_proxy 127.0.0.1:8080
}

adguard.lab.holz.one {
	tls /etc/ssl/lab.holz.one/fullchain.pem /etc/ssl/lab.holz.one/privkey.pem
	reverse_proxy 127.0.0.1:3000
}
```

* **`X-Forwarded-For` Preservation:** Caddy forwards the original client IP to DuckLake (`127.0.0.1:5000`), so `app.py`'s `trust_forwarded_for` network check continues enforcing CIDR / **Allow Remote AI** policy accurately.
* **Landing Page (`/var/www/lab.holz.one`):** Serves the static **HOLZ Home Lab** landing page (`index.html` + `holz-homelab.jpg`) on `https://lab.holz.one`. When migrating `lab.holz.one` to GitHub Pages in the future, point DNS for `lab.holz.one` to GitHub Pages and remove the `lab.holz.one` block from `/etc/caddy/Caddyfile`.

---

## 5. Local DNS Resolution (`*.lab.holz.one`)

Because `*.holz.one` resolves publicly to GitHub Pages, LAN clients need a local DNS rewrite pointing `*.lab.holz.one` to the Orange Pi RV2:

1. Complete the **AdGuard Home** setup wizard at `http://192.168.18.15:3000/` (disable Ubuntu's `systemd-resolved` stub listener `DNSStubListener=no` in `/etc/systemd/resolved.conf` if port `53` is occupied).
2. In AdGuard Home (**Filters → DNS rewrites → Add DNS rewrite**), add:
   * **Domain:** `*.lab.holz.one`
   * **Answer:** `192.168.18.15` (or `10.0.2.1` for wired `br0` clients)

*(Note: Ensure `lab.holz.one` is also covered either by adding a second rewrite for `lab.holz.one` if your AdGuard rewrite rule only matches subdomains, or per-host entries.)*

---

## 6. Operations & Verification Commands

```bash
# Check Caddy service and validate Caddyfile syntax:
sudo systemctl status caddy
sudo caddy validate --config /etc/caddy/Caddyfile

# List managed certificates and next renewal date:
~/.acme.sh/acme.sh --list

# Force manual renewal test (only when needed):
~/.acme.sh/acme.sh --renew -d lab.holz.one --ecc --force

# Test trusted HTTPS from a LAN machine without local DNS yet:
curl -fsS --resolve ducklake.lab.holz.one:443:192.168.18.15 https://ducklake.lab.holz.one/status
curl -fsSI --resolve lab.holz.one:443:192.168.18.15 https://lab.holz.one/
```
