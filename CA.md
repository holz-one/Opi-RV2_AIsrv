# Home Lab HTTPS, Wildcard Certificates & CA Architecture (`CA.md`)

This guide explains how to set up **publicly trusted wildcard HTTPS certificates** (`*.lab.<your-domain>`) for private home lab services without self-signed certificates, without exposing inbound LAN ports to the internet, and without breaking an existing public website or GitHub Pages site on your root domain.

For LAN routing, bridge networking, firewall hardening, and AdGuard Home DNS setup, refer to [NETWORK.md](NETWORK.md).

---

## 1. Architecture Overview (Publicly Trusted Let's Encrypt vs. Private CA)

To eliminate browser security warnings across all devices on your LAN without installing a custom Root CA certificate on every phone and laptop, use **Let's Encrypt (`ECDSA`) certificates via ACME DNS-01 validation** paired with **Caddy** as your local HTTPS reverse proxy:

```
                  Domain Registrar / Public DNS (<your-domain>)
                  * @ / www               -> Public site (e.g. GitHub Pages, untouched)
                  * _acme-challenge.lab   -> CNAME -> <uuid>.auth.acme-dns.io
                                                     │
                                     DNS-01 TXT updates (acme.sh)
                                                     ▼
                                     Let's Encrypt Public ACME CA
                                                     │
                                  Wildcard Cert: lab.<your-domain>, *.lab.<your-domain>
                                                     ▼
  LAN Client ──(HTTPS :443)──► Caddy Reverse Proxy (<server-ip>)
   (Local DNS rewrite:         ├── lab.<your-domain>       ──► /var/www/lab (Static Portal)
    *.lab.<your-domain>        ├── ducklake.lab.<your-domain> ──► 127.0.0.1:5000 (DuckLake & AI)
    -> <server-ip>)            ├── adminer.lab.<your-domain>  ──► 127.0.0.1:8080 (Adminer UI)
                               └── adguard.lab.<your-domain>  ──► 127.0.0.1:3000 (AdGuard Home)
```

### Why `acme-dns` CNAME Delegation?
Some registrars (such as **Squarespace Domains**) do not provide a public DNS API for automated ACME `TXT` updates. Delegating `_acme-challenge.lab.<your-domain>` via a single static `CNAME` to an `acme-dns` instance (`auth.acme-dns.io`, or a self-hosted instance) lets `acme.sh` obtain and renew wildcard certificates automatically while leaving your root domain's DNS records untouched.

*(Alternative: If you use or delegate nameservers to a DNS provider with a native API such as Cloudflare, you can pass `--dns dns_cf` directly to `acme.sh` instead of `dns_acmedns`.)*

---

## 2. Step-by-Step Setup on Your Server

The steps below work on any Debian/Ubuntu host (`riscv64`, `aarch64`, or `x86_64`). Replace `<your-domain>`, `<user>`, and `<server-ip>` with your own values.

### Step 1: Install Caddy & `acme.sh`
```bash
# Install Caddy web server / reverse proxy and socat
sudo apt-get update
sudo apt-get install -y caddy socat curl

# Install acme.sh under your non-root user and default to Let's Encrypt
curl -fsSL https://get.acme.sh | sh -s email=admin@<your-domain>
~/.acme.sh/acme.sh --set-default-ca --server letsencrypt
```

### Step 2: Register an `acme-dns` Account
Request a dedicated `acme-dns` subdomain and API credential pair:
```bash
curl -fsSL -X POST https://auth.acme-dns.io/register > ~/.acme.sh/acmedns_lab.json
chmod 600 ~/.acme.sh/acmedns_lab.json
cat ~/.acme.sh/acmedns_lab.json
```

Note the `fulldomain`, `username`, `password`, and `subdomain` values returned by the JSON output.

### Step 3: Add the `CNAME` Delegation in Your Public DNS
In your registrar or DNS provider's dashboard for `<your-domain>` (leaving your existing `@` `A` records and `www` `CNAME` untouched):

| Host | Type | Target |
| :--- | :--- | :--- |
| `_acme-challenge.lab` | `CNAME` | `<fulldomain-from-step-2>` *(e.g. `<uuid>.auth.acme-dns.io`)* |

> [!NOTE]
> Even if your domain has a wildcard `CNAME` (`*` → external host), an explicit `_acme-challenge.lab` `CNAME` takes precedence solely for ACME validation.

Verify DNS propagation before issuing the certificate:
```bash
dig +short CNAME _acme-challenge.lab.<your-domain> @1.1.1.1
```

### Step 4: Prepare Certificate Storage & Issue the Wildcard Cert
Create a certificate directory readable by Caddy (`caddy` group) and writable by your ACME user:
```bash
sudo usermod -aG caddy "$USER"
sudo mkdir -p /etc/ssl/lab
sudo chown "$USER":caddy /etc/ssl/lab
sudo chmod 750 /etc/ssl/lab
```

Export your `acme-dns` credentials from Step 2 and issue the wildcard cert (covering both `lab.<your-domain>` and `*.lab.<your-domain>`):
```bash
export ACMEDNS_BASE_URL="https://auth.acme-dns.io"
export ACMEDNS_USERNAME="<username-from-step-2>"
export ACMEDNS_PASSWORD="<password-from-step-2>"
export ACMEDNS_SUBDOMAIN="<subdomain-from-step-2>"

~/.acme.sh/acme.sh --issue \
    --dns dns_acmedns \
    -d "lab.<your-domain>" \
    -d "*.lab.<your-domain>" \
    --server letsencrypt

~/.acme.sh/acme.sh --install-cert -d "lab.<your-domain>" \
    --fullchain-file /etc/ssl/lab/fullchain.pem \
    --key-file /etc/ssl/lab/privkey.pem \
    --reloadcmd "chmod 640 /etc/ssl/lab/privkey.pem && sudo chgrp caddy /etc/ssl/lab/privkey.pem && sudo systemctl reload-or-restart caddy"
```

`acme.sh` automatically saves these parameters and schedules a background cron job to renew the certificate before expiry and reload Caddy.

---

## 3. Caddy HTTPS Reverse Proxy Configuration

Configure `/etc/caddy/Caddyfile` to terminate HTTPS on port `443` (with automatic HTTP → HTTPS redirects on port `80`) and proxy each subdomain to its local service port:

```caddyfile
lab.<your-domain> {
	tls /etc/ssl/lab/fullchain.pem /etc/ssl/lab/privkey.pem
	root * /var/www/lab
	file_server
}

ducklake.lab.<your-domain> {
	tls /etc/ssl/lab/fullchain.pem /etc/ssl/lab/privkey.pem
	reverse_proxy 127.0.0.1:5000
}

adminer.lab.<your-domain> {
	tls /etc/ssl/lab/fullchain.pem /etc/ssl/lab/privkey.pem
	reverse_proxy 127.0.0.1:8080
}

adguard.lab.<your-domain> {
	tls /etc/ssl/lab/fullchain.pem /etc/ssl/lab/privkey.pem
	reverse_proxy 127.0.0.1:3000
}
```

Validate and start Caddy:
```bash
sudo caddy validate --config /etc/caddy/Caddyfile
sudo systemctl enable --now caddy
```

* **`X-Forwarded-For` Preservation:** Caddy forwards the original client IP to DuckLake (`127.0.0.1:5000`), allowing `app.py` (`trust_forwarded_for: true`) to enforce AI CIDR and **Allow Remote AI** policies accurately.
* **Static Landing Page (`/var/www/lab`):** Keeps `https://lab.<your-domain>` separate from the DuckLake application (`https://ducklake.lab.<your-domain>`). Replace `lab.<your-domain>` in DNS with a GitHub Pages `CNAME` whenever you want to host the portal externally.

---

## 4. Local DNS Resolution (`*.lab.<your-domain>`)

To route `*.lab.<your-domain>` requests to `<server-ip>` inside your LAN instead of the public internet:

1. **AdGuard Home (Recommended on RV2 / Home Server):**
   * Complete setup at `http://<server-ip>:3000/` (if Ubuntu's `systemd-resolved` occupies port `53`, set `DNSStubListener=no` in `/etc/systemd/resolved.conf` and restart `systemd-resolved`).
   * Under **Filters → DNS rewrites → Add DNS rewrite**, add:
     * **Domain:** `*.lab.<your-domain>` → **Answer:** `<server-ip>`
     * **Domain:** `lab.<your-domain>` → **Answer:** `<server-ip>`
2. **Alternative (Router DNS or `/etc/hosts`):**
   * Add host overrides on your LAN router/Pi-hole/dnsmasq, or map the subdomains to `<server-ip>` in client `/etc/hosts` files.

---

## 5. Operations & Verification Commands

```bash
# Check Caddy status and validate configuration:
sudo systemctl status caddy
sudo caddy validate --config /etc/caddy/Caddyfile

# List installed certificates and check cron schedule:
~/.acme.sh/acme.sh --list
crontab -l

# Force manual renewal test (if debugging):
~/.acme.sh/acme.sh --renew -d "lab.<your-domain>" --ecc --force

# Test trusted HTTPS from any LAN client (even before configuring local DNS):
curl -fsS --resolve "ducklake.lab.<your-domain>:443:<server-ip>" "https://ducklake.lab.<your-domain>/status"
curl -fsSI --resolve "lab.<your-domain>:443:<server-ip>" "https://lab.<your-domain>/"
```
