#!/usr/bin/env python3
"""
CertManager.py - Certificate Management, Caddy Routing & Secure SSL Key Distribution
====================================================================================
Manages wildcard & per-service TLS certificates via acme.sh (DNS-01 / acme-dns),
generates and validates Caddyfile routing for select home lab domains/services,
and provides audited, CIDR-gated, one-time / encrypted SSL key distribution.
"""

import base64
import hashlib
import hmac
import io
import ipaddress
import json
import logging
import os
import re
import secrets
import shlex
import shutil
import subprocess
import tarfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("CertManager")

_SUBDOMAIN_RE = re.compile(r"^(@|[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)$")
_DOMAIN_RE = re.compile(r"^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)+$")
_SAFE_TARGET_RE = re.compile(r"^[a-zA-Z0-9._:/-]+$")
_SSH_HOST_RE = re.compile(r"^[a-zA-Z0-9._-]+$")
_SSH_USER_RE = re.compile(r"^[a-z_][a-z0-9._-]{0,63}$")
_APP_NAME_RE = re.compile(r"^[a-zA-Z0-9._-]{1,64}$")
_SSL_PATH_RE = re.compile(r"^(/[a-zA-Z0-9._/-]+|~/?[a-zA-Z0-9._/-]*)$")


def default_cert_config() -> Dict[str, Any]:
    """Returns default certificate configuration dictionary."""
    home_acme = str(Path.home() / ".acme.sh")
    return {
        "enabled": True,
        "base_domain": "lab.example.com",
        "include_wildcard": True,
        "acme_provider": "dns_acmedns",
        "ca_server": "letsencrypt",
        "ssl_dir": "/etc/ssl/lab",
        "acme_home": home_acme,
        "acme_dns_conf": "",
        "caddyfile": "/etc/caddy/Caddyfile",
        "portal_root": "/var/www/lab",
        "allowed_cidrs": ["10.0.2.0/24", "127.0.0.1/32", "::1/128"],
        "services": [
            {"subdomain": "@", "type": "static", "target": "/var/www/lab", "enabled": True, "tls": True},
            {"subdomain": "ducklake", "type": "proxy", "target": "127.0.0.1:5000", "enabled": True, "tls": True},
            {"subdomain": "adminer", "type": "proxy", "target": "127.0.0.1:8080", "enabled": True, "tls": True},
            {"subdomain": "adguard", "type": "proxy", "target": "127.0.0.1:3000", "enabled": True, "tls": True},
        ],
    }


def init_cert_tables(get_pg_conn_fn):
    """Creates PostgreSQL tables for distribution tokens and audit logs."""
    try:
        with get_pg_conn_fn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS cert_distribution_tokens (
                        token_hash VARCHAR(64) PRIMARY KEY,
                        label VARCHAR(128) NOT NULL,
                        domains_json JSONB DEFAULT '[]'::jsonb,
                        encrypted BOOLEAN DEFAULT FALSE,
                        bundle_path TEXT DEFAULT '',
                        expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        max_uses INTEGER DEFAULT 1,
                        uses_count INTEGER DEFAULT 0,
                        created_by_ip VARCHAR(64) DEFAULT '',
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS cert_distribution_logs (
                        id SERIAL PRIMARY KEY,
                        action VARCHAR(64) NOT NULL,
                        client_ip VARCHAR(64) DEFAULT '',
                        target_label VARCHAR(128) DEFAULT '',
                        domains TEXT DEFAULT '',
                        success BOOLEAN DEFAULT TRUE,
                        detail TEXT DEFAULT '',
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS enrolled_apps (
                        id SERIAL PRIMARY KEY,
                        app_name VARCHAR(128) NOT NULL UNIQUE,
                        subdomain VARCHAR(128) NOT NULL,
                        ssh_host VARCHAR(255) NOT NULL,
                        ssh_port INTEGER DEFAULT 22,
                        ssh_user VARCHAR(64) DEFAULT 'orangepi',
                        app_type VARCHAR(32) DEFAULT 'flask',
                        remote_ssl_dir TEXT DEFAULT '/home/orangepi/ssl/lab',
                        app_port INTEGER DEFAULT 5000,
                        reload_cmd TEXT DEFAULT '',
                        auto_sync BOOLEAN DEFAULT TRUE,
                        last_deployed_at TIMESTAMP WITH TIME ZONE,
                        last_status VARCHAR(32) DEFAULT 'pending',
                        last_message TEXT DEFAULT '',
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                conn.commit()
    except Exception as e:
        logger.warning(f"Failed to initialize certificate tables: {e}")


class CertManager:
    """Manages lab certificates, Caddy reverse proxy rules, and key distribution."""

    def __init__(self, config: Dict[str, Any], get_pg_conn_fn):
        self.config = config
        self.get_pg_connection = get_pg_conn_fn
        self.bundle_dir = Path(config.get("storage", {}).get("db_cache_dir", "/cluster/database")) / "cert_bundles"
        os.makedirs(self.bundle_dir, exist_ok=True)
        try:
            os.chmod(self.bundle_dir, 0o700)
        except Exception:
            pass

    def _cert_cfg(self) -> Dict[str, Any]:
        defaults = default_cert_config()
        merged = dict(defaults)
        merged.update(self.config.get("certificates", {}) or {})
        return merged

    def _log_audit(self, action: str, client_ip: str, label: str, domains: str, success: bool, detail: str = ""):
        try:
            with self.get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO cert_distribution_logs (action, client_ip, target_label, domains, success, detail)
                        VALUES (%s, %s, %s, %s, %s, %s);
                        """,
                        (action[:64], client_ip[:64], label[:128], domains[:500], bool(success), detail[:1000]),
                    )
                    conn.commit()
        except Exception as e:
            logger.debug(f"Audit log error: {e}")

    def is_ip_allowed_for_keys(self, client_ip: str) -> bool:
        """Checks if client_ip is inside certificates.allowed_cidrs."""
        allowed_cidrs = self._cert_cfg().get("allowed_cidrs", ["10.0.2.0/24", "127.0.0.1/32", "::1/128"])
        try:
            ip = ipaddress.ip_address(client_ip)
            if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped:
                ip = ip.ipv4_mapped
            for cidr in allowed_cidrs:
                net = ipaddress.ip_network(str(cidr).strip(), strict=False)
                if ip in net:
                    return True
        except Exception as e:
            logger.warning(f"Cert CIDR check error for {client_ip}: {e}")
        return False

    def _resolve_acme_dns_conf(self, cfg: Dict[str, Any]) -> Optional[Path]:
        """Locates acme-dns JSON credentials without exposing secrets."""
        explicit = (cfg.get("acme_dns_conf") or "").strip()
        if explicit and Path(explicit).is_file():
            return Path(explicit)
        acme_home = Path(cfg.get("acme_home") or (Path.home() / ".acme.sh"))
        if acme_home.is_dir():
            for candidate in sorted(acme_home.glob("acmedns*.json")):
                if candidate.is_file():
                    return candidate
        return None

    def _read_acme_dns_meta(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        """Reads only safe public CNAME metadata from acme-dns config."""
        conf_path = self._resolve_acme_dns_conf(cfg)
        if not conf_path:
            return {"configured": False, "fulldomain": None, "conf_path": None}
        try:
            data = json.loads(conf_path.read_text(encoding="utf-8"))
            return {
                "configured": True,
                "fulldomain": data.get("fulldomain"),
                "conf_path": str(conf_path),
            }
        except Exception:
            return {"configured": False, "fulldomain": None, "conf_path": str(conf_path)}

    def _inspect_x509(self, cert_path: Path) -> Dict[str, Any]:
        """Parses X.509 certificate metadata via openssl."""
        if not cert_path.is_file():
            return {"exists": False}
        try:
            res = subprocess.run(
                [
                    "openssl",
                    "x509",
                    "-in",
                    str(cert_path),
                    "-noout",
                    "-subject",
                    "-issuer",
                    "-dates",
                    "-fingerprint",
                    "-sha256",
                    "-ext",
                    "subjectAltName",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=5,
            )
            if res.returncode != 0:
                return {"exists": True, "error": res.stderr.strip()}

            info: Dict[str, Any] = {"exists": True, "sans": []}
            lines = res.stdout.splitlines()
            for idx, line in enumerate(lines):
                s = line.strip()
                if s.startswith("subject="):
                    info["subject"] = s.split("=", 1)[1].strip()
                elif s.startswith("issuer="):
                    info["issuer"] = s.split("=", 1)[1].strip()
                elif s.startswith("notBefore="):
                    info["not_before"] = s.split("=", 1)[1].strip()
                elif s.startswith("notAfter="):
                    info["not_after"] = s.split("=", 1)[1].strip()
                elif "Fingerprint=" in s:
                    info["sha256_fingerprint"] = s.split("=", 1)[1].strip()
                elif "DNS:" in s:
                    for part in s.split(","):
                        p = part.strip()
                        if p.startswith("DNS:"):
                            info["sans"].append(p[4:])
            return info
        except Exception as e:
            return {"exists": True, "error": str(e)}

    def _service_fqdn(self, base_domain: str, subdomain: str) -> str:
        if subdomain == "@":
            return base_domain
        return f"{subdomain}.{base_domain}"

    def _validate_cert_config(self, cfg: Dict[str, Any]) -> Tuple[bool, str]:
        base_domain = (cfg.get("base_domain") or "").strip().lower()
        if not _DOMAIN_RE.match(base_domain):
            return False, f"Invalid base_domain '{base_domain}'"

        services = cfg.get("services") or []
        if not isinstance(services, list):
            return False, "'services' must be a list"

        for svc in services:
            if not isinstance(svc, dict):
                return False, "Each service must be an object"
            sub = str(svc.get("subdomain", "")).strip().lower()
            stype = str(svc.get("type", "proxy")).strip().lower()
            target = str(svc.get("target", "")).strip()
            if not _SUBDOMAIN_RE.match(sub):
                return False, f"Invalid subdomain '{sub}'"
            if stype not in {"static", "proxy", "cert-only"}:
                return False, f"Invalid service type '{stype}' (use static, proxy, or cert-only)"
            if stype in {"static", "proxy"} and (not target or not _SAFE_TARGET_RE.match(target)):
                return False, f"Invalid target '{target}' for subdomain '{sub}'"

        return True, ""

    def get_desired_domains(self, cfg: Optional[Dict[str, Any]] = None) -> List[str]:
        """Computes the ordered list of SAN domains for the certificate."""
        c = cfg or self._cert_cfg()
        base = (c.get("base_domain") or "").strip().lower()
        domains: List[str] = []
        if base:
            domains.append(base)
            if c.get("include_wildcard", True):
                domains.append(f"*.{base}")
        for svc in c.get("services") or []:
            if not svc.get("enabled", True) or not svc.get("tls", True):
                continue
            sub = str(svc.get("subdomain", "")).strip().lower()
            fqdn = self._service_fqdn(base, sub) if base else sub
            # Skip redundant explicit entries already covered by wildcard unless wildcard disabled
            if c.get("include_wildcard", True) and sub != "@" and "." not in sub:
                continue
            if fqdn and fqdn not in domains:
                domains.append(fqdn)
        return domains

    def get_status(self) -> Dict[str, Any]:
        """Returns sanitized certificate, Caddy, acme-dns, token, and audit status."""
        cfg = self._cert_cfg()
        ssl_dir = Path(cfg.get("ssl_dir", "/etc/ssl/lab"))
        fullchain_path = ssl_dir / "fullchain.pem"
        privkey_path = ssl_dir / "privkey.pem"

        cert_info = self._inspect_x509(fullchain_path)
        cert_info["fullchain_path"] = str(fullchain_path)
        cert_info["privkey_present"] = privkey_path.is_file()

        caddy_active = False
        try:
            res = subprocess.run(["systemctl", "is-active", "caddy"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=3)
            caddy_active = res.stdout.strip() == "active"
        except Exception:
            pass

        self._cleanup_expired_tokens()

        tokens: List[Dict[str, Any]] = []
        audit_logs: List[Dict[str, Any]] = []
        try:
            with self.get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT LEFT(token_hash, 16), token_hash, label, domains_json, encrypted, expires_at, max_uses, uses_count, created_at
                        FROM cert_distribution_tokens
                        WHERE expires_at > CURRENT_TIMESTAMP AND uses_count < max_uses
                        ORDER BY created_at DESC LIMIT 25;
                    """)
                    for r in cur.fetchall():
                        tokens.append({
                            "id": r[0],
                            "token_hash": r[1],
                            "label": r[2],
                            "domains": r[3] or [],
                            "encrypted": bool(r[4]),
                            "expires_at": r[5].isoformat() if r[5] else None,
                            "max_uses": r[6],
                            "uses_count": r[7],
                            "created_at": r[8].isoformat() if r[8] else None,
                        })

                    cur.execute("""
                        SELECT id, action, client_ip, target_label, domains, success, detail, created_at
                        FROM cert_distribution_logs
                        ORDER BY id DESC LIMIT 25;
                    """)
                    for r in cur.fetchall():
                        audit_logs.append({
                            "id": r[0],
                            "action": r[1],
                            "client_ip": r[2],
                            "target_label": r[3],
                            "domains": r[4],
                            "success": bool(r[5]),
                            "detail": r[6],
                            "created_at": r[7].isoformat() if r[7] else None,
                        })
        except Exception as e:
            logger.debug(f"Failed loading cert tokens/logs: {e}")

        base_domain = (cfg.get("base_domain") or "").strip().lower()
        acme_meta = self._read_acme_dns_meta(cfg)
        challenge_host = f"_acme-challenge.{base_domain}" if base_domain else ""

        return {
            "status": "success",
            "config": {
                "enabled": bool(cfg.get("enabled", True)),
                "base_domain": base_domain,
                "include_wildcard": bool(cfg.get("include_wildcard", True)),
                "acme_provider": cfg.get("acme_provider", "dns_acmedns"),
                "ca_server": cfg.get("ca_server", "letsencrypt"),
                "ssl_dir": str(ssl_dir),
                "portal_root": cfg.get("portal_root", "/var/www/lab"),
                "allowed_cidrs": cfg.get("allowed_cidrs", []),
                "services": cfg.get("services", []),
            },
            "desired_domains": self.get_desired_domains(cfg),
            "certificate": cert_info,
            "acme_dns": {
                "configured": acme_meta.get("configured", False),
                "cname_host": challenge_host,
                "cname_target": acme_meta.get("fulldomain"),
            },
            "caddy_active": caddy_active,
            "bearer_token_configured": bool(os.getenv("CERT_DISTRIBUTION_TOKEN", "").strip()),
            "ssh_public_key": self.get_ssh_public_key(),
            "enrolled_apps": self.list_enrolled_apps(),
            "active_tokens": tokens,
            "audit_logs": audit_logs,
        }

    def check_dns_delegation(self) -> Dict[str, Any]:
        """Checks public DNS propagation for _acme-challenge.<base_domain>."""
        cfg = self._cert_cfg()
        base_domain = (cfg.get("base_domain") or "").strip().lower()
        if not base_domain:
            return {"status": "error", "error": "base_domain is not configured"}

        challenge_host = f"_acme-challenge.{base_domain}"
        acme_meta = self._read_acme_dns_meta(cfg)
        expected = (acme_meta.get("fulldomain") or "").strip().rstrip(".")

        found = ""
        try:
            res = subprocess.run(
                ["dig", "+short", "CNAME", challenge_host, "@1.1.1.1"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=8,
            )
            found = res.stdout.strip().splitlines()[0].rstrip(".") if res.stdout.strip() else ""
        except Exception as e:
            return {"status": "error", "error": f"DNS lookup failed: {e}"}

        matched = bool(expected and found.lower() == expected.lower())
        return {
            "status": "success",
            "challenge_host": challenge_host,
            "expected_cname": expected,
            "resolved_cname": found or None,
            "ready": matched,
        }

    def render_caddyfile(self, cfg: Optional[Dict[str, Any]] = None) -> str:
        """Generates Caddyfile contents from configured lab services."""
        c = cfg or self._cert_cfg()
        base = (c.get("base_domain") or "").strip().lower()
        ssl_dir = Path(c.get("ssl_dir", "/etc/ssl/lab"))
        fullchain = ssl_dir / "fullchain.pem"
        privkey = ssl_dir / "privkey.pem"
        has_cert = fullchain.is_file() and privkey.is_file()

        lines = [
            f"# Auto-generated by DuckLake CertManager for *.{base}",
            "# Do not edit manually; configure via Settings -> Certificates & Key Distribution",
            "",
        ]

        for svc in c.get("services") or []:
            if not svc.get("enabled", True):
                continue
            sub = str(svc.get("subdomain", "")).strip().lower()
            stype = str(svc.get("type", "proxy")).strip().lower()
            target = str(svc.get("target", "")).strip()
            if stype == "cert-only":
                continue

            fqdn = self._service_fqdn(base, sub)
            site_addr = fqdn if (has_cert and svc.get("tls", True)) else f"http://{fqdn}"
            lines.append(f"{site_addr} {{")
            if has_cert and svc.get("tls", True):
                lines.append(f"\ttls {fullchain} {privkey}")
            if stype == "static":
                lines.append(f"\troot * {target}")
                lines.append("\tfile_server")
            else:
                lines.append(f"\treverse_proxy {target}")
            lines.append("}")
            lines.append("")

        return "\n".join(lines)

    def apply_caddy_config(self) -> Tuple[Dict[str, Any], int]:
        """Writes Caddyfile, validates syntax, and reloads Caddy."""
        cfg = self._cert_cfg()
        ok, err = self._validate_cert_config(cfg)
        if not ok:
            return {"status": "error", "error": err}, 400

        caddyfile_path = cfg.get("caddyfile", "/etc/caddy/Caddyfile")
        content = self.render_caddyfile(cfg)

        try:
            proc = subprocess.run(
                ["sudo", "tee", caddyfile_path],
                input=content,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                timeout=10,
            )
            if proc.returncode != 0:
                return {"status": "error", "error": f"Failed writing {caddyfile_path}: {proc.stderr.strip()}"}, 500

            val = subprocess.run(
                ["sudo", "caddy", "validate", "--config", caddyfile_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=10,
            )
            if val.returncode != 0:
                return {"status": "error", "error": f"Caddyfile validation failed: {val.stderr.strip()}"}, 400

            rel = subprocess.run(
                ["sudo", "systemctl", "reload-or-restart", "caddy"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=15,
            )
            if rel.returncode != 0:
                return {"status": "error", "error": f"Failed reloading caddy: {rel.stderr.strip()}"}, 500

            return {"status": "success", "message": "Caddyfile updated, validated, and Caddy reloaded.", "caddyfile": content}, 200
        except Exception as e:
            return {"status": "error", "error": str(e)}, 500

    def generate_certificate(self, force: bool = False, client_ip: str = "127.0.0.1") -> Tuple[Dict[str, Any], int]:
        """Issues or renews TLS certificate via acme.sh and installs it for Caddy."""
        cfg = self._cert_cfg()
        ok, err = self._validate_cert_config(cfg)
        if not ok:
            return {"status": "error", "error": err}, 400

        base_domain = cfg["base_domain"].strip().lower()
        domains = self.get_desired_domains(cfg)
        if not domains:
            return {"status": "error", "error": "No domains configured for certificate generation"}, 400

        acme_home = Path(cfg.get("acme_home") or (Path.home() / ".acme.sh"))
        acme_bin = acme_home / "acme.sh"
        if not acme_bin.is_file():
            return {"status": "error", "error": f"acme.sh not found at {acme_bin}. Run sudo ./setup_ca.sh first."}, 400

        provider = cfg.get("acme_provider", "dns_acmedns")
        ca_server = cfg.get("ca_server", "letsencrypt")
        ssl_dir = Path(cfg.get("ssl_dir", "/etc/ssl/lab"))
        os.makedirs(ssl_dir, exist_ok=True)

        env = os.environ.copy()
        if provider == "dns_acmedns":
            conf_path = self._resolve_acme_dns_conf(cfg)
            if not conf_path:
                return {"status": "error", "error": "acme-dns credentials JSON not found. Run sudo ./setup_ca.sh first."}, 400
            dns_check = self.check_dns_delegation()
            if not dns_check.get("ready"):
                self._log_audit("issue", client_ip, "acme.sh", ",".join(domains), False, "CNAME delegation not resolved")
                return {
                    "status": "error",
                    "error": (
                        f"DNS CNAME not ready: {dns_check.get('challenge_host')} should point to "
                        f"{dns_check.get('expected_cname')} (resolved: {dns_check.get('resolved_cname') or 'none'})"
                    ),
                }, 400
            creds = json.loads(conf_path.read_text(encoding="utf-8"))
            env["ACMEDNS_BASE_URL"] = "https://auth.acme-dns.io"
            env["ACMEDNS_USERNAME"] = creds["username"]
            env["ACMEDNS_PASSWORD"] = creds["password"]
            env["ACMEDNS_SUBDOMAIN"] = creds["subdomain"]

        issue_cmd = [str(acme_bin), "--home", str(acme_home), "--issue", "--dns", provider, "--server", ca_server]
        for dom in domains:
            issue_cmd.extend(["-d", dom])
        if force:
            issue_cmd.append("--force")

        try:
            proc = subprocess.run(issue_cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=180)
            # Exit code 2 from acme.sh means cert already valid and not due for renewal
            if proc.returncode not in (0, 2):
                self._log_audit("issue", client_ip, "acme.sh", ",".join(domains), False, proc.stdout[-500:])
                return {"status": "error", "error": "Certificate issuance failed", "output": proc.stdout[-2000:]}, 500

            fullchain_out = ssl_dir / "fullchain.pem"
            privkey_out = ssl_dir / "privkey.pem"
            reload_cmd = f"chmod 640 {privkey_out} && sudo chgrp caddy {privkey_out} && sudo systemctl reload-or-restart caddy"

            inst_cmd = [
                str(acme_bin),
                "--home",
                str(acme_home),
                "--install-cert",
                "-d",
                base_domain,
                "--fullchain-file",
                str(fullchain_out),
                "--key-file",
                str(privkey_out),
                "--reloadcmd",
                reload_cmd,
            ]
            inst = subprocess.run(inst_cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=60)
            if inst.returncode != 0:
                self._log_audit("issue", client_ip, "acme.sh", ",".join(domains), False, inst.stdout[-500:])
                return {"status": "error", "error": "Certificate install failed", "output": inst.stdout[-2000:]}, 500

            caddy_res, caddy_code = self.apply_caddy_config()
            if caddy_code != 200:
                self._log_audit("issue", client_ip, "acme.sh", ",".join(domains), False, caddy_res.get("error", ""))
                return caddy_res, caddy_code

            msg = "Certificate already valid (skipped renewal) and Caddy synced." if proc.returncode == 2 else "Certificate issued/renewed and installed successfully."
            self._log_audit("issue", client_ip, "acme.sh", ",".join(domains), True, msg)
            sync_res = self.deploy_to_all_apps(client_ip=client_ip)
            return {
                "status": "success",
                "message": msg,
                "domains": domains,
                "certificate": self._inspect_x509(fullchain_out),
                "enrolled_sync": sync_res,
            }, 200
        except Exception as e:
            self._log_audit("issue", client_ip, "acme.sh", ",".join(domains), False, str(e))
            return {"status": "error", "error": str(e)}, 500

    def _build_tar_bytes(self) -> Tuple[Optional[bytes], Optional[str]]:
        """Packages fullchain.pem, privkey.pem, and metadata.json into an in-memory tar.gz."""
        cfg = self._cert_cfg()
        ssl_dir = Path(cfg.get("ssl_dir", "/etc/ssl/lab"))
        fullchain = ssl_dir / "fullchain.pem"
        privkey = ssl_dir / "privkey.pem"
        if not fullchain.is_file() or not privkey.is_file():
            return None, "Certificate files (fullchain.pem / privkey.pem) do not exist yet."

        cert_info = self._inspect_x509(fullchain)
        meta = {
            "base_domain": cfg.get("base_domain"),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "certificate": cert_info,
        }

        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tf:
            for arcname, src_path, mode in (
                ("fullchain.pem", fullchain, 0o644),
                ("privkey.pem", privkey, 0o600),
            ):
                data = src_path.read_bytes()
                ti = tarfile.TarInfo(name=arcname)
                ti.size = len(data)
                ti.mode = mode
                ti.mtime = int(time.time())
                tf.addfile(ti, io.BytesIO(data))

            meta_bytes = json.dumps(meta, indent=2).encode("utf-8")
            ti_m = tarfile.TarInfo(name="metadata.json")
            ti_m.size = len(meta_bytes)
            ti_m.mode = 0o644
            ti_m.mtime = int(time.time())
            tf.addfile(ti_m, io.BytesIO(meta_bytes))

        return buf.getvalue(), None

    def _cleanup_expired_tokens(self):
        """Removes expired or exhausted tokens and any associated encrypted bundle files."""
        try:
            with self.get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT token_hash, bundle_path FROM cert_distribution_tokens
                        WHERE expires_at <= CURRENT_TIMESTAMP OR uses_count >= max_uses;
                    """)
                    rows = cur.fetchall()
                    for _, bpath in rows:
                        if bpath and Path(bpath).is_file():
                            try:
                                Path(bpath).unlink()
                            except Exception:
                                pass
                    if rows:
                        cur.execute("""
                            DELETE FROM cert_distribution_tokens
                            WHERE expires_at <= CURRENT_TIMESTAMP OR uses_count >= max_uses;
                        """)
                        conn.commit()
        except Exception as e:
            logger.debug(f"Token cleanup error: {e}")

    def create_distribution_token(
        self,
        label: str,
        ttl_seconds: int = 600,
        max_uses: int = 1,
        passphrase: str = "",
        client_ip: str = "127.0.0.1",
    ) -> Tuple[Dict[str, Any], int]:
        """Creates a short-lived, optional AES-256 encrypted key distribution token."""
        label = (label or "homelab-node").strip()[:120]
        ttl_seconds = max(60, min(int(ttl_seconds or 600), 86400))
        max_uses = max(1, min(int(max_uses or 1), 20))
        passphrase = (passphrase or "").strip()

        raw_tar, err = self._build_tar_bytes()
        if err:
            return {"status": "error", "error": err}, 400

        token = secrets.token_urlsafe(32)
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)

        encrypted = False
        bundle_path = ""
        if passphrase:
            encrypted = True
            target_file = self.bundle_dir / f"{token_hash}.tar.gz.enc"
            env_enc = os.environ.copy()
            env_enc["BUNDLE_PASSPHRASE"] = passphrase
            proc = subprocess.run(
                ["openssl", "enc", "-aes-256-cbc", "-pbkdf2", "-salt", "-pass", "env:BUNDLE_PASSPHRASE", "-out", str(target_file)],
                input=raw_tar,
                env=env_enc,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=15,
            )
            if proc.returncode != 0:
                return {"status": "error", "error": f"Encryption failed: {proc.stderr.decode(errors='ignore')}"}, 500
            os.chmod(target_file, 0o600)
            bundle_path = str(target_file)

        cfg = self._cert_cfg()
        domains = self.get_desired_domains(cfg)

        try:
            with self.get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO cert_distribution_tokens
                        (token_hash, label, domains_json, encrypted, bundle_path, expires_at, max_uses, created_by_ip)
                        VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s, %s);
                        """,
                        (token_hash, label, json.dumps(domains), encrypted, bundle_path, expires_at, max_uses, client_ip),
                    )
                    conn.commit()
        except Exception as e:
            if bundle_path and Path(bundle_path).is_file():
                Path(bundle_path).unlink()
            return {"status": "error", "error": f"Database error creating token: {e}"}, 500

        self._log_audit("token_created", client_ip, label, ",".join(domains), True, f"ttl={ttl_seconds}s, encrypted={encrypted}")
        return {
            "status": "success",
            "token": token,
            "token_hash": token_hash,
            "label": label,
            "encrypted": encrypted,
            "expires_at": expires_at.isoformat(),
            "max_uses": max_uses,
            "domains": domains,
        }, 200

    def revoke_token(self, token_hash: str, client_ip: str = "127.0.0.1") -> Tuple[Dict[str, Any], int]:
        """Revokes a distribution token immediately and removes its encrypted bundle."""
        try:
            with self.get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT bundle_path, label FROM cert_distribution_tokens WHERE token_hash = %s;", (token_hash,))
                    row = cur.fetchone()
                    if not row:
                        return {"status": "error", "error": "Token not found"}, 404
                    bpath, label = row
                    if bpath and Path(bpath).is_file():
                        Path(bpath).unlink()
                    cur.execute("DELETE FROM cert_distribution_tokens WHERE token_hash = %s;", (token_hash,))
                    conn.commit()
            self._log_audit("token_revoked", client_ip, label or "", "", True, "Revoked manually")
            return {"status": "success", "message": "Token revoked"}, 200
        except Exception as e:
            return {"status": "error", "error": str(e)}, 500

    def consume_bundle_token(self, token: str, client_ip: str) -> Tuple[Optional[bytes], Dict[str, Any], int]:
        """Validates and consumes a one-time distribution token, returning bundle bytes."""
        if not self.is_ip_allowed_for_keys(client_ip):
            self._log_audit("bundle_denied", client_ip, "token", "", False, "Client IP outside certificates.allowed_cidrs")
            return None, {"status": "error", "error": f"Access denied: {client_ip} is not in certificates.allowed_cidrs"}, 403

        self._cleanup_expired_tokens()
        token_hash = hashlib.sha256((token or "").encode("utf-8")).hexdigest()

        try:
            with self.get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT label, domains_json, encrypted, bundle_path, expires_at, max_uses, uses_count
                        FROM cert_distribution_tokens
                        WHERE token_hash = %s FOR UPDATE;
                        """,
                        (token_hash,),
                    )
                    row = cur.fetchone()
                    if not row:
                        self._log_audit("bundle_denied", client_ip, "invalid_token", "", False, "Invalid or expired token")
                        return None, {"status": "error", "error": "Invalid, expired, or already used distribution token"}, 404

                    label, domains_json, encrypted, bundle_path, expires_at, max_uses, uses_count = row
                    if uses_count >= max_uses or expires_at <= datetime.now(timezone.utc):
                        return None, {"status": "error", "error": "Distribution token expired or exhausted"}, 410

                    if encrypted:
                        bp = Path(bundle_path)
                        if not bp.is_file():
                            return None, {"status": "error", "error": "Encrypted bundle file missing"}, 404
                        payload = bp.read_bytes()
                    else:
                        payload, err = self._build_tar_bytes()
                        if err or payload is None:
                            return None, {"status": "error", "error": err or "Bundle build failed"}, 500

                    new_uses = uses_count + 1
                    if new_uses >= max_uses:
                        cur.execute("DELETE FROM cert_distribution_tokens WHERE token_hash = %s;", (token_hash,))
                        if encrypted and bundle_path and Path(bundle_path).is_file():
                            Path(bundle_path).unlink()
                    else:
                        cur.execute(
                            "UPDATE cert_distribution_tokens SET uses_count = %s WHERE token_hash = %s;",
                            (new_uses, token_hash),
                        )
                    conn.commit()

            domains_str = ",".join(domains_json or [])
            self._log_audit("bundle_downloaded", client_ip, label, domains_str, True, f"encrypted={encrypted}")
            return payload, {"encrypted": bool(encrypted), "label": label}, 200
        except Exception as e:
            return None, {"status": "error", "error": str(e)}, 500

    def pull_with_bearer(self, auth_header: str, client_ip: str, fmt: str = "targz") -> Tuple[Any, int]:
        """Allows automated cron pulls via CERT_DISTRIBUTION_TOKEN from allowed CIDRs."""
        if not self.is_ip_allowed_for_keys(client_ip):
            self._log_audit("bearer_denied", client_ip, "bearer_pull", "", False, "Client IP outside certificates.allowed_cidrs")
            return {"status": "error", "error": f"Access denied: {client_ip} is not in certificates.allowed_cidrs"}, 403

        expected = os.getenv("CERT_DISTRIBUTION_TOKEN", "").strip()
        if not expected:
            return {"status": "error", "error": "CERT_DISTRIBUTION_TOKEN is not configured in .env"}, 503

        provided = ""
        if auth_header and auth_header.lower().startswith("bearer "):
            provided = auth_header[7:].strip()

        if not provided or not hmac.compare_digest(provided, expected):
            self._log_audit("bearer_denied", client_ip, "bearer_pull", "", False, "Invalid Bearer token")
            return {"status": "error", "error": "Unauthorized: Invalid Bearer token"}, 401

        cfg = self._cert_cfg()
        domains_str = ",".join(self.get_desired_domains(cfg))

        if fmt == "json":
            ssl_dir = Path(cfg.get("ssl_dir", "/etc/ssl/lab"))
            fullchain = ssl_dir / "fullchain.pem"
            privkey = ssl_dir / "privkey.pem"
            if not fullchain.is_file() or not privkey.is_file():
                return {"status": "error", "error": "Certificate files not found"}, 404
            self._log_audit("bearer_pull", client_ip, "cron-node", domains_str, True, "format=json")
            return {
                "status": "success",
                "base_domain": cfg.get("base_domain"),
                "fullchain_pem": fullchain.read_text(encoding="utf-8"),
                "privkey_pem": privkey.read_text(encoding="utf-8"),
            }, 200

        payload, err = self._build_tar_bytes()
        if err or payload is None:
            return {"status": "error", "error": err or "Bundle build failed"}, 404

        self._log_audit("bearer_pull", client_ip, "cron-node", domains_str, True, "format=targz")
        return payload, 200

    # ==========================================================================
    # App Enrollment & Keyless SSH SSL Deployment
    # ==========================================================================
    def get_ssh_public_key(self) -> str:
        """Returns the host's public SSH key (for authorizing keyless SSH on app nodes)."""
        ssh_dir = Path.home() / ".ssh"
        for name in ("id_ed25519.pub", "id_rsa.pub", "id_ecdsa.pub"):
            p = ssh_dir / name
            if p.is_file():
                try:
                    return p.read_text(encoding="utf-8").strip()
                except Exception:
                    pass
        return ""

    def list_enrolled_apps(self) -> List[Dict[str, Any]]:
        """Lists all enrolled apps from PostgreSQL."""
        base_domain = (self._cert_cfg().get("base_domain") or "").strip().lower()
        apps: List[Dict[str, Any]] = []
        try:
            with self.get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT id, app_name, subdomain, ssh_host, ssh_port, ssh_user, app_type,
                               remote_ssl_dir, app_port, reload_cmd, auto_sync,
                               last_deployed_at, last_status, last_message, created_at
                        FROM enrolled_apps ORDER BY id ASC;
                    """)
                    for r in cur.fetchall():
                        sub = r[2]
                        fqdn = self._service_fqdn(base_domain, sub) if base_domain else sub
                        apps.append({
                            "id": r[0],
                            "app_name": r[1],
                            "subdomain": sub,
                            "fqdn": fqdn,
                            "ssh_host": r[3],
                            "ssh_port": r[4],
                            "ssh_user": r[5],
                            "app_type": r[6],
                            "remote_ssl_dir": r[7],
                            "app_port": r[8],
                            "reload_cmd": r[9] or "",
                            "auto_sync": bool(r[10]),
                            "last_deployed_at": r[11].isoformat() if r[11] else None,
                            "last_status": r[12] or "pending",
                            "last_message": r[13] or "",
                            "created_at": r[14].isoformat() if r[14] else None,
                        })
        except Exception as e:
            logger.debug(f"Failed listing enrolled apps: {e}")
        return apps

    def _get_enrolled_app(self, app_id: int) -> Optional[Dict[str, Any]]:
        for app in self.list_enrolled_apps():
            if app["id"] == int(app_id):
                return app
        return None

    def enroll_app(self, data: Dict[str, Any]) -> Tuple[Dict[str, Any], int]:
        """Creates or updates an enrolled application target."""
        app_id = data.get("id")
        app_name = str(data.get("app_name", "")).strip()
        subdomain = str(data.get("subdomain", "")).strip().lower()
        ssh_host = str(data.get("ssh_host", "")).strip()
        ssh_port = int(data.get("ssh_port", 22) or 22)
        ssh_user = str(data.get("ssh_user", "orangepi")).strip()
        app_type = str(data.get("app_type", "flask")).strip().lower()
        remote_ssl_dir = str(data.get("remote_ssl_dir", f"/home/{ssh_user}/ssl/lab")).strip()
        app_port = int(data.get("app_port", 5000) or 5000)
        reload_cmd = str(data.get("reload_cmd", "")).strip()
        auto_sync = bool(data.get("auto_sync", True))

        if not _APP_NAME_RE.match(app_name):
            return {"status": "error", "error": "Invalid app_name (use letters, digits, ., _, -)"}, 400
        if not _SUBDOMAIN_RE.match(subdomain):
            return {"status": "error", "error": "Invalid subdomain (e.g. 'app', 'api', or '@')"}, 400
        if not _SSH_HOST_RE.match(ssh_host):
            return {"status": "error", "error": "Invalid ssh_host (hostname or IP)"}, 400
        if not (1 <= ssh_port <= 65535):
            return {"status": "error", "error": "ssh_port must be 1..65535"}, 400
        if not _SSH_USER_RE.match(ssh_user):
            return {"status": "error", "error": "Invalid ssh_user"}, 400
        if app_type not in {"flask", "gunicorn", "uvicorn", "nginx", "caddy", "custom"}:
            return {"status": "error", "error": "Unsupported app_type"}, 400
        if not _SSL_PATH_RE.match(remote_ssl_dir):
            return {"status": "error", "error": "Invalid remote_ssl_dir path"}, 400

        try:
            with self.get_pg_connection() as conn:
                with conn.cursor() as cur:
                    if app_id:
                        cur.execute(
                            """
                            UPDATE enrolled_apps
                            SET app_name=%s, subdomain=%s, ssh_host=%s, ssh_port=%s, ssh_user=%s,
                                app_type=%s, remote_ssl_dir=%s, app_port=%s, reload_cmd=%s, auto_sync=%s
                            WHERE id=%s RETURNING id;
                            """,
                            (app_name, subdomain, ssh_host, ssh_port, ssh_user, app_type, remote_ssl_dir, app_port, reload_cmd, auto_sync, int(app_id)),
                        )
                        row = cur.fetchone()
                        if not row:
                            return {"status": "error", "error": f"Enrolled app ID {app_id} not found"}, 404
                        target_id = row[0]
                    else:
                        cur.execute(
                            """
                            INSERT INTO enrolled_apps
                            (app_name, subdomain, ssh_host, ssh_port, ssh_user, app_type, remote_ssl_dir, app_port, reload_cmd, auto_sync)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (app_name) DO UPDATE SET
                                subdomain=EXCLUDED.subdomain, ssh_host=EXCLUDED.ssh_host, ssh_port=EXCLUDED.ssh_port,
                                ssh_user=EXCLUDED.ssh_user, app_type=EXCLUDED.app_type, remote_ssl_dir=EXCLUDED.remote_ssl_dir,
                                app_port=EXCLUDED.app_port, reload_cmd=EXCLUDED.reload_cmd, auto_sync=EXCLUDED.auto_sync
                            RETURNING id;
                            """,
                            (app_name, subdomain, ssh_host, ssh_port, ssh_user, app_type, remote_ssl_dir, app_port, reload_cmd, auto_sync),
                        )
                        target_id = cur.fetchone()[0]
                    conn.commit()
            return {"status": "success", "app": self._get_enrolled_app(target_id)}, 200
        except Exception as e:
            return {"status": "error", "error": str(e)}, 500

    def delete_enrolled_app(self, app_id: int) -> Tuple[Dict[str, Any], int]:
        """Removes an enrolled app registration."""
        try:
            with self.get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM enrolled_apps WHERE id = %s RETURNING app_name;", (int(app_id),))
                    row = cur.fetchone()
                    conn.commit()
                    if not row:
                        return {"status": "error", "error": "App not found"}, 404
            return {"status": "success", "deleted": row[0]}, 200
        except Exception as e:
            return {"status": "error", "error": str(e)}, 500

    def _run_ssh_script(self, app: Dict[str, Any], script_body: str, timeout: int = 20) -> Tuple[bool, str]:
        """Executes a script over keyless SSH (BatchMode=yes) via stdin."""
        cmd = [
            "ssh",
            "-o", "BatchMode=yes",
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ConnectTimeout=6",
            "-p", str(app["ssh_port"]),
            f"{app['ssh_user']}@{app['ssh_host']}",
            "bash -s",
        ]
        try:
            proc = subprocess.run(cmd, input=script_body, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=timeout)
            output = (proc.stdout or "").strip()
            return proc.returncode == 0, output
        except subprocess.TimeoutExpired:
            return False, "SSH connection timed out"
        except Exception as e:
            return False, str(e)

    def test_app_ssh(self, app_id: int) -> Tuple[Dict[str, Any], int]:
        """Tests keyless SSH login and remote SSL directory writeability."""
        app = self._get_enrolled_app(app_id)
        if not app:
            return {"status": "error", "error": "Enrolled app not found"}, 404

        ssl_dir_expr = app["remote_ssl_dir"]
        if ssl_dir_expr.startswith("~/"):
            dir_assign = f'TARGET_DIR="$HOME/{shlex.quote(ssl_dir_expr[2:])}"'
        elif ssl_dir_expr == "~":
            dir_assign = 'TARGET_DIR="$HOME"'
        else:
            dir_assign = f"TARGET_DIR={shlex.quote(ssl_dir_expr)}"

        script = f"""set -e
{dir_assign}
mkdir -p "$TARGET_DIR" 2>/dev/null || sudo -n mkdir -p "$TARGET_DIR"
if [ ! -w "$TARGET_DIR" ]; then
    sudo -n chown "$(id -un)" "$TARGET_DIR"
fi
test -w "$TARGET_DIR"
echo "SSH_OK user=$(id -un) host=$(hostname) dir=$TARGET_DIR"
"""
        ok, out = self._run_ssh_script(app, script, timeout=15)
        code = 200 if ok else 400
        return {"status": "success" if ok else "error", "reachable": ok, "output": out, "app": app}, code

    def deploy_to_app(self, app_id: int, client_ip: str = "127.0.0.1") -> Tuple[Dict[str, Any], int]:
        """Pushes fullchain.pem + privkey.pem atomically over keyless SSH and runs reload_cmd."""
        app = self._get_enrolled_app(app_id)
        if not app:
            return {"status": "error", "error": "Enrolled app not found"}, 404

        cfg = self._cert_cfg()
        ssl_dir = Path(cfg.get("ssl_dir", "/etc/ssl/lab"))
        fullchain = ssl_dir / "fullchain.pem"
        privkey = ssl_dir / "privkey.pem"
        if not fullchain.is_file() or not privkey.is_file():
            return {"status": "error", "error": "Server certificate files do not exist yet. Generate a certificate first."}, 400

        cert_b64 = base64.b64encode(fullchain.read_bytes()).decode("ascii")
        key_b64 = base64.b64encode(privkey.read_bytes()).decode("ascii")

        ssl_dir_expr = app["remote_ssl_dir"]
        if ssl_dir_expr.startswith("~/"):
            dir_assign = f'TARGET_DIR="$HOME/{shlex.quote(ssl_dir_expr[2:])}"'
        elif ssl_dir_expr == "~":
            dir_assign = 'TARGET_DIR="$HOME"'
        else:
            dir_assign = f"TARGET_DIR={shlex.quote(ssl_dir_expr)}"

        reload_section = ""
        if app["reload_cmd"]:
            reload_section = f"\n{app['reload_cmd']}\n"

        script = f"""set -e
{dir_assign}
mkdir -p "$TARGET_DIR" 2>/dev/null || sudo -n mkdir -p "$TARGET_DIR"
if [ ! -w "$TARGET_DIR" ]; then
    sudo -n chown "$(id -un)" "$TARGET_DIR"
fi
umask 077
printf '%s' {shlex.quote(key_b64)} | base64 -d > "${{TARGET_DIR}}/privkey.pem.tmp"
mv -f "${{TARGET_DIR}}/privkey.pem.tmp" "${{TARGET_DIR}}/privkey.pem"
chmod 600 "${{TARGET_DIR}}/privkey.pem"
umask 022
printf '%s' {shlex.quote(cert_b64)} | base64 -d > "${{TARGET_DIR}}/fullchain.pem.tmp"
mv -f "${{TARGET_DIR}}/fullchain.pem.tmp" "${{TARGET_DIR}}/fullchain.pem"
chmod 644 "${{TARGET_DIR}}/fullchain.pem"
{reload_section}echo "DEPLOY_OK dir=$TARGET_DIR"
"""
        ok, out = self._run_ssh_script(app, script, timeout=30)
        status_str = "deployed" if ok else "failed"
        msg = out[-500:] if out else ("Deployed successfully" if ok else "SSH deployment failed")

        try:
            with self.get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE enrolled_apps
                        SET last_deployed_at = CURRENT_TIMESTAMP, last_status = %s, last_message = %s
                        WHERE id = %s;
                        """,
                        (status_str, msg, int(app_id)),
                    )
                    conn.commit()
        except Exception as e:
            logger.debug(f"Failed updating app sync status: {e}")

        self._log_audit("ssh_deploy", client_ip, f"{app['app_name']} ({app['ssh_user']}@{app['ssh_host']})", app["fqdn"], ok, msg)
        code = 200 if ok else 500
        return {
            "status": "success" if ok else "error",
            "message": msg,
            "app": self._get_enrolled_app(app_id),
        }, code

    def deploy_to_all_apps(self, client_ip: str = "127.0.0.1") -> Dict[str, Any]:
        """Deploys SSL certificates to all auto_sync=TRUE enrolled apps."""
        apps = [a for a in self.list_enrolled_apps() if a.get("auto_sync")]
        results = []
        succeeded = 0
        for a in apps:
            res, code = self.deploy_to_app(a["id"], client_ip=client_ip)
            ok = code == 200
            if ok:
                succeeded += 1
            results.append({
                "id": a["id"],
                "app_name": a["app_name"],
                "fqdn": a["fqdn"],
                "success": ok,
                "message": res.get("message"),
            })
        return {
            "total": len(apps),
            "succeeded": succeeded,
            "failed": len(apps) - succeeded,
            "results": results,
        }
