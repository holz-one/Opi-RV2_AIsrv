# Orange Pi RV2 Network, Routing, Firewall & AdGuard Home Guide

This document details the network architecture, dual-Ethernet bridging, IP forwarding, NAT masquerading, firewall security rules, and AdGuard Home DNS deployment on the Orange Pi RV2 (`riscv64`, 4 GB RAM).

For PostgreSQL database administration, storage, and `pgvector` usage, refer to [POSTGRES.md](file:///home/kholz/Projects/Opi-RV2_AIsrv/POSTGRES.md).

---

## 1. Network Architecture & Topology

The Orange Pi RV2 functions as an edge compute node and router/gateway for connected cluster devices, with hardware-level network isolation between the external Wi-Fi WAN uplink and the internal dual-Ethernet subnet.

```
                      +---------------------------------------+
                      |           Orange Pi RV2               |
                      |                                       |
Internet <--- wlan0 --+-- NAT Masquerade (iptables)           |
 (192.168.18.15)      |   * Port 5432: DISABLED on network    |
                      |     (DuckLake Unix socket / lo only)  |
                      |   * Port 5000: AI & DuckLake (open)   |
                      |   * Port 8080: Adminer Web UI (open)  |
                      |   * Port 3000: AdGuard Home (open)    |
                      |   * Port 53:   AdGuard DNS Resolver   |
                      |                                       |
                      |   br0 Bridge (10.0.2.1/24)            |
                      |   +-------------------+               |
                      |   | end0 (Gigabit 1)  |               |
                      |   | end1 (Gigabit 2)  |               |
                      |   +---------+---------+               |
                      +-------------|-------------------------+
                                    |
                    Wired Clients (10.0.2.x)
                    * DuckLake API & Local AI Access
                    * Routed to Internet via wlan0 NAT
```

### Network Interfaces

| Interface | Type | IP Address / Subnet | Role & Description |
| :--- | :--- | :--- | :--- |
| **`wlan0`** | Wi-Fi 6 (Station) | `192.168.18.15/24` (DHCP) | Primary uplink to upstream router & Internet access |
| **`br0`** | Software Bridge | `10.0.2.1/24` (Static) | Local high-speed private cluster subnet |
| **`end0`** | Physical Gigabit PHY | Slave to `br0` | Ethernet Port 1 for cluster nodes or laptops |
| **`end1`** | Physical Gigabit PHY | Slave to `br0` | Ethernet Port 2 for cluster nodes or switches |
| **`lo`** | Loopback | `127.0.0.1/8` | Internal IPC, Adminer local socket, and local queries |

---

## 2. Dual-Ethernet Hardware Bridge (`br0`)

The board features dual Gigabit Ethernet controllers (`end0` and `end1`). To provide a single unified local subnet (`10.0.2.0/24`) without needing an external network switch, both physical interfaces are bridged together under `br0`.

### Configuration via NetworkManager

The bridge is managed via NetworkManager connections:

```bash
# 1. Create the bridge interface with static IP:
sudo nmcli con add type bridge con-name br0 ifname br0 ip4 10.0.2.1/24

# 2. Attach physical Gigabit ports end0 and end1 as slave members:
sudo nmcli con add type ethernet con-name br0-end0 ifname end0 master br0
sudo nmcli con add type ethernet con-name br0-end1 ifname end1 master br0

# 3. Bring up the bridge:
sudo nmcli con up br0
sudo nmcli con up br0-end0
sudo nmcli con up br0-end1
```

### Verifying Bridge Status

```bash
# Check interface states:
ip -br a show br0 end0 end1

# Check bridge membership:
bridge link show
```

Any computer or micro-cluster node plugged into either `end0` or `end1` can communicate directly with the RV2 and with other connected nodes.

---

## 3. Kernel IP Forwarding & NAT Masquerading

The RV2 routes outbound traffic from the internal wired network (`10.0.2.0/24`) out through the Wi-Fi uplink (`wlan0`) using Linux kernel packet forwarding and iptables Source NAT (SNAT / Masquerade).

### A. Enabling Kernel IPv4 Forwarding

Configured in `/etc/sysctl.d/99-ip-forward.conf`:
```ini
net.ipv4.ip_forward = 1
```

To apply immediately without rebooting:
```bash
sudo sysctl -w net.ipv4.ip_forward=1
```

### B. iptables NAT & Forwarding Rules

```bash
# 1. Masquerade outbound packets originating from br0 leaving wlan0:
sudo iptables -t nat -A POSTROUTING -o wlan0 -j MASQUERADE

# 2. Accept forwarding from the internal bridge to the external uplink:
sudo iptables -A FORWARD -i br0 -o wlan0 -j ACCEPT

# 3. Allow established and related incoming return packets:
sudo iptables -A FORWARD -i wlan0 -o br0 -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
```

### C. Persisting Rules Across Reboots

Rules are saved and automatically restored on boot using `netfilter-persistent`:
```bash
sudo netfilter-persistent save
# Saved to /etc/iptables/rules.v4
```

---

## 4. Firewall Hardening & Port Access Matrix

To maintain security while allowing open development on the wired subnet, incoming traffic from the external Wi-Fi WAN (`wlan0`) is selectively filtered.

### Port Security Matrix

| Port | Service | `wlan0` (Wi-Fi WAN) | `br0` (Wired `10.0.2.0/24`) | Loopback (`127.0.0.1`) |
| :--- | :--- | :--- | :--- | :--- |
| **`22`** | SSH Server | **Allowed** (Key/Password) | **Allowed** | **Allowed** |
| **`53`** | AdGuard DNS Resolver | **Allowed** (LAN DNS) | **Allowed** (Cluster DNS) | **Allowed** |
| **`3000`** | AdGuard Home Web UI | **Allowed** | **Allowed** | **Allowed** |
| **`5000`** | DuckLake & AI Server | **Allowed** (AI gated to CIDRs unless `allow_remote_ai` is ON) | **Allowed** (Full API & Chat) | **Allowed** |
| **`5432`** | PostgreSQL Database | **BLOCKED (DROP)** | **BLOCKED (DROP)** | **Loopback / Unix Socket Only** |
| **`8080`** | Adminer Database UI | **Allowed** (Auth required) | **Allowed** (Auth required) | **Allowed** |

### Database Protection Rule

PostgreSQL is bound strictly to `localhost` (`listen_addresses = 'localhost'`) with no remote `pg_hba.conf` entries, and TCP port `5432` is dropped on all non-loopback interfaces at the packet filter level:
```bash
sudo iptables -I INPUT ! -i lo -p tcp --dport 5432 -j DROP
```

This ensures PostgreSQL is accessed exclusively by the local DuckLake engine (via Unix domain socket `peer` auth) with zero external or LAN network exposure.

---

## 5. AdGuard Home Network-Level DNS & Blocker

AdGuard Home runs as a local DNS resolver and network-wide tracker/ad blocker for the RV2 and all connected cluster nodes.

### Deployment Details

* **Binary Location:** `/opt/AdGuardHome/AdGuardHome`
* **Configuration:** `/opt/AdGuardHome/AdGuardHome.yaml`
* **Systemd Service:** `AdGuardHome.service`
* **Web Admin Portal:** `http://192.168.18.15:3000/` (or `http://10.0.2.1:3000/`)
* **DNS Listening Ports:** Port `53` (UDP/TCP)

### Service Management Commands

```bash
# Check status:
sudo systemctl status AdGuardHome

# Restart service:
sudo systemctl restart AdGuardHome

# Stop / Start service:
sudo systemctl stop AdGuardHome
sudo systemctl start AdGuardHome

# View live service logs:
sudo journalctl -u AdGuardHome -f
```

### Automated Optional Setup (`setup_adguard.sh`)

AdGuard Home is **optional** (not installed by default in `install.sh`). To install and pre-configure it for maximum ad, tracker, malware, phishing, and telemetry blocking with encrypted DoH/DoQ upstreams and optimistic DNS caching:

```bash
# Standalone installation (generates a random admin password on fresh setup):
sudo ./setup_adguard.sh

# Or include local wildcard DNS rewrites for your home lab domain (see CA.md):
sudo ./setup_adguard.sh --user admin --domain lab.<your-domain> --rewrite-ip <server-ip>

# Or install during initial DuckLake server setup:
sudo ./install.sh --with-adguard --adguard-domain lab.<your-domain>
```

`setup_adguard.sh` automatically disables Ubuntu's `systemd-resolved` stub listener on port `53` (`/etc/systemd/resolved.conf.d/adguardhome.conf`), enables Safe Browsing, configures `16 MB` optimistic caching, and loads 9 curated blocklists (HaGeZi Multi PRO++, HaGeZi Threat Intelligence, OISD Big, AdGuard DNS filter, StevenBlack, URLHaus, PhishTank/OpenPhish, Dandelion Sprout Anti-Malware, and AdGuard Popup Hosts).

#### Admin Credentials & Password Management
* **No Hardcoded Default Password:** When `--password` is omitted on a fresh install, `setup_adguard.sh` generates a random 18-character password, displays it once in the terminal summary, and stores only its `bcrypt` hash inside `/opt/AdGuardHome/AdGuardHome.yaml`.
* **Default Username:** Uses `--user <name>` (defaults to the invoking `SUDO_USER`, or `admin`).
* **Custom Credentials at Setup:** Pass `--user <name> --password '<your-password>'` to `setup_adguard.sh` (or set `ADGUARD_USER` / `ADGUARD_PASSWORD`).
* **Resetting a Forgotten Password:** Update the `password:` bcrypt hash under `users:` in `/opt/AdGuardHome/AdGuardHome.yaml` (for example with `htpasswd -nbBC 10 "" 'newpass' | cut -d: -f2`) and run `sudo systemctl restart AdGuardHome`.

Connected cluster clients on `10.0.2.0/24` (and LAN devices) can point their DNS to the RV2 (`10.0.2.1` or `192.168.18.15`) to benefit from centralized DNS caching, ad/malware blocking, and local hostname rewrites.

---

## 6. Network Diagnostics & Troubleshooting Reference

### View Active Interfaces & IP Addresses
```bash
ip -br a
```

### View Routing Table
```bash
ip route show
```

### Inspect iptables Filter & NAT Chains
```bash
# View filter table rules with packet/byte counters:
sudo iptables -L -n -v

# View NAT table rules:
sudo iptables -t nat -L -n -v
```

### Check Listening Ports & Associated Processes
```bash
sudo ss -tulpn
```

### NetworkManager Status & Connections
```bash
nmcli device status
nmcli connection show
```
