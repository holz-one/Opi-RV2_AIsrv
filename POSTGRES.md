# Orange Pi RV2 PostgreSQL 16 & pgvector Server Guide

This document details the configuration, architecture, network setup, and administration of the PostgreSQL 16 and Adminer Web UI deployment on the Orange Pi RV2 (`riscv64`, 4 GB RAM).

---

## 1. System & Architecture Overview

| Component | Detail | Notes |
| :--- | :--- | :--- |
| **Board / SoC** | Orange Pi RV2 (SpacemiT K1 / Ky X1, 8 cores @ 1.6 GHz) | Native `riscv64` architecture |
| **Operating System** | Ubuntu 24.04.5 LTS (Noble Numbat) | Kernel `6.6.63-ky` |
| **PostgreSQL Version** | **PostgreSQL 16.15** | Official Ubuntu Noble native `riscv64` build |
| **Vector Extension** | **`pgvector` 0.6.0** | HNSW & IVFFlat index support, up to 16,000 dimensions |
| **Storage Location** | **`/srv/postgresql/16/main`** | Located on 476 GB NVMe drive (`nvme0n1p1`, >450 GB free) |
| **Web Admin Interface** | **Adminer 4.8.1** (Port `8080`) | Powered by PHP 8.3 CLI with Dracula dark theme |
| **Ethernet Bridge** | **`br0`** (`10.0.2.1/24`) | Hardware-switched bridge uniting `end0` and `end1` |
| **Internet Uplink** | **`wlan0`** (`192.168.18.15`) | Kernel IP forwarding & NAT masquerade enabled |

---

## 2. Storage & NVMe Placement

To prevent root partition exhaustion and maximize I/O throughput for WAL (Write-Ahead Logging) and vector indexing, the database cluster was moved from `/var/lib/postgresql` to the largest available NVMe partition:

* **Mount Point:** `/srv` (476 GB NVMe partition)
* **Data Directory:** `/srv/postgresql/16/main`
* **Permissions:** Owner `postgres:postgres`, mode `700`

Configured in `/etc/postgresql/16/main/postgresql.conf`:
```ini
data_directory = '/srv/postgresql/16/main'
```

---

## 3. Network Architecture & Security

The server is configured with a hybrid network topology that isolates external access while providing zero-friction development connectivity:

```
                      +---------------------------------------+
                      |           Orange Pi RV2               |
                      |                                       |
Internet <--- wlan0 --+-- NAT Masquerade (iptables)           |
 (192.168.18.15)      |   * Port 5432: BLOCKED from wlan0     |
                      |   * Port 8080: Adminer Web UI (open)  |
                      |   * Port 3000: AdGuard Home (open)    |
                      |                                       |
                      |   br0 Bridge (10.0.2.1/24)            |
                      |   +-------------------+               |
                      |   | end0 (Gigabit 1)  |               |
                      |   | end1 (Gigabit 2)  |               |
                      |   +---------+---------+               |
                      +-------------|-------------------------+
                                    |
                    Wired Clients (10.0.2.x)
                    * Passwordless PostgreSQL (trust)
                    * Routed to Internet via wlan0
```

### A. Dual-Ethernet Bridge (`br0`)
* Both physical Gigabit Ethernet ports (`end0` and `end1`) are attached as slave ports to `br0`.
* Configured with a static IP: **`10.0.2.1/24`**.
* Any devices connected to either port share the same network layer.

### B. Routing & NAT Masquerading
* Kernel IP forwarding is permanently enabled via `/etc/sysctl.d/99-ip-forward.conf`:
  ```ini
  net.ipv4.ip_forward = 1
  ```
* `iptables` routes and masquerades outbound packets from `br0` (`10.0.2.0/24`) through `wlan0`:
  ```bash
  iptables -t nat -A POSTROUTING -o wlan0 -j MASQUERADE
  iptables -A FORWARD -i br0 -o wlan0 -j ACCEPT
  iptables -A FORWARD -i wlan0 -o br0 -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
  ```
* Rules are persisted across reboots via `netfilter-persistent`.

### C. Client Authentication Matrix (`pg_hba.conf` & Firewall)

| Source Interface / Subnet | Target Port | Authentication Method | Policy |
| :--- | :--- | :--- | :--- |
| **Ethernet Bridge (`10.0.2.0/24`)** | `5432` | **`trust`** | Allowed without password |
| **Local Web UI (`127.0.0.1`)** | `5432` | **`scram-sha-256`** | Allowed with password |
| **Local CLI Socket** | `/var/run/postgresql` | **`peer`** | Allowed for matching Linux user |
| **Wi-Fi LAN (`wlan0`)** | `5432` | **Firewall DROP** | Direct TCP access blocked |
| **Wi-Fi LAN (`wlan0`)** | `8080` | **HTTP (Adminer)** | Allowed (secured by DB password) |

---

## 4. Adminer Web Interface

Adminer is configured as a persistent systemd service (`adminer.service`) running on PHP 8.3 CLI:

* **URL:** `http://192.168.18.15:8080/`
* **Document Root:** `/opt/adminer`
* **Theme:** Dracula dark theme (`/opt/adminer/adminer.css`)

### Automatic Loopback Translation
PHP applications default `localhost` to UNIX sockets, which triggers PostgreSQL `peer` authentication (causing permission errors because Adminer runs as `www-data`). Adminer's configuration automatically translates any empty or `localhost` connection into `127.0.0.1` (TCP loopback with `scram-sha-256` authentication), and pre-populates:
* **System:** `PostgreSQL`
* **Server:** `127.0.0.1`
* **Username:** `orangepi`
* **Database:** `orangepi`

---

## 5. Working with PostgreSQL & `pgvector`

### A. Terminal Access (on the board)
Connect directly without a password using Linux peer authentication:
```bash
psql
```

### B. Setting or Changing the User Password
To change the password for `orangepi` (used for Adminer and TCP logins):
```bash
psql -c "ALTER USER orangepi WITH PASSWORD 'new_password_here';"
```

### C. Connecting from Wired Network Clients (`10.0.2.x`)
From any machine connected to `end0` or `end1`:
```bash
psql -h 10.0.2.1 -U orangepi -d orangepi
```
*(No password will be prompted because of the `trust` rule).*

### D. Testing Vector Similarity Operations

Connect to `psql` and run:

```sql
-- 1. Create a table with a 3-dimensional vector
CREATE TABLE sample_vectors (
    id bigserial PRIMARY KEY,
    title text,
    embedding vector(3)
);

-- 2. Insert test vectors
INSERT INTO sample_vectors (title, embedding) VALUES
    ('Document A', '[1.0, 2.0, 3.0]'),
    ('Document B', '[1.1, 2.1, 2.9]'),
    ('Document C', '[9.0, 8.0, 7.0]');

-- 3. Query nearest neighbors using Cosine Distance (<=>)
SELECT 
    title, 
    embedding <=> '[1.0, 2.0, 3.0]' AS cosine_distance
FROM sample_vectors
ORDER BY cosine_distance ASC;

-- 4. Create an HNSW index for fast approximate search
CREATE INDEX ON sample_vectors USING hnsw (embedding vector_cosine_ops);
```

### E. Python Client Example (`psycopg` + `pgvector`)

```python
import psycopg
from pgvector.psycopg import register_vector

# Connect over wired Ethernet (passwordless trust):
conn = psycopg.connect("host=10.0.2.1 dbname=orangepi user=orangepi")
register_vector(conn)

with conn.cursor() as cur:
    cur.execute("SELECT id, title, embedding FROM sample_vectors LIMIT 5;")
    for row in cur.fetchall():
        print(row)
```

---

## 6. Automated Setup Script (`setup_postgres_server.sh`)

An idempotent, automated installer script is located at `/cluster/setup_postgres_server.sh`.

### How to Run:
```bash
# Run with default settings:
sudo /cluster/setup_postgres_server.sh

# Or run with a pre-set password:
sudo PG_PASSWORD="my_secure_password" /cluster/setup_postgres_server.sh
```

### Environment Variables:
* `PG_USER`: Database username (default: `orangepi`)
* `PG_DB`: Database name (default: `orangepi`)
* `PG_PASSWORD`: Password for the user (optional)
* `WAN_INTERFACE`: Internet uplink interface (default: `wlan0`)

---

## 7. Service Management & Troubleshooting

### Check Service Status
```bash
# PostgreSQL status
sudo systemctl status postgresql

# Adminer Web UI status
sudo systemctl status adminer

# AdGuard Home status
sudo systemctl status AdGuardHome
```

### Restart Services
```bash
sudo systemctl restart postgresql
sudo systemctl restart adminer
```

### Check Storage Usage
```bash
df -h /srv
du -sh /srv/postgresql/16/main
```

### View Firewall & NAT Rules
```bash
sudo iptables -L -n -v
sudo iptables -t nat -L -n -v
```

---

## 8. Universal Dataset Importer (`pg_import.py`)

A high-performance CLI utility is available at `/cluster/pg_import.py` to import tabular datasets directly into PostgreSQL.

### A. Supported Formats & Capabilities

| Format | Extensions | Engine / Libraries | Multi-Table / Multi-Sheet |
| :--- | :--- | :--- | :--- |
| **SQLite** | `.sqlite`, `.db`, `.sqlite3` | `sqlite3` (built-in) | Imports all tables or `--source-table` |
| **DuckDB** | `.duckdb`, `.ddb` | `duckdb` | Imports all tables or `--source-table` |
| **Parquet** | `.parquet`, `.pq` | `duckdb` | High-speed columnar streaming into PostgreSQL |
| **CSV / TSV** | `.csv`, `.tsv`, `.txt`, `.tab` | `pandas` + `csv.Sniffer` | Auto-detects delimiter, chunked stream |
| **Excel** | `.xlsx`, `.xls` | `openpyxl`, `xlrd` | Imports all sheets or `--sheet <name>` |
| **Calc (LibreOffice)** | `.ods` | `odfpy` | Imports all sheets or `--sheet <name>` |

### B. Core Features

* **High-Throughput Streaming:** Uses PostgreSQL `COPY ... FROM STDIN` streaming via `psycopg` (binary/text protocol) rather than slow individual `INSERT` statements.
* **Safe Identifier Sanitization:** Automatically normalizes table and column names to lowercase alphanumeric PostgreSQL standards, cleans reserved words, and deduplicates identical headers.
* **Smart Type Inference & Affinity Handling:** In addition to mapping native datatypes (dates, booleans, floating-point numbers, bigints, text), automatically resolves SQLite affinity quirks (e.g. text stored in numeric-affinity columns).
* **Modes:**
  * `--mode replace` (default): Drops existing table (`DROP TABLE IF EXISTS ... CASCADE`) and recreates it.
  * `--mode append`: Inserts into existing table schema without dropping.
  * `--mode fail`: Aborts if table already exists.
* **Preview Mode (`--preview` / `-n`):** Inspects schema, column types, and prints an aligned ASCII table preview of the first 5 rows without connecting to or modifying PostgreSQL.
* **List Mode (`--list` / `-l`):** Lists all tables or sheets present in the source file along with row counts.

### C. CLI Usage Examples

```bash
# Activate Python environment on the RV2:
source /cluster/.venv/bin/activate

# 1. Preview a Parquet dataset or Excel file without touching the database:
./pg_import.py dataset.parquet --preview
./pg_import.py sales_data.xlsx --preview

# 2. List sheets in a LibreOffice Calc spreadsheet:
./pg_import.py finances.ods --list

# 3. Import a Parquet file directly into PostgreSQL:
./pg_import.py corpus.parquet -d orangepi -t corpus

# 4. Import all tables from an SQLite database into schema 'analytics':
./pg_import.py app.db -d orangepi -s analytics

# 5. Import a CSV file into a specific table:
./pg_import.py customers.csv -d orangepi -t customers

# 6. Import DuckDB database into schema 'warehouse':
./pg_import.py metrics.duckdb -d orangepi -s warehouse

# 7. Import a specific sheet from Excel, appending to an existing table:
./pg_import.py monthly_report.xlsx --sheet "November" -t monthly_sales --mode append
```




