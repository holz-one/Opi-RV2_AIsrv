# Orange Pi RV2 DuckLake & Local AI Server (`aisrv.py`)

A unified, high-performance **DuckLake Analytical Server**, **Raw Data Warehouse**, and **Local LLM Inference Engine** designed for the Orange Pi RV2 (SpacemiT K1 / Ky X1 8-core `riscv64`, 4 GB RAM).

Combines an embedded **DuckDB** analytical query engine with **PostgreSQL 16** metadata cataloging, high-speed **Parquet lake storage** on NVMe, raw dataset warehousing with **Project Partitions**, and **ONNX Runtime GenAI** local inference with network-level authorization.

---

## 1. System Architecture

```
                                  +---------------------------------------+
                                  |            Orange Pi RV2              |
                                  |                                       |
    Internet / External WiFi <----+--- wlan0 (192.168.18.15)              |
                                  |    * Projects & Archiving: OPEN       |
                                  |    * DuckLake Analytics: OPEN         |
                                  |    * Ingestion & Warehouse: OPEN      |
                                  |    * AI Features: BLOCKED (403)       |
                                  |                                       |
    Wired Cluster Clients    <----+--- br0 Bridge (10.0.2.1/24)           |
    (end0 + end1 Gigabit)         |    * Projects & Archiving: OPEN       |
                                  |    * DuckLake Analytics: OPEN         |
                                  |    * Ingestion & Warehouse: OPEN      |
                                  |    * AI Inference: PERMITTED          |
                                  +---------------------------------------+
                                                     |
                                   +-----------------+-----------------+
                                   |                                   |
                             DuckDB Engine                    PostgreSQL 16
                             (Analytical)                      (Metadata)
                                   |                                   |
                    +--------------+--------------+             ducklake_projects
                    |                             |             ducklake_tables
             /srv/DuckLake                 /srv/Warehouse       ducklake_raw_files
            (*.parquet lake)              (Raw CSV/JSON/DB)     ai_models
            ├── default/                  ├── default/          ai_chat_logs
            ├── dawahtools/               ├── configs/          ducklake_queries
            └── <project_id>/             └── <project_id>/
```

### Core Capabilities:
1. **DuckLake Engine (`DuckDB`):** Analytical query processor executing across Parquet files and raw warehouse files with vectorization and ZSTD compression.
2. **Metadata Store (`PostgreSQL 16`):** Central catalog holding table schemas, dataset inventory, project partitions, AI model registry, query telemetry, and chat interaction logs.
3. **Parquet Lake (`/srv/DuckLake`):** Storage for optimized, compressed columnar Parquet datasets.
4. **Authoritative Raw Warehouse (`/srv/Warehouse`):** Sole physical ingestion zone for CSV, TSV, JSON, SQLite, and Excel files (no symlinks, correct spelling).
5. **Dedicated Project Partitions (`/projects`):** Isolated storage zones (`/srv/DuckLake/<project>` and `/srv/Warehouse/<project>`) for external applications to archive their structured data and raw logs.
6. **Local AI Engine (`onnxruntime-genai`):** OpenAI-compatible chat completions (`/v1/chat/completions`) and model registry for quantized local models (Qwen2, Llama, MiniCPM). Gated to `10.0.2.0/24`.
7. **Web Management Console:** Unified web interface for managing projects, executing SQL queries, inspecting tables/files, testing AI models, toggling AI/Adminer memory, and configuring server parameters.
8. **Self-Discovery Manifest (`/manifest`):** Real-time service manifest and OpenAI-compatible tool schemas for local LLMs and agent frameworks.

---

## 2. Automated Installation & Setup

To deploy the DuckLake & AI Server on a fresh machine (Orange Pi RV2 or any Ubuntu/Debian Linux system):

```bash
# 1. Clone the repository
git clone https://github.com/kholzm/Opi-RV2_AIsrv.git /cluster
cd /cluster

# 2. Run the automated installer
sudo ./install.sh
```

### What `install.sh` Does Automatically:
1. **System Detection & Packages:** Detects CPU architecture (`riscv64`, `aarch64`, `x86_64`) and installs required packages (`postgresql`, `python3-venv`, `libpq-dev`, `build-essential`, `jq`, `curl`).
2. **Storage Setup:** Initializes `/srv/DuckLake` (Parquet storage) and `/srv/Warehouse` (raw files), sets permissions (`775`), and cleans up any legacy symlinks.
3. **PostgreSQL Setup:** Configures PostgreSQL metadata user & database with passwordless peer socket authentication.
4. **Python Virtualenv:** Creates `.venv`, installs dependencies from `requirements.txt` (`duckdb`, `flask`, `gunicorn`, `pandas`, `psycopg`, `pyyaml`).
5. **Configuration Files:** Generates `server.yaml` and `.env` with a cryptographically secure random `SECRET_KEY`.
6. **Systemd Service:** Installs and starts `/etc/systemd/system/aisrv.service` running memory-optimized Gunicorn (`--workers 1 --threads 4 --worker-class gthread`).
7. **Health Verification:** Performs an automated health check on `http://127.0.0.1:5000/status`.

### Custom Installation Options:
```bash
./install.sh --help

# Custom user and database:
sudo ./install.sh --user myuser --db-name mydb

# Custom port & storage paths:
sudo ./install.sh --port 8000 --ducklake-dir /mnt/nvme/DuckLake --warehouse-dir /mnt/nvme/Warehouse

# Setup environment without creating systemd service:
sudo ./install.sh --no-service
```

---

## 3. Configuration Management

### A. `server.yaml`
All deployment parameters are centralized in `server.yaml` and can be edited directly or updated live via the Web UI / REST API (`PATCH /config`):

```yaml
# server.yaml - Central Configuration
server:
  host: "0.0.0.0"
  port: 5000
  debug: false
  cors_enabled: true

postgres:
  host: ""                  # Empty string defaults to local Unix socket (peer auth), or "10.0.2.1"
  port: 5432
  dbname: "orangepi"
  user: "orangepi"
  password: ""              # Overridden by PG_PASSWORD in .env
  connect_timeout: 10
  min_connections: 1
  max_connections: 5

storage:
  ducklake_dir: "/srv/DuckLake"       # Columnar Parquet data lake
  warehouse_dir: "/srv/Warehouse"     # Sole raw data warehouse (no symlinks)
  models_dir: "/cluster/RV2-AI"       # Local ONNX AI models
  db_cache_dir: "/cluster/database"
  max_upload_size_mb: 250             # Max upload size in Megabytes

ducklake:
  memory_limit: "1GB"                 # Memory budget for analytical queries
  threads: 4                          # Analytical worker threads (SpacemiT K1 CPU)
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
  ai_allowed_cidrs:
    - "10.0.2.0/24"                   # Dual Gigabit Ethernet Bridge (br0)
    - "127.0.0.1/32"                  # Localhost IPv4
    - "::1/128"                       # Localhost IPv6
  trust_forwarded_for: true
```

### B. Configuration REST API
- **Read Active Configuration (`GET /config`):** Returns sanitized runtime configuration.
- **Update Configuration Live (`PATCH /config`):** Persists updates back to `server.yaml` and reloads runtime engine pragmas immediately (e.g. dynamic memory limits, query threads, and security CIDRs):
  ```bash
  curl -X PATCH http://192.168.18.15:5000/config \
    -H "Content-Type: application/json" \
    -d '{
      "ducklake": {"memory_limit": "1.5GB", "threads": 4},
      "security": {"ai_allowed_cidrs": ["10.0.2.0/24", "192.168.18.0/24", "127.0.0.1/32"]}
    }'
  ```

---

## 4. Project Partitions & App Archiving

Projects provide partitioned data isolation for external applications archiving data into the server.

### A. Project Directory Architecture
Each project `<project_id>` automatically gets dedicated directories:
- Parquet Lake: `/srv/DuckLake/<project_id>/`
- Raw Warehouse: `/srv/Warehouse/<project_id>/`

### B. Project REST Endpoints

#### 1. List Projects (`GET /projects`)
Returns all registered projects with table counts, raw file counts, total row counts, and combined disk usage:
```bash
curl http://192.168.18.15:5000/projects
```

#### 2. Create Project (`POST /projects`)
Registers a new project partition and creates its physical directories on disk:
```bash
curl -X POST http://192.168.18.15:5000/projects \
  -H "Content-Type: application/json" \
  -d '{
    "project_id": "iot_sensors",
    "display_name": "IoT Sensor Telemetry",
    "description": "Temperature and humidity stream from edge sensors"
  }'
```

#### 3. Inspect Project Details & Assets (`GET /projects/<project_id>`)
Returns metadata, table catalog, raw file list, and copyable integration snippets for that project:
```bash
curl http://192.168.18.15:5000/projects/iot_sensors
```

#### 4. Delete Project (`DELETE /projects/<project_id>`)
Unassigns project tables/files to default, or purges them from disk if `?purge=true`:
```bash
curl -X DELETE "http://192.168.18.15:5000/projects/iot_sensors?purge=false"
```

### C. Archiving Data to a Project (For External Applications)

External apps can archive data directly into a project partition using either RESTful methods or multipart uploads:

```bash
# Ingest / Replace a Parquet or CSV Table:
curl -X PUT "http://192.168.18.15:5000/projects/iot_sensors/tables/readings" \
  -F "file=@readings.csv"

# Append JSON Records Stream to a Table:
curl -X PATCH "http://192.168.18.15:5000/projects/iot_sensors/tables/readings" \
  -H "Content-Type: application/json" \
  -d '[{"device_id": "esp32_01", "temp": 24.3, "humidity": 55.2}]'

# Upload a Raw Warehouse File (Tar, SQLite, Logs):
curl -X PUT "http://192.168.18.15:5000/projects/iot_sensors/files/sensor_dump.tar.gz" \
  -F "file=@sensor_dump.tar.gz"
```

**Python `requests` Integration Example:**
```python
import requests

# Append structured records directly into project DuckLake table
records = [{"id": 1, "status": "ok", "voltage": 3.3}]
res = requests.patch("http://192.168.18.15:5000/projects/iot_sensors/tables/telemetry", json=records)
print(res.json())

# Archive raw log file to warehouse
with open("device.log", "rb") as f:
    res = requests.put("http://192.168.18.15:5000/projects/iot_sensors/files/device.log", files={"file": f})
print(res.json())
```

---

## 5. RESTful API Specifications

### URL Prefix Convention:
- **`/v1/` is reserved strictly for OpenAI-compatible AI functions:** `GET /v1/models` and `POST /v1/chat/completions`.
- **Analytical & storage endpoints reside at clean root paths:**
  - DuckLake: `/ducklake/query`, `/ducklake/catalog`, `/ducklake/tables/*`, `/ducklake/sync`, `/ducklake/convert`
  - Warehouse: `/warehouse/files`, `/warehouse/files/*`
  - Projects: `/projects`, `/projects/<id>/*`
  - Management & Discovery: `/config`, `/status`, `/services/*`, `/manifest`, `/openapi.json`
- **Strict Endpoint Separation:** Non-AI endpoints are strictly excluded from `/v1/`. Calls to `/v1/projects` or `/v1/ducklake/*` return `404 Not Found`.

The server strictly follows RESTful principles:
- `PUT` creates or replaces a resource idempotently.
- `PATCH` appends or partially updates an existing resource.
- `GET` reads resources, schemas, or downloads files.
- `DELETE` drops resources.
- `POST` is reserved strictly for operational commands (`/ducklake/query`, `/ducklake/convert`, `/ducklake/sync`, `/v1/chat/completions`, `/services/*`).

### A. DuckLake Table Resources (`/ducklake/tables/<table_name>`)

#### 1. Create or Replace Table (`PUT /ducklake/tables/<table_name>`)
```bash
# Upload CSV/Parquet file to replace table:
curl -X PUT http://10.0.2.1:5000/ducklake/tables/inventory \
  -F "file=@inventory.csv"

# Send JSON array to replace table:
curl -X PUT http://10.0.2.1:5000/ducklake/tables/metrics \
  -H "Content-Type: application/json" \
  -d '[{"id": 1, "cpu": 12.5}, {"id": 2, "cpu": 18.2}]'
```

#### 2. Append Records to Table (`PATCH /ducklake/tables/<table_name>`)
```bash
curl -X PATCH http://10.0.2.1:5000/ducklake/tables/metrics \
  -H "Content-Type: application/json" \
  -d '[{"id": 3, "cpu": 9.4}]'
```

#### 3. Inspect Table Schema & Preview Rows (`GET /ducklake/tables/<table_name>`)
```bash
curl "http://10.0.2.1:5000/ducklake/tables/inventory?limit=25"
```

#### 4. Download Parquet Binary (`GET /ducklake/tables/<table_name>/download`)
```bash
curl -O http://10.0.2.1:5000/ducklake/tables/inventory/download
```

#### 5. Drop Table (`DELETE /ducklake/tables/<table_name>`)
```bash
curl -X DELETE http://10.0.2.1:5000/ducklake/tables/inventory
```

---

### B. Raw Warehouse File Resources (`/warehouse/files/<path:filename>`)

Stored in `/srv/Warehouse` (no symlinks).

#### 1. Create or Replace Raw File (`PUT /warehouse/files/<path:filename>`)
```bash
# Stream raw text directly:
curl -X PUT http://10.0.2.1:5000/warehouse/files/configs/cluster.conf \
  --data-binary "role=master\nthreads=8"

# Multipart upload:
curl -X PUT http://10.0.2.1:5000/warehouse/files/backups/dump.sqlite \
  -F "file=@dump.sqlite"
```

#### 2. List Warehouse Files (`GET /warehouse/files`)
```bash
curl http://10.0.2.1:5000/warehouse/files
```

#### 3. Read or Download Raw File (`GET /warehouse/files/<path:filename>`)
```bash
# Download raw file:
curl -O http://10.0.2.1:5000/warehouse/files/configs/cluster.conf

# Preview text / CSV / JSONL as structured JSON lines:
curl "http://10.0.2.1:5000/warehouse/files/logs/events.jsonl?preview=true&max_lines=50"
```

#### 4. Delete Raw File (`DELETE /warehouse/files/<path:filename>`)
```bash
curl -X DELETE http://10.0.2.1:5000/warehouse/files/logs/events.jsonl
```

---

### C. Commands & Actions (`POST` Endpoints)

#### 1. Execute Analytical SQL (`POST /ducklake/query`)
Executes vector-accelerated SQL queries on DuckDB across Parquet lake tables and Warehouse datasets.
```bash
curl -X POST http://10.0.2.1:5000/ducklake/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM cluster_nodes WHERE role = '\''compute'\'';"}'
```

#### 2. Convert Raw Dataset to Parquet (`POST /ducklake/convert`)
Transforms a raw Warehouse dataset (CSV, JSON) into a high-speed columnar Parquet table in `/srv/DuckLake`:
```bash
curl -X POST http://10.0.2.1:5000/ducklake/convert \
  -H "Content-Type: application/json" \
  -d '{"source_file": "telemetry.csv", "table_name": "telemetry"}'
```

#### 3. Synchronize Storage Catalog (`POST /ducklake/sync`)
Triggers a full rescan of `/srv/DuckLake` and `/srv/Warehouse`, auto-discovering project partitions and updating PostgreSQL:
```bash
curl -X POST http://10.0.2.1:5000/ducklake/sync
```

#### 4. Local AI Chat Completions (`POST /v1/chat/completions`)
OpenAI-compatible inference powered by `onnxruntime-genai` (gated strictly to `10.0.2.0/24`):
```bash
curl -X POST http://10.0.2.1:5000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "OrangePI/qwen2-int4-0.5b",
    "messages": [{"role": "user", "content": "Explain DuckLake architecture."}],
    "temperature": 0.7
  }'
```

#### 5. AI Chat Runtime Toggle (`POST /services/ai`)
Toggle the AI inference service on or off. When disabled, all loaded ONNX models are immediately unloaded from RAM (`gc.collect()`), freeing 800 MB to 2.4 GB of RAM on the 4 GB RISC-V SBC for analytical DuckDB workloads:
```bash
# Check status and loaded models:
curl http://192.168.18.15:5000/services/ai

# Stop AI / Unload all models from RAM:
curl -X POST http://192.168.18.15:5000/services/ai -H "Content-Type: application/json" -d '{"action": "stop"}'

# Toggle AI state:
curl -X POST http://192.168.18.15:5000/services/ai -H "Content-Type: application/json" -d '{"action": "toggle"}'
```

#### 6. Adminer Memory Toggle (`POST /services/adminer`)
Toggle or check the Adminer PHP web server to free or allocate system memory on demand:
```bash
# Check status and memory usage:
curl http://192.168.18.15:5000/services/adminer

# Stop Adminer (free ~25-30 MB RAM):
curl -X POST http://192.168.18.15:5000/services/adminer -H "Content-Type: application/json" -d '{"action": "stop"}'

# Or use the CLI shortcut directly on the RV2:
adminer stop     # or 'adminer off'
adminer start    # or 'adminer on'
adminer toggle
adminer status
```

---

## 6. AI Models Registry & Inventory

### A. List Models Catalog (`GET /ai/models`)
Returns full rich details of all local ONNX models registered in PostgreSQL (model ID, variant, architecture, context limit, and disk footprint). Accessible from any client network:
```bash
curl http://192.168.18.15:5000/ai/models
```

### B. Rescan Models on Disk (`POST /ai/models/sync`)
Scans `/cluster/RV2-AI` and registers newly downloaded models into PostgreSQL:
```bash
curl -X POST http://192.168.18.15:5000/ai/models/sync
```

---

## 7. Web Management Console

Open `http://<server-ip>:5000/` in any web browser to access the unified dashboard:
- **📁 Projects & Archiving Tab:** View all project partitions, create new projects, upload data directly to a project partition, and copy application integration snippets (`curl` / Python).
- **🦆 DuckLake Analytics & SQL Tab:** Run interactive SQL queries, browse Parquet tables and Warehouse files, and convert datasets on-the-fly.
- **🤖 AI Models & Chat Tab:** Inspect registered local LLM variants, check parameters, chat interactively with streaming output, and toggle AI runtime memory state on/off.
- **⚙️ Configuration Settings Tab:** Live management of DuckDB memory limits, SpacemiT K1 CPU thread concurrency, network CIDRs, and upload limits with instant save.
- **📜 Manifest & LLM Tools Tab:** Service manifest (`/manifest`) and OpenAPI 3.0 specification (`/openapi.json`) for agent self-discovery.
- **📊 System Status Tab:** Live health status (`/status`), client IP address authorization, and Adminer & AI runtime memory toggles.
