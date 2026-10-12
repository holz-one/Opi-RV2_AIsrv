# Orange Pi RV2 Local AI Server (`aisrv.py`)

A lightweight, OpenAI-compatible Flask API server designed for running local ONNX models on an Orange Pi RV2 (`riscv64`, 4 GB RAM) using `onnxruntime-genai` and a persistent DuckDB model registry.

---

## Features

* **OpenAI-Compatible Endpoints:** Exposes standard `/v1/models` and `/v1/chat/completions` routes.
* **DuckDB Model Registry:** Automatically scans `/cluster/RV2-AI`, catalogs available model variants, and attributes ownership dynamically (e.g., Hugging Face organizations vs. Orange PI).
* **Low-Memory Optimization:** Lazy-loads models on demand with pinned `numpy<2` ABI compatibility.

---

## Quick Start

1. **Activate the environment:**
```bash
source /cluster/.venv/bin/activate
```

2. **Run API Server**
```bash
uv python aisrv.py
```

## Testing Endpoints

-  List available models:
```bash
curl http://localhost:5000/v1/models
```

- Test chat completion:
```bash
curl http://localhost:5000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "qwen2-int4-0.5b",
    "messages": [{"role": "user", "content": "Hello!"}],
    "stream": false
  }'
```

**Read:**  *SETUP.md* for more details on my setup
