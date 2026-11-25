#!/usr/bin/env python3
"""
aisrv.py - Local AI Inference Engine & Model Registry Module for Orange Pi RV2
==============================================================================
A standalone, modular AI engine powered by ONNX Runtime GenAI for native
RISC-V LLM execution (SpacemiT K1 / Ky X1, 8 cores).

Features:
  - ONNX Runtime GenAI model loader & memory-managed cache (load_model_instance, unload_all_models).
  - Model registry scanning & metadata sync with PostgreSQL (sync_models_to_db, get_detailed_models_list).
  - OpenAI-compatible chat generation with ChatML formatting (streaming SSE and standard JSON).
  - Chat interaction telemetry logging to PostgreSQL (ai_chat_logs).
  - Dynamic memory offloading to free RAM for analytical DuckLake workloads.
"""

import gc
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, Union

# Optional ONNX Runtime GenAI import (graceful fallback if wheels not present)
try:
    import onnxruntime_genai as og
    OG_AVAILABLE = True
except ImportError:
    og = None
    OG_AVAILABLE = False

import duckdb

logger = logging.getLogger("aisrv")

# Module-level model instance cache & locks
loaded_models: Dict[str, Any] = {}
_model_lock = threading.Lock()

_MODULE_CONFIG: Dict[str, Any] = {
    "storage": {
        "models_dir": "/cluster/RV2-AI",
        "db_cache_dir": "/cluster/cache",
    },
    "ai": {
        "enabled": True,
        "default_context_length": 2048,
        "default_max_tokens": 256,
        "default_temperature": 0.7,
        "repetition_penalty": 1.1,
    },
    "postgres": {
        "dbname": "orangepi",
        "user": "orangepi",
        "host": "",
        "port": 5432,
        "password": "",
    }
}

_PG_CONN_FACTORY: Optional[Callable[[], Any]] = None


def set_config(config: Dict[str, Any]):
    """Updates the AI module's configuration dictionary."""
    global _MODULE_CONFIG
    _MODULE_CONFIG.update(config)


def set_pg_connection_factory(factory_func: Callable[[], Any]):
    """Sets the database connection factory function for PostgreSQL queries."""
    global _PG_CONN_FACTORY
    _PG_CONN_FACTORY = factory_func


def _get_db_conn(get_pg_conn: Optional[Callable[[], Any]] = None):
    """Retrieves an active PostgreSQL connection using the provided or global factory."""
    factory = get_pg_conn or _PG_CONN_FACTORY
    if factory:
        return factory()
    import psycopg
    pg = _MODULE_CONFIG.get("postgres", {})
    conn_params = {
        "dbname": pg.get("dbname", "orangepi"),
        "user": pg.get("user", "orangepi"),
        "connect_timeout": pg.get("connect_timeout", 10),
    }
    host = pg.get("host", "").strip() if pg.get("host") else ""
    if host:
        conn_params["host"] = host
        conn_params["port"] = pg.get("port", 5432)
    if pg.get("password"):
        conn_params["password"] = pg["password"]
    return psycopg.connect(**conn_params)


def sync_models_to_db(config: Optional[Dict[str, Any]] = None, get_pg_conn: Optional[Callable[[], Any]] = None):
    """Scans models directory for ONNX models and registers metadata in PostgreSQL."""
    cfg = config or _MODULE_CONFIG
    base_dir = cfg.get("storage", {}).get("models_dir", "/cluster/RV2-AI")
    if not os.path.exists(base_dir):
        return

    for root, dirs, files in os.walk(base_dir):
        if ".cache" in root or ".git" in root:
            continue

        has_genai = "genai_config.json" in files
        has_standard = "config.json" in files

        if has_genai or has_standard:
            model_dir = root
            config_path = os.path.join(model_dir, "genai_config.json")

            # Auto-generate minimal genai_config.json if only standard config.json is present
            if not has_genai and has_standard:
                try:
                    onnx_files = [f for f in files if f.endswith(".onnx")]
                    if onnx_files or "onnx" in dirs:
                        basic_config = {
                            "model_type": "llama",
                            "vocab_size": 32000,
                            "hidden_size": 2048,
                            "num_attention_heads": 32,
                            "num_hidden_layers": 22,
                            "context_length": cfg.get("ai", {}).get("default_context_length", 2048),
                            "provider": "cpu",
                        }
                        try:
                            with open(os.path.join(model_dir, "config.json"), "r") as cf:
                                c_data = json.load(cf)
                                if "model_type" in c_data:
                                    basic_config["model_type"] = c_data["model_type"]
                        except Exception:
                            pass

                        with open(config_path, "w") as gf:
                            json.dump(basic_config, gf, indent=2)
                except Exception as e:
                    logger.debug(f"Skipping config generation for {model_dir}: {e}")

            if os.path.exists(config_path):
                rel_path = os.path.relpath(model_dir, base_dir)
                clean_id = rel_path.replace(os.sep, "/")
                variant_name = os.path.basename(model_dir)

                try:
                    with _get_db_conn(get_pg_conn) as pg_conn:
                        with pg_conn.cursor() as cur:
                            cur.execute("""
                                INSERT INTO ai_models (id, owned_by, directory_path, variant_name, updated_at)
                                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                                ON CONFLICT (id) DO UPDATE SET
                                    directory_path = EXCLUDED.directory_path,
                                    variant_name = EXCLUDED.variant_name,
                                    updated_at = CURRENT_TIMESTAMP;
                            """, (clean_id, "local", model_dir, variant_name))
                            pg_conn.commit()
                except Exception as e:
                    logger.warning(f"Failed to register model {clean_id} in PostgreSQL: {e}")


def get_detailed_models_list(config: Optional[Dict[str, Any]] = None, get_pg_conn: Optional[Callable[[], Any]] = None) -> List[Dict[str, Any]]:
    """Retrieves full model inventory for UI and management endpoints."""
    cfg = config or _MODULE_CONFIG
    sync_models_to_db(cfg, get_pg_conn)
    models = []
    try:
        with _get_db_conn(get_pg_conn) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, owned_by, directory_path, variant_name, model_type, created_at, updated_at
                    FROM ai_models ORDER BY id ASC;
                """)
                for r in cur.fetchall():
                    m_id, owned, dir_path, variant, m_type, created, updated = r
                    disk_size_mb = 0
                    p = Path(dir_path) if dir_path else None
                    if p and p.exists():
                        try:
                            total_bytes = sum(f.stat().st_size for f in p.rglob("*") if f.is_file())
                            disk_size_mb = round(total_bytes / (1024 * 1024), 1)
                        except Exception:
                            pass
                    models.append({
                        "id": m_id,
                        "owned_by": owned or "local",
                        "directory_path": dir_path,
                        "variant_name": variant or (Path(dir_path).name if dir_path else m_id),
                        "model_type": m_type or "genai_onnx",
                        "disk_size_mb": disk_size_mb,
                        "status": "ready" if (p and p.exists()) else "missing_files",
                        "context_length": cfg.get("ai", {}).get("default_context_length", 2048),
                        "default_max_tokens": cfg.get("ai", {}).get("default_max_tokens", 256),
                        "created_at": created.isoformat() if created else None
                    })
    except Exception as e:
        logger.error(f"Failed to get detailed models list: {e}")
    return models


def get_model_path(model_id: str, config: Optional[Dict[str, Any]] = None, get_pg_conn: Optional[Callable[[], Any]] = None) -> Optional[str]:
    """Resolves model id or alias to the directory containing genai_config.json via PostgreSQL."""
    cfg = config or _MODULE_CONFIG
    sync_models_to_db(cfg, get_pg_conn)
    try:
        with _get_db_conn(get_pg_conn) as pg_conn:
            with pg_conn.cursor() as cur:
                cur.execute("""
                    SELECT directory_path FROM ai_models
                    WHERE id = %s OR variant_name = %s OR id LIKE '%%/' || %s
                    ORDER BY CASE WHEN id = %s THEN 1 ELSE 2 END
                    LIMIT 1;
                """, (model_id, model_id, model_id, model_id))
                row = cur.fetchone()
                if row and os.path.exists(os.path.join(row[0], "genai_config.json")):
                    return row[0]
    except Exception as e:
        logger.warning(f"Error querying model path for {model_id}: {e}")
    return None


def load_model_instance(model_id: str, config: Optional[Dict[str, Any]] = None, get_pg_conn: Optional[Callable[[], Any]] = None) -> Optional[Dict[str, Any]]:
    """Loads ONNX GenAI model and tokenizer into memory with caching."""
    if not OG_AVAILABLE:
        logger.error("onnxruntime_genai is not installed or unavailable on this system.")
        return None

    with _model_lock:
        if model_id in loaded_models:
            return loaded_models[model_id]

        path = get_model_path(model_id, config, get_pg_conn)
        if not path:
            logger.error(f"Model path not found for: {model_id}")
            return None

        logger.info(f"Loading ONNX model into memory: {model_id} from {path}...")
        try:
            model = og.Model(path)
            tokenizer = og.Tokenizer(model)
            loaded_models[model_id] = {"model": model, "tokenizer": tokenizer, "path": path}
            return loaded_models[model_id]
        except Exception as e:
            logger.error(f"Failed to load ONNX model {model_id}: {e}")
            return None


def unload_all_models() -> int:
    """Unloads all cached ONNX models from RAM and forces garbage collection."""
    with _model_lock:
        unloaded_count = len(loaded_models)
        loaded_models.clear()
        gc.collect()
        logger.info(f"AI memory freed: unloaded {unloaded_count} model(s) from RAM.")
        return unloaded_count


def get_loaded_models() -> List[str]:
    """Returns the IDs of all models currently resident in RAM."""
    with _model_lock:
        return list(loaded_models.keys())


def log_chat_interaction(
    model_id: str,
    role: str,
    content: str,
    client_ip: str,
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
    get_pg_conn: Optional[Callable[[], Any]] = None
):
    """Logs conversation interaction telemetry into PostgreSQL."""
    try:
        with _get_db_conn(get_pg_conn) as pg_conn:
            with pg_conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ai_chat_logs (model_id, role, content, client_ip, prompt_tokens, completion_tokens)
                    VALUES (%s, %s, %s, %s, %s, %s);
                """, (model_id, role, content, client_ip, prompt_tokens, completion_tokens))
                pg_conn.commit()
    except Exception as e:
        logger.warning(f"Failed to log chat interaction: {e}")


def migrate_legacy_duckdb_models(config: Optional[Dict[str, Any]] = None, get_pg_conn: Optional[Callable[[], Any]] = None):
    """One-time migration of existing models from models.duckdb to PostgreSQL if empty."""
    cfg = config or _MODULE_CONFIG
    legacy_path = Path(cfg.get("storage", {}).get("db_cache_dir", "/cluster/cache")) / "models.duckdb"
    if not legacy_path.exists():
        return

    try:
        with _get_db_conn(get_pg_conn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM ai_models;")
                count = cur.fetchone()[0]
                if count == 0:
                    logger.info(f"Importing legacy models from {legacy_path} to PostgreSQL...")
                    d_conn = duckdb.connect(str(legacy_path), read_only=True)
                    rows = d_conn.execute("SELECT id, owned_by, directory_path, variant_name FROM models").fetchall()
                    d_conn.close()
                    for r in rows:
                        cur.execute("""
                            INSERT INTO ai_models (id, owned_by, directory_path, variant_name, updated_at)
                            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                            ON CONFLICT (id) DO UPDATE SET directory_path = EXCLUDED.directory_path;
                        """, r)
                    conn.commit()
                    logger.info(f"Successfully migrated {len(rows)} models into PostgreSQL ai_models.")
    except Exception as e:
        logger.warning(f"Could not migrate legacy models from {legacy_path}: {e}")


def build_chatml_prompt(messages: List[Dict[str, str]]) -> str:
    """Constructs a ChatML prompt formatted string from an OpenAI message array."""
    prompt = ""
    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        if role in {"system", "user", "assistant"}:
            prompt += f"<|im_start|>{role}\n{content}<|im_end|>\n"
    prompt += "<|im_start|>assistant\n"
    return prompt


def generate_chat_completion(
    model_id: str,
    messages: List[Dict[str, str]],
    max_tokens: int = 256,
    temperature: float = 0.7,
    repetition_penalty: float = 1.1,
    client_ip: str = "127.0.0.1",
    stream: bool = False,
    config: Optional[Dict[str, Any]] = None,
    get_pg_conn: Optional[Callable[[], Any]] = None
) -> Union[Dict[str, Any], Generator[str, None, None]]:
    """
    Executes local inference with ONNX Runtime GenAI.
    Returns a dict response payload for standard mode or a Generator yielding SSE chunks for streaming.
    """
    if not OG_AVAILABLE:
        raise RuntimeError("ONNX Runtime GenAI is not available on this system.")

    cfg = config or _MODULE_CONFIG
    instance = load_model_instance(model_id, cfg, get_pg_conn)
    if not instance:
        raise ValueError(f"Model '{model_id}' could not be loaded.")

    model = instance["model"]
    tokenizer = instance["tokenizer"]

    # Log incoming user message
    for msg in messages:
        if msg.get("role") in {"system", "user"}:
            log_chat_interaction(model_id, msg.get("role", "user"), msg.get("content", ""), client_ip, get_pg_conn=get_pg_conn)

    prompt = build_chatml_prompt(messages)
    tokens = tokenizer.encode(prompt)
    context_limit = cfg.get("ai", {}).get("default_context_length", 2048)
    total_max_len = min(len(tokens) + max_tokens, context_limit)

    params = og.GeneratorParams(model)
    params.set_search_options(
        max_length=total_max_len,
        temperature=temperature,
        repetition_penalty=repetition_penalty
    )
    params.input_ids = tokens

    if stream:
        def stream_generator():
            generator = og.Generator(model, params)
            full_response = ""
            while not generator.is_done():
                generator.compute_logits()
                generator.generate_next_token()
                new_token = generator.get_next_tokens()[0]
                token_text = tokenizer.decode(new_token)
                full_response += token_text

                chunk = {
                    "choices": [{
                        "index": 0,
                        "delta": {"content": token_text},
                        "finish_reason": None
                    }]
                }
                yield f"data: {json.dumps(chunk)}\n\n"

            prompt_toks = len(tokens)
            comp_toks = len(tokenizer.encode(full_response))
            log_chat_interaction(model_id, "assistant", full_response, client_ip, prompt_toks, comp_toks, get_pg_conn=get_pg_conn)
            yield "data: [DONE]\n\n"

        return stream_generator()

    # Non-streaming execution
    generator = og.Generator(model, params)
    output_text = ""
    while not generator.is_done():
        generator.compute_logits()
        generator.generate_next_token()
        new_token = generator.get_next_tokens()[0]
        output_text += tokenizer.decode(new_token)

    completion_tokens = len(tokenizer.encode(output_text))
    prompt_tokens = len(tokens)
    log_chat_interaction(model_id, "assistant", output_text, client_ip, prompt_tokens, completion_tokens, get_pg_conn=get_pg_conn)

    return {
        "id": "chatcmpl-local",
        "object": "chat.completion",
        "model": model_id,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": output_text},
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens
        }
    }


class AIEngine:
    """Class wrapper for AI Model Management and Inference Services."""

    def __init__(self, config: Dict[str, Any], get_pg_conn_fn: Optional[Callable[[], Any]] = None):
        self.config = config
        set_config(config)
        if get_pg_conn_fn:
            set_pg_connection_factory(get_pg_conn_fn)

    @property
    def is_available(self) -> bool:
        return OG_AVAILABLE

    def sync_models(self):
        sync_models_to_db(self.config)

    def list_models(self) -> List[Dict[str, Any]]:
        return get_detailed_models_list(self.config)

    def get_model_path(self, model_id: str) -> Optional[str]:
        return get_model_path(model_id, self.config)

    def load_model(self, model_id: str) -> Optional[Dict[str, Any]]:
        return load_model_instance(model_id, self.config)

    def unload_all(self) -> int:
        return unload_all_models()

    def get_loaded_models(self) -> List[str]:
        return get_loaded_models()

    def chat_completion(
        self,
        model_id: str,
        messages: List[Dict[str, str]],
        max_tokens: int = 256,
        temperature: float = 0.7,
        stream: bool = False,
        client_ip: str = "127.0.0.1"
    ):
        rep_penalty = self.config.get("ai", {}).get("repetition_penalty", 1.1)
        return generate_chat_completion(
            model_id=model_id,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
            repetition_penalty=rep_penalty,
            client_ip=client_ip,
            stream=stream,
            config=self.config
        )
