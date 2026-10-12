# app.py
import os
from flask import Flask, request, jsonify, Response, stream_with_context
import onnxruntime_genai as og
import duckdb

app = Flask(__name__)

DB_PATH = "/cluster/models.duckdb"

def init_db():
    """Initializes DuckDB tables for models registry and future chat logs."""
    conn = duckdb.connect(DB_PATH)
    
    # Models registry table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS models (
            id VARCHAR PRIMARY KEY,
            owned_by VARCHAR,
            directory_path VARCHAR,
            variant_name VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Future chat log table placeholder
    conn.execute("""
        CREATE TABLE IF NOT EXISTS chat_logs (
            log_id VARCHAR,
            model_id VARCHAR,
            role VARCHAR,
            content TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.close()

def sync_models_to_db():
    """Dynamically scans filesystem for models, determining ownership and clean IDs without hardcoding org names."""
    init_db()
    conn = duckdb.connect(DB_PATH)
    
    BASE_DIR = "/cluster/RV2-AI"
    if not os.path.exists(BASE_DIR):
        conn.close()
        return

    for root, dirs, files in os.walk(BASE_DIR):
        if ".cache" in root or ".git" in root:
            continue
            
        onnx_files = [f for f in files if f.endswith(".onnx")]
        has_config = "config.json" in files
        
        # Calculate path relative to /cluster/RV2-AI
        rel_path = os.path.relpath(root, BASE_DIR)
        if rel_path == ".":
            continue
            
        path_parts = rel_path.split(os.sep)
        
        # Scenario 1: It's a nested layout (e.g., org/model_name/onnx/ or org/model_name/)
        # If path has at least 2 parts (e.g., ['onnx-community', 'nsfw_image_detection-ONNX', 'onnx'])
        if len(path_parts) >= 2:
            hf_org = path_parts[0] # e.g., 'onnx-community', 'microsoft', etc.
            model_repo = path_parts[1] # e.g., 'nsfw_image_detection-ONNX'
            
            # If we are inside an 'onnx' subfolder containing the actual weights
            if onnx_files and path_parts[-1] == "onnx":
                for onnx_file in onnx_files:
                    variant = os.path.splitext(onnx_file)[0]
                    custom_id = f"{model_repo}:{variant}"
                    owned_by = hf_org
                    
                    conn.execute("""
                        INSERT INTO models (id, owned_by, directory_path, variant_name)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT (id) DO UPDATE SET 
                            directory_path = EXCLUDED.directory_path,
                            owned_by = EXCLUDED.owned_by
                    """, (custom_id, owned_by, root, variant))
            
            # If the model repo root itself contains config or weights
            elif (has_config or onnx_files) and len(path_parts) == 2:
                for onnx_file in onnx_files:
                    variant = os.path.splitext(onnx_file)[0]
                    custom_id = f"{model_repo}:{variant}"
                    owned_by = hf_org
                    
                    conn.execute("""
                        INSERT INTO models (id, owned_by, directory_path, variant_name)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT (id) DO UPDATE SET 
                            directory_path = EXCLUDED.directory_path,
                            owned_by = EXCLUDED.owned_by
                    """, (custom_id, owned_by, root, variant))

        # Scenario 2: Direct top-level folders under RV2-AI (e.g., RV2-AI/qwen2-int4-0.5b)
        elif len(path_parts) == 1 and (has_config or onnx_files):
            folder_name = path_parts[0]
            if folder_name != "onnx-community": # safety catch if top-level is an org folder
                custom_id = folder_name
                owned_by = "Orange PI"
                
                conn.execute("""
                    INSERT INTO models (id, owned_by, directory_path, variant_name)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT (id) DO UPDATE SET 
                        directory_path = EXCLUDED.directory_path,
                        owned_by = EXCLUDED.owned_by
                """, (custom_id, owned_by, root, folder_name))
                
    conn.close()

def discover_models():
    """Scans for model folders and individual ONNX variants to register them as distinct choices."""
    available_models = []
    for base_dir in MODEL_ROOTS:
        if not os.path.exists(base_dir):
            continue
        for root, dirs, files in os.walk(base_dir):
            # Skip hidden metadata/cache folders
            if ".cache" in root or ".git" in root:
                continue
            
            # Check for .onnx files in the current directory
            onnx_files = [f for f in files if f.endswith(".onnx")]
            if onnx_files:
                rel_base = os.path.relpath(root, "/cluster")
                
                # If ONNX files are in an 'onnx/' subfolder or root model folder, 
                # expose each variant or the folder itself
                for onnx_file in onnx_files:
                    variant_name = os.path.splitext(onnx_file)[0]
                    if rel_base == ".":
                        model_id = variant_name
                    else:
                        # Clean up path representation
                        clean_path = rel_base.replace("\\", "/")
                        model_id = f"{clean_path}/{variant_name}"
                    available_models.append(model_id)
            
            # Also register the parent directory if it has a config.json (standard generation model format)
            if "config.json" in files:
                rel_path = os.path.relpath(root, "/cluster").replace("\\", "/")
                if rel_path not in available_models:
                    available_models.append(rel_path)
                    
    return sorted(list(set(available_models)))


def load_model_instance(model_id):
    """Lazy loads the model and tokenizer into memory to save RAM."""
    if model_id in loaded_models:
        return loaded_models[model_id]
    
    path = get_model_path(model_id)
    if not path:
        return None

    print(f"Loading model into memory: {model_id} from {path}...")
    model = og.Model(path)
    tokenizer = og.Tokenizer(model)
    loaded_models[model_id] = {"model": model, "tokenizer": tokenizer}
    return loaded_models[model_id]

@app.route("/v1/models", methods=["GET"])
def list_models():
    """Retrieves available models dynamically from DuckDB registry."""
    sync_models_to_db() # Syncs any newly downloaded models automatically
    
    conn = duckdb.connect(DB_PATH, read_only=True)
    rows = conn.execute("SELECT id, owned_by FROM models").fetchall()
    conn.close()
    
    data = []
    for row in rows:
        data.append({
            "id": row[0],
            "object": "model",
            "owned_by": row[1],
            "permission": []
        })
    return jsonify({"object": "list", "data": data})

def get_model_path(model_id):
    """Queries DuckDB to resolve the clean model id to its physical path."""
    conn = duckdb.connect(DB_PATH, read_only=True)
    res = conn.execute("SELECT directory_path, variant_name FROM models WHERE id = ?", [model_id]).fetchone()
    conn.close()
    
    if not res:
        return None
        
    dir_path, variant = res
    # If it points to a specific variant file inside an onnx folder
    potential_file = os.path.join(dir_path, f"{variant}.onnx")
    if os.path.exists(potential_file):
        return potential_file
        
    return dir_path

@app.route("/v1/chat/completions", methods=["POST"])
def chat_completions():
    """OpenAI-compatible chat completions endpoint."""
    body = request.json or {}
    model_id = body.get("model")
    messages = body.get("messages", [])
    stream = body.get("stream", False)
    max_tokens = body.get("max_tokens", 256)
    temperature = body.get("temperature", 0.7)

    if not model_id:
        return jsonify({"error": {"message": "Model ID is required", "type": "invalid_request_error"}}), 400

    instance = load_model_instance(model_id)
    if not instance:
        return jsonify({"error": {"message": f"Model '{model_id}' not found", "type": "invalid_request_error"}}), 404

    model = instance["model"]
    tokenizer = instance["tokenizer"]

# Construct proper chat template using Qwen / ChatML formatting
    prompt = ""
    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        if role == "system":
            prompt += f"<|im_start|>system\n{content}<|im_end|>\n"
        elif role == "user":
            prompt += f"<|im_start|>user\n{content}<|im_end|>\n"
        elif role == "assistant":
            prompt += f"<|im_start|>assistant\n{content}<|im_end|>\n"
    prompt += "<|im_start|>assistant\n"

    tokens = tokenizer.encode(prompt)
    params = og.GeneratorParams(model)
    params.set_search_options(max_length=max_tokens, temperature=temperature, repetition_penalty=1.1)
    params.input_ids = tokens

    generator = og.Generator(model, params)
    output_text = ""
    while not generator.is_done():
        generator.compute_logits()
        generator.generate_next_token()
        new_token = generator.get_next_tokens()[0]
        output_text += tokenizer.decode(new_token)

    response_payload = {
        "id": "chatcmpl-local",
        "object": "chat.completion",
        "model": model_id,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": output_text},
            "finish_reason": "stop"
        }],
        "usage": {"prompt_tokens": len(tokens), "completion_tokens": 0, "total_tokens": len(tokens)}
    }
    return jsonify(response_payload)


def log_chat_interaction(model_id, role, content):
    """Appends chat history records into DuckDB."""
    conn = duckdb.connect(DB_PATH)
    conn.execute(
        "INSERT INTO chat_logs (model_id, role, content) VALUES (?, ?, ?)",
        [model_id, role, content]
    )
    conn.close()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
