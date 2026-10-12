# Orange Pi RV2 (RISC-V) AI Environment Setup Guide with `uv`

This guide covers setting up an isolated Python environment on the Orange Pi RV2 running Ubuntu over SSH, configuring vendor-supplied `riscv64` ONNX Runtime wheels, resolving common RISC-V dependency pitfalls (such as NumPy 2.x ABI conflicts), and testing local INT4 LLM inference.

---
## 0. Install Ubuntu for Risc V

[Ubuntu developer images now available for OrangePi RV2: a low-cost RISC-V SBC](https://canonical.com/blog/ubuntu-developer-images-now-available-for-orangepi-rv2-a-low-cost-risc-v-sbc)

### My Tips

- I tried 3 SD Cards that failed before I could get the system to boot
- use a microSDXC SD card, mine had 3D NAND and UHS-I U3 ratings
- Don't Install SSD drives until after first boot
- After you have a working system, then install your SSD drives, only bottom is bootable
- Use the orangepi-settings to clone your system to the bottom NVME drive
- The system gets hot, get a case with a fan and heat sinks
- A good 5 Volt PSU with at least 4 amps, I'm using my Orange Pi 5 Plus PSU for this now
- GUI Apps for RISC-V are limited, Google doen't work in chromiumm
- Add a swap partition or file, good for building python modules like duckdb or pandas

#### Swap File
```bash
sudo fallocate -l 8G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

#### /etc/fstab
```bash
/swapfile none swap sw 0 0
```
*/swapfile* can be replaced with the UUID or LABEL of your swap partition, dev names have changed on be with this system, so ids listed in *sudo blkid* work best.

## 1. Environment Preparation & System Dependencies

Before creating a Python environment, install essential C/C++ development tools and Python headers needed for compiling native extensions on RISC-V.

```bash
sudo apt update
sudo apt install -y python3-dev build-essential cmake
```

## 2. Virtual Environment Setup using *uv*

Install UV:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh

```

Navigate to your workspace directory (e.g., */cluster*) and set up an isolated virtual environment using *uv*.
My second NVME drive is mounted on */cluster* 

```bash
cd /cluster

# Create the virtual environment
uv venv .venv

# Activate the virtual environment
source .venv/bin/activate

```


Note on File System Hardlinks: If you see a warning regarding hardlinks (*UV_LINK_MODE*), you can append export *UV_LINK_MODE=copy* to your *~/.bashrc* file.

Also if your using a terminal like Kitty, you might want to add *export TERM=xterm* so pasting in vim doesn't act up and mess with formating

## 3. Resolving RISC-V Binary Wheel Dependencies

### A. The RISC-V PyPI Limitation

Standard PyPI does not host pre-compiled *onnxruntime* or *onnxruntime-genai* binaries for *manylinux_2_39_riscv64*. Attempting to install directly from PyPI via *uv pip install onnxruntime* will fail or attempt a long C++ source build.

### B. Installing Vendor RISC-V Wheels

Use the pre-compiled *.whl* binaries provided with the RV2 SDK image or board repository.

[Orange Pi RV2 Tools](https://drive.google.com/drive/folders/1QTmKSqsByvn0gi1MpbzK7FnjPt4H6dI5) has the extra python modules and select onnx models for local AI.  *ky-ort.riskv64.1.2.2.tar.gz* has what you need.

```bash
# Locate local wheel files on your system
find / -iname '*.whl' 2>/dev/null

# Install vendor wheels using uv
uv pip install /path/to/onnxruntime_genai-0.4.0.dev0-cp312-cp312-linux_riscv64.whl
uv pip install /path/to/ky_ort-*.whl

```

### C. Pinning NumPy to 1.x (<2)

Pre-compiled C++ *onnxruntime* bindings for RISC-V were built against **NumPy 1.x C-API**. Installing NumPy 2.x will trigger runtime *ImportError* crashes. Force *numpy<2* to ensure stability:

```bash
uv pip install "numpy<2"
```

## 4. Verification & Testing

### A. Testing Model Graph Loading (*test_model.py*)

Create a script to verify that *onnxruntime* can parse a model's ONNX graph and input tensors.

```python
# test_model.py
import os
import onnxruntime as ort

model_path = "/cluster/RV2-AI/qwen2-int4-1.5b/model.onnx"

print(f"Checking model path: {model_path}")
if os.path.exists(model_path):
    print("Model file found successfully!")
    
    providers = ["CPUExecutionProvider"]
    if "KyExecutionProvider" in ort.get_available_providers():
        providers.insert(0, "KyExecutionProvider")

    print(f"Loading session with providers: {providers}")
    session = ort.InferenceSession(model_path, providers=providers)
    
    print("Model loaded successfully into ONNX Runtime!")
    print("Inputs:")
    for input_node in session.get_inputs():
        print(f"  - Name: {input_node.name}, Shape: {input_node.shape}, Type: {input_node.type}")
else:
    print(f"Model path not found: {model_path}")
```

Run the verification test:

```bash
uv run python test_model.py

```

### B. Running Text Generation Inference (*run_chat.py*)

Create an interactive test script using *onnxruntime-genai* to run inference on quantized local models (such as *qwen2-int4-1.5b*):

```python
# run_chat.py
import os
import onnxruntime_genai as og

model_path = "/cluster/RV2-AI/qwen2-int4-1.5b"

if not os.path.exists(model_path):
    print(f"Model directory not found at {model_path}")
    exit(1)

print(f"Loading model from {model_path}...")
model = og.Model(model_path)
tokenizer = og.Tokenizer(model)
print("Model loaded successfully!")

# Format chat prompt
prompt = "<|im_start|>user\nHello! Introduce yourself briefly.<|im_end|>\n<|im_start|>assistant\n"
tokens = tokenizer.encode(prompt)

params = og.GeneratorParams(model)
params.set_search_options(max_length=256, temperature=0.7)
params.input_ids = tokens

generator = og.Generator(model, params)

print("\n--- Generating response ---")
while not generator.is_done():
    generator.compute_logits()
    generator.generate_next_token()
    new_token = generator.get_next_tokens()[0]
    print(tokenizer.decode(new_token), end="", flush=True)
print("\n---------------------------")

```

Run the chat generator:

```bash
uv run python run_chat.py

```

### C. Add more modules to RISC-V system

**Install Flask:**

```bash
uv pip install flask
```

**Install DuckDB:**

- You need to add a swap file or partition to build this, it fails to build with only 4GB ram.

```bash
uv pip install duckdb
```

*duckdb-cli* is not avalable on RISC-V yet.



### 5. Troubleshooting Reference

| Symptom / Error | Cause | Fix |
| :--- | :--- | :--- |
| `No solution found when resolving dependencies` for `onnxruntime` | PyPI lacks `riscv64` pre-compiled wheels. | Do not use PyPI `onnxruntime`. Install local board-specific `.whl` files. |
| `ImportError: A module that was compiled using NumPy 1.x cannot be run in NumPy 2.x` | `onnxruntime` C++ bindings require NumPy 1.x ABI. | Run `uv pip install "numpy<2"`. |
| `Could NOT find Python3 (missing: Development.Module)` when building ONNX | Missing system Python development headers. | Run `sudo apt install python3-dev build-essential cmake`. |

### 6. Hugging Face and More ONNX models

If you want more onnx models and don't want to convert them, check out [huggingface onnx-community](https://huggingface.co/onnx-community), create an account and generate a token too.

```bash
uv pip install huggingface_hub
```

Download models using:

```bash
# Download a model repository directly into a local folder
hf download onnx-community/Qwen2.5-0.5B-Instruct-ONNX --local-dir /cluster/onnx-community/Qwen2.5-0.5B-Instruct-ONNX
```

Somw models will require a login, so you'll need a token you generated on your hugging face setting page

```bash
huggingface-cli login
```

### 6. Increasing memory and making headless

OpenSSH isn't installed by default, so it will need to be installed to manage it remotly

```bash
sudo apt install openssh-server
sudo systemctl enable ssh
sudo systemctl start ssh
```

Stop Desktop enviroment

```bash
sudo systemctl disable gdm
sudo systemctl stop gdm

```

Stop Docker
```bash
sudo systemctl disable docker 
sudo systemctl stop docker
sudo systemctl disable docker.socket
sudo systemctl stop docker.socket
sudo systemctl disable containerd
sudo systemctl stop containerd

```
