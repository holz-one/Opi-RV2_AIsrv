import os
import onnxruntime_genai as og

# Point directly to your pre-converted model folder
model_path = "/cluster/RV2-AI/qwen2-int4-1.5b"

if not os.path.exists(model_path):
    print(f"Model directory not found at {model_path}")
    exit(1)

print(f"Loading model from {model_path}...")
model = og.Model(model_path)
tokenizer = og.Tokenizer(model)
print("Model loaded successfully!")

# Simple chat loop
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
