# test_model.py
import onnxruntime as ort
import numpy as np
import os

# Choose one of your local models from /cluster
model_path = "/cluster/RV2-AI/qwen2-int4-1.5b/model.onnx"

print(f"Checking model path: {model_path}")
if os.path.exists(model_path):
    print("Model file found successfully!")
            
    # Initialize ONNX Runtime session 
    # (If using the vendor's specialized execution provider like KyExecutionProvider, include it in providers)
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
