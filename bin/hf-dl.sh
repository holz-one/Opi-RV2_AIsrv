#!/usr/bin/env bash
set -e

# Verify an argument was provided
if [ -z "$1" ]; then
    echo "Error: No model repository specified."
    echo "Usage: hf-dl.sh <org/model-name>"
    echo "Example: hf-dl.sh onnx-community/Qwen2.5-0.5B-Instruct-ONNX"
    exit 1
fi

REPO_ID="$1"
LOCAL_DIR="/cluster/RV2-AI/$REPO_ID"

echo "Downloading repository: $REPO_ID"
echo "Destination: $LOCAL_DIR"

# Ensure huggingface_hub CLI is available
if ! command -v hf &> /dev/null; then
    echo "Error: 'hf' command not found. Make sure huggingface_hub is installed in your active environment."
    exit 1
fi

# Execute download
hf download "$REPO_ID" --local-dir "$LOCAL_DIR"

echo "Download complete!"
