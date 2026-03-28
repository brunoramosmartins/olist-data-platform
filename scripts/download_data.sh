#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# download_data.sh — Download Olist Brazilian E-Commerce dataset from Kaggle
# ============================================================================
#
# Prerequisites:
#   1. Install Kaggle CLI: pip install kaggle
#   2. Place your API token at ~/.kaggle/kaggle.json
#      (Download from https://www.kaggle.com/settings → "Create New Token")
#
# Usage:
#   bash scripts/download_data.sh
# ============================================================================

DATA_DIR="data/raw"
DATASET="olistbr/brazilian-ecommerce"

echo ">>> Downloading Olist dataset..."
mkdir -p "${DATA_DIR}"

kaggle datasets download -d "${DATASET}" -p "${DATA_DIR}" --unzip

echo ""
echo ">>> Dataset files:"
ls -lh "${DATA_DIR}"/*.csv

echo ""
echo "============================================"
echo "  Download complete!"
echo "  Files saved to: ${DATA_DIR}/"
echo "============================================"
