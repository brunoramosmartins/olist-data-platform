#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# setup_env.sh — Bootstrap Python virtual environment and install dependencies
# ============================================================================

VENV_DIR=".venv"

echo ">>> Creating virtual environment in ${VENV_DIR}..."
python3 -m venv "${VENV_DIR}"

echo ">>> Activating virtual environment..."
source "${VENV_DIR}/bin/activate"

echo ">>> Installing project dependencies..."
make install

echo ">>> Verifying dbt installation..."
cd dbt_project && dbt debug && cd ..

echo ""
echo "============================================"
echo "  Environment ready!"
echo "  Activate with: source ${VENV_DIR}/bin/activate"
echo "============================================"
