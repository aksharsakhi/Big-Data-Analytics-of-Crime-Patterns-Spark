#!/bin/bash
# ==============================================================================
# 23CSE352: Big Data Analytics - Project Review 3
# Script: Run Automated Visual Analytics Generator
# ==============================================================================

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

echo "=================================================================="
echo " 23CSE352: Big Data Analytics - Project Review 3"
echo " Step 4: Generating High-Resolution Visual Analytics"
echo "=================================================================="

if [ -f ".venv/bin/python" ]; then
    .venv/bin/python visualizations/generate_visualizations.py
elif command -v python3 &>/dev/null; then
    python3 visualizations/generate_visualizations.py
else
    echo "[ERROR] Python 3 not found."
    exit 1
fi

echo "[✓] Visual analytics generated in visualizations/ directory."
