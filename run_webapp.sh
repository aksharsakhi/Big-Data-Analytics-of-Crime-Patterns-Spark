#!/bin/bash
# ==============================================================================
# 23CSE352: Big Data Analytics - Project Review 3
# Script: Launch Full-Stack Web Application (Frontend + Backend + DB + ML)
# ==============================================================================

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

echo "=================================================================="
echo " 23CSE352: Big Data Analytics - Project Review 3"
echo " Launching Full-Stack Application: CrimeLens Spark"
echo " (Frontend UI + Flask REST API + SQLite Warehouse + PySpark ML)"
echo "=================================================================="

PYTHON_CMD="python3"
if [ -f ".venv/bin/python" ]; then
    PYTHON_CMD=".venv/bin/python"
elif [ -f "../.venv/bin/python" ]; then
    PYTHON_CMD="../.venv/bin/python"
fi

export PORT=5001

echo "[*] Using Python: $PYTHON_CMD"
echo "[*] Initializing SQLite Data Warehouse & REST Services..."

# Launch server
$PYTHON_CMD webapp/backend/app.py
