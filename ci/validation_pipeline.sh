#!/usr/bin/env bash
# Continuous Integration Pipeline Validator for Review 3
set -e

echo "=== [CI STEP 1] Validating Dataset Integrity ==="
python3 scripts/validate_dataset.py

echo "=== [CI STEP 2] Running Parser Unit Tests ==="
python3 scripts/test_parser.py

echo "=== [CI STEP 3] Testing High-Res Visual Analytics ==="
python3 visualizations/generate_visualizations.py

echo "[CI RESULT] All test suites passed successfully."
