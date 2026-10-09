#!/usr/bin/env bash
# End-to-end integration test runner for Review 3
set -e

echo "=== Running Dataset Validation ==="
python3 scripts/validate_dataset.py

echo "=== Running Parser Unit Tests ==="
python3 scripts/test_parser.py

echo "=== Running Memory Profiler ==="
python3 scripts/profile_memory.py

echo "=== Running Visual Analytics ==="
python3 visualizations/generate_visualizations.py

echo "[ALL INTEGRATION TESTS PASSED]"
