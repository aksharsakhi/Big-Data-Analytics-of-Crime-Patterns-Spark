#!/usr/bin/env bash
# Automated PySpark ML Pipeline Validation Test Suite
# Author: Nishanth S Gowda <nsgxi43@gmail.com>
set -e

echo "=== [STEP 1] Testing Class Imbalance Audit ==="
python3 pyspark/check_class_imbalance.py

echo "=== [STEP 2] Testing Hyperparameter Grid Search ==="
python3 pyspark/tune_hyperparameters.py

echo "=== [STEP 3] Testing 3-Fold Cross-Validation ==="
python3 pyspark/cross_validate.py

echo "=== [STEP 4] Testing Inference Benchmark ==="
python3 pyspark/benchmark_inference.py

echo "[SUCCESS] PySpark ML pipeline validation test suite passed 100%."
