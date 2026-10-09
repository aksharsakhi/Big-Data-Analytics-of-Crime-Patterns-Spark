#!/bin/bash
# ==============================================================================
# 23CSE352: Big Data Analytics - Project Review 3
# Script: Run PySpark Machine Learning Pipeline
# ==============================================================================

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

echo "=================================================================="
echo " 23CSE352: Big Data Analytics - Project Review 3"
echo " Step 3: PySpark Machine Learning Execution"
echo " Task: Binary Classification for Arrest Probability"
echo "=================================================================="

DATASET_FILE="dataset/chicago_crimes_clean.csv"
if [ ! -f "$DATASET_FILE" ]; then
    echo "[ERROR] Clean dataset file not found at $DATASET_FILE"
    exit 1
fi

if command -v spark-submit &>/dev/null; then
    echo "[✓] Found spark-submit: $(which spark-submit)"
    spark-submit --master local[*] pyspark/crime_ml_pyspark.py "$DATASET_FILE"
elif command -v python3 &>/dev/null; then
    echo "[*] spark-submit not found. Attempting execution via python3 with local pyspark..."
    python3 pyspark/crime_ml_pyspark.py "$DATASET_FILE"
else
    echo "[ERROR] Neither spark-submit nor python3 found."
    exit 1
fi

echo ""
echo "=================================================================="
echo " [SUCCESS] PySpark ML Pipeline Completed!"
echo " Results and metrics saved to visualizations/ml_metrics_summary.json"
echo "=================================================================="
