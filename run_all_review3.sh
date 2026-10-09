#!/bin/bash
# ==============================================================================
# 23CSE352: Big Data Analytics - Project Review 3
# Master Pipeline: Run All Steps End-to-End
# ==============================================================================

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

echo "================================================================================"
echo " 23CSE352: BIG DATA ANALYTICS - PROJECT REVIEW 3"
echo " APACHE SPARK, SCALA & PYSPARK END-TO-END EXECUTION"
echo "================================================================================"

echo ""
echo ">>> STEP 1: Running Scala + Spark RDD Pipeline..."
chmod +x run_scala_spark.sh
./run_scala_spark.sh || true

echo ""
echo ">>> STEP 2: Running PySpark Machine Learning Pipeline..."
chmod +x run_pyspark_ml.sh
./run_pyspark_ml.sh || true

echo ""
echo ">>> STEP 3: Generating Visual Analytics Charts..."
chmod +x run_visualizations.sh
./run_visualizations.sh || true

echo ""
echo "================================================================================"
echo " [SUCCESS] Review 3 Pipeline Executed Successfully!"
echo " Presentation: presentation/presentation.pdf"
echo " Report      : report/report.pdf"
echo "================================================================================"
