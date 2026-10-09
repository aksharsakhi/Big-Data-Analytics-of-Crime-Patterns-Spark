#!/bin/bash
# ==============================================================================
# 23CSE352: Big Data Analytics - Project Review 3
# Script: Run Scala + Spark RDD Processing Pipeline
# ==============================================================================

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

echo "=================================================================="
echo " 23CSE352: Big Data Analytics - Project Review 3"
echo " Step 2: Apache Spark & Scala RDD Pipeline Execution"
echo "=================================================================="

DATASET_FILE="dataset/chicago_crimes_clean.csv"
if [ ! -f "$DATASET_FILE" ]; then
    echo "[ERROR] Clean dataset file not found at $DATASET_FILE"
    exit 1
fi

echo "[✓] Dataset located: $DATASET_FILE ($(wc -l < "$DATASET_FILE") rows)"

# Check if spark-shell or spark-submit is available
if command -v spark-submit &>/dev/null; then
    echo "[✓] Found spark-submit: $(which spark-submit)"
    
    # Check if sbt is available to package
    if command -v sbt &>/dev/null; then
        echo "[*] Packaging Scala project with sbt..."
        sbt package
        JAR_FILE=$(find target/scala-2.12 -name "*.jar" 2>/dev/null | head -n 1)
        if [ -n "$JAR_FILE" ]; then
            echo "[*] Submitting to spark-submit: $JAR_FILE"
            spark-submit --class bigdata.CrimeAnalyticsRDD --master local[*] "$JAR_FILE" "$DATASET_FILE"
            exit 0
        fi
    fi
    
    # Alternatively compile directly with scalac if available
    if command -v scalac &>/dev/null; then
        echo "[*] Compiling with scalac..."
        mkdir -p target/classes
        scalac -cp "$(spark-submit --version 2>&1 >/dev/null || true):$SPARK_HOME/jars/*" -d target/classes src/main/scala/bigdata/CrimeAnalyticsRDD.scala
        jar -cvf target/crime_analytics_rdd.jar -C target/classes .
        spark-submit --class bigdata.CrimeAnalyticsRDD --master local[*] target/crime_analytics_rdd.jar "$DATASET_FILE"
        exit 0
    fi
fi

if command -v spark-shell &>/dev/null; then
    echo "[✓] Found spark-shell. Executing interactive Scala script..."
    spark-shell --master local[*] -i src/main/scala/bigdata/spark_rdd_script.scala
    exit 0
fi

echo "[*] Neither spark-submit nor spark-shell was found in local PATH."
echo "    On your University VM, run:"
echo "      spark-shell -i src/main/scala/bigdata/spark_rdd_script.scala"
echo "    OR"
echo "      sbt run"
