#!/usr/bin/env bash
# Production cluster submission script for Apache Spark
set -e

SPARK_MASTER=${1:-"local[*]"}
JAR_PATH="target/scala-2.12/chicago-crime-analytics-spark_2.12-1.0.jar"

echo "=========================================================="
echo " Submitting Spark Job to Cluster: $SPARK_MASTER"
echo "=========================================================="

if [ ! -f "$JAR_PATH" ]; then
    echo "JAR not found. Building with sbt package..."
    sbt package
fi

spark-submit \
  --class bigdata.CrimeAnalyticsRDD \
  --master "$SPARK_MASTER" \
  --driver-memory 2G \
  --executor-memory 2G \
  --executor-cores 2 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  "$JAR_PATH"

echo "Cluster submission completed successfully."
