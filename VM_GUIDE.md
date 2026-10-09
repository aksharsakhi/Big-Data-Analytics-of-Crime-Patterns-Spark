# University VM Execution & Screenshot Guide: Project Review 3 (Apache Spark)

This guide walks you through executing the **Apache Spark, Scala & PySpark** project on your University Virtual Machine (`hadoop@aksharsakhi-QEMU-Virtual-Machine`) and details **where and when to capture the required screenshots**.

---

## Step 1: Copy or Pull the Code to Your VM

On your Ubuntu VM terminal:
```bash
cd ~/BigData/Projects/
# If cloning via git:
git clone https://github.com/aksharsakhi/Big-Data-Analytics-of-Crime-Patterns-Spark.git
cd Big-Data-Analytics-of-Crime-Patterns-Spark
```
*(Alternatively, copy the `Big-Data-Analytics-of-Crime-Patterns-Spark` folder directly from your Mac to the VM).*

---

## Step 2: Verify Spark, Scala & Python Environment

Ensure Apache Spark and Scala are in your PATH:
```bash
spark-shell --version
spark-submit --version
python3 --version
```

---

## Step 3: Run Scala + Spark RDD Processing & Capture Screenshots

### Option A: Interactive spark-shell Execution (Easiest & Visual)
```bash
spark-shell --master local[*] -i src/main/scala/bigdata/spark_rdd_script.scala
```

📸 **SCREENSHOT 1: Scala RDD Transformations & Cache Benchmark**
- **Terminal output showing:**
  1. Initial Partitions count.
  2. Filter, Map, and FlatMap execution.
  3. Cache Benchmark comparing un-cached vs cached runtime (speedup).

📸 **SCREENSHOT 2: Key-Value Aggregation & RDD Lineage DAG**
- **Terminal output showing:**
  1. `reduceByKey()` crime frequency results per District.
  2. `toDebugString` showing the execution stages, shuffle boundaries, and DAG lineage.

---

## Step 4: Run PySpark Machine Learning Pipeline & Capture Screenshot

Execute the automated Machine Learning script:
```bash
spark-submit --master local[*] pyspark/crime_ml_pyspark.py dataset/chicago_crimes_clean.csv
```

📸 **SCREENSHOT 3: PySpark Machine Learning Execution & Metrics**
- **Terminal output showing:**
  1. Feature vector assembly with `StringIndexer` and `VectorAssembler`.
  2. Train/Test split (80/20).
  3. Logistic Regression vs. Random Forest model comparison (Accuracy, Precision, Recall, F1, ROC-AUC).
  4. Confusion Matrix and Feature Importance ranking.

---

## Step 5: Generate Visualizations & Compile Presentation / Report

Generate the publication figures and compile the final LaTeX PDF deliverables:
```bash
./run_visualizations.sh
```

To compile the LaTeX presentation and report on your Mac:
```bash
# Presentation (12 Slides):
tectonic presentation/presentation.tex

# Academic Report (15-20 Pages):
tectonic report/report.tex
```

---

## Summary of Rubric Alignment (15 Marks)

| Rubric Component | Marks | Files & Evidence |
|---|---|---|
| **Scala + Spark RDD Processing & Spark Concepts** | **10** | `src/main/scala/bigdata/CrimeAnalyticsRDD.scala`, `spark_rdd_script.scala`: RDD, transformations (`filter`, `map`, `flatMap`, `reduceByKey`, `sortBy`), actions (`count`, `take`, `collect`, `reduce`), partitions, lineage (`toDebugString`), DAG, persistence (`cache`, `persist`). |
| **PySpark ML Model** | **3** | `pyspark/crime_ml_pyspark.py`: Feature engineering, train/test split, Logistic Regression vs Random Forest classifier, metrics (Accuracy, Precision, Recall, F1, ROC-AUC), Confusion matrix. |
| **Visualization & Results** | **2** | `visualizations/generate_visualizations.py`: 4 publication-quality charts depicting district hotspots, crime type clearance rates, ROC curves/Confusion matrix, and feature importances. |
| **Total** | **15/15** | **All criteria comprehensively met** |

## 🔧 Troubleshooting & Common VM Errors FAQ

### Q1: `java.lang.OutOfMemoryError: Java heap space`
- **Cause:** Single-node VM default heap is capped at 512MB.
- **Solution:** Pass `--driver-memory 2g` to `spark-submit` or `spark-shell`:
  ```bash
  spark-shell --driver-memory 2g -i src/main/scala/bigdata/spark_rdd_script.scala
  ```

### Q2: Port 4040 Already in Use (`BindException`)
- **Cause:** Previous Spark session is still active in background.
- **Solution:** Spark automatically increments to port 4041. You can access the Web UI at `http://localhost:4041`.

### Q3: File Path `FileNotFoundException`
- **Cause:** Running commands from a child directory instead of the project root.
- **Solution:** Always run from project root:
  ```bash
  cd ~/Big-Data-Analytics-of-Crime-Patterns-Spark
  ```

## ⚙️ Recommended Cluster Tuning Formulas
For distributed production clusters:
- **Number of Partitions:** $2 \times \text{Total Available Cores}$ (ensures full core utilization without scheduling overhead).
- **Executor Cores:** 4--5 cores per executor (optimal I/O throughput avoiding garbage collection stalls).
- **Driver Memory:** Minimum 2 GB for metadata tracking and small action collections.
