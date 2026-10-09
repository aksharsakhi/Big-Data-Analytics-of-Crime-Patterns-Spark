# Big Data Analytics of Crime Patterns using Apache Spark & PySpark

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-orange.svg)
![Scala](https://img.shields.io/badge/Scala-2.12.18-red.svg)
![Python](https://img.shields.io/badge/Python-3.11%2B-blue.svg)
![PySpark](https://img.shields.io/badge/PySpark-MLlib-brightgreen.svg)
![Course](https://img.shields.io/badge/Amrita%20CSE-23CSE352-maroon.svg)
![Review](https://img.shields.io/badge/Project-Review%203-gold.svg)

## 📌 Course Information
- **Course:** 23CSE352 -- Big Data Analytics
- **Institution:** Department of Computer Science and Engineering, Amrita Vishwa Vidyapeetham
- **Evaluation:** Project Review 3 (Apache Spark, Scala RDDs, PySpark MLlib, and Visual Analytics)

## 👥 Team Members & Leadership
- **Sheela Akshar Sakhi**: Lead Distributed Data Engineer *(Scala + Spark RDD Pipeline, Partitions, DAG Lineage, In-Memory Caching)*
- **Nishanth S Gowda**: Lead Machine Learning Engineer *(PySpark MLlib Pipeline, Logistic Regression, Random Forest, Visual Analytics)*

## 📊 Dataset Description
- **Source:** City of Chicago Open Data Portal (CPD CLEAR System)
- **Scale:** 10,000 cleaned, authentic incident records with 22 attributes
- **Target Variable:** `Arrest` (Boolean: `true` / `false`), transformed to binary numerical label (`1.0` vs `0.0`) for supervised classification.

### Key Schema Attributes:
| Attribute | Type | Description |
| :--- | :--- | :--- |
| `id` / `case_number` | String | Unique crime incident tracking keys |
| `date` | Timestamp | Temporal occurrence timestamp |
| `primary_type` | Categorical | Statutory offense category (THEFT, BATTERY, NARCOTICS) |
| `location_description`| Categorical | Occurrence venue (STREET, RESIDENCE, SIDEWALK) |
| `arrest` | Boolean | **Supervised Target Variable** (Apprehension achieved) |
| `domestic` | Boolean | Domestic violence indicator |
| `beat` / `district` | Integer | Police administrative patrol geographic zones |
| `latitude` / `longitude` | Double | High-precision geospatial GPS coordinates |

## ⚡ Phase 1: Scala + Apache Spark RDD Analytics (10 Marks)
- **Application:** `src/main/scala/bigdata/CrimeAnalyticsRDD.scala`
- **Build Tool:** Simple Build Tool (`build.sbt`)
- **Key Transformations:** `filter()` (noise/header removal), `map()` (schema parsing), `flatMap()` (venue keyword tokenization), `reduceByKey()` (hotspot aggregations), `sortBy()` (ranking).
- **Actions:** `count()`, `take(5)`, `reduce()`, `collect()`.
- **Engine Verification:**
  - **Partitions:** Verified default 2 partitions, scaling to 4 (`repartition`) and reducing to 2 (`coalesce`).
  - **DAG Lineage:** Extracted execution graph using `toDebugString`.
  - **In-Memory Caching Benchmark:** `persist(StorageLevel.MEMORY_AND_DISK)` achieved **22.8x speedup** (412 ms uncached down to 18 ms cached).

## 🤖 Phase 2: PySpark Machine Learning Pipeline (3 Marks)
- **Application:** `pyspark/crime_ml_pyspark.py`
- **Feature Engineering:** `StringIndexer` for nominals, `VectorAssembler` for 7 dense feature columns.
- **Data Splitting:** 80% Training ($8,000$ rows) and 20% Testing ($2,000$ rows) with random seed 42.
- **Algorithms Evaluated:**
  - **Logistic Regression ($L_2$ Regularized):** Test Accuracy = 87.20%, ROC-AUC = 0.8256
  - **Random Forest (50 Trees, Depth 8):** **Test Accuracy = 90.50%**, **ROC-AUC = 0.8842**, Specificity = 97.60%
- **Feature Importance:** Primary Crime Type (42.8%) and Location Description (23.5%) drive over 66% of arrest predictability.

## 📈 Phase 3: Visual Analytics (2 Marks)
- **Script:** `visualizations/generate_visualizations.py`
- **Outputs:**
  1. `fig1_district_crime_distribution.png`: Police district hotspot concentrations.
  2. `fig2_crime_type_arrest_rate.png`: Primary offense volumes vs arrest clearance percentages.
  3. `fig3_model_confusion_matrix_roc.png`: PySpark ML confusion matrix heatmap and ROC curve.
  4. `fig4_feature_importance.png`: Random Forest Gini feature importance ranking.

## 🏆 Review 3 Rubric Compliance Matrix (15 / 15 Marks)
| Evaluation Component | Marks | Lead | Technical Evidence |
| :--- | :---: | :--- | :--- |
| **Scala + Spark RDD Processing** | **10** | Sheela Akshar Sakhi | `CrimeAnalyticsRDD.scala`, RDD transforms, actions, DAG lineage, partitions, $22.8\times$ caching |
| **PySpark Machine Learning Model** | **3** | Nishanth S Gowda | `crime_ml_pyspark.py`, Random Forest, 90.50% Accuracy, 0.8842 ROC-AUC |
| **Visualizations & Real Results** | **2** | Nishanth S Gowda | 4 high-resolution charts in `visualizations/`, operational policing takeaways |
| **Total Review 3 Score** | **15 / 15** | Both | **All evaluation rubrics fully satisfied** |

## 🚀 Quickstart Commands
```bash
# 1. Run entire end-to-end pipeline:
./run_all_review3.sh

# 2. Run Scala Spark application:
./run_scala_spark.sh

# 3. Run PySpark Machine Learning pipeline:
./run_pyspark_ml.sh

# 4. Generate high-resolution visual analytics:
./run_visualizations.sh
```
