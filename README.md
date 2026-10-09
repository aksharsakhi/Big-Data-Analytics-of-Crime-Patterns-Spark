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
