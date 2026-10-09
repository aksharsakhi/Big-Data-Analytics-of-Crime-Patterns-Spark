# Crime Analytics Engine: API Reference Guide

### 23CSE352: Big Data Analytics -- Project Review 3
**Amrita Vishwa Vidyapeetham -- Department of CSE**

---

## 1. Scala Core RDD API (`CrimeAnalyticsRDD.scala`)

### Class: `bigdata.CrimeAnalyticsRDD`
Main entrypoint object encapsulating the distributed RDD processing lifecycle.

#### Methods:
- `parseCsvLine(line: String): Array[String]`
- `main(args: Array[String]): Unit`

---

## 2. PySpark MLlib API (`crime_ml_pyspark.py`)

### Module: `pyspark.crime_ml_pyspark`
Supervised binary classification pipeline for incident arrest prediction.
