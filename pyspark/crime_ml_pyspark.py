import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
#!/usr/bin/env python3
"""
23CSE352: Big Data Analytics - Project Review 3
PySpark Machine Learning Pipeline for Chicago Crime Pattern Analytics
Task: Binary Classification Predicting Arrest Probability (Arrest = 1 vs 0)

Demonstrates:
1. Feature Preparation (StringIndexer, VectorAssembler)
2. Train/Test Split (80/20)
3. Model Training: Logistic Regression & Random Forest Classifier
4. Evaluation: Accuracy, Precision, Recall, F1-Score, and ROC-AUC
5. Confusion Matrix & Feature Importances Output
"""

import sys
import os
import json
import time

def run_pyspark_pipeline(csv_path="dataset/chicago_crimes_clean.csv", output_dir="visualizations"):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, when, hour, to_timestamp
    from pyspark.ml.feature import StringIndexer, VectorAssembler
    from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
    from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

    print("================================================================================")
    print(" 23CSE352: Big Data Analytics - Project Review 3")
    print(" PySpark Machine Learning: Crime Arrest Prediction Pipeline")
    print(" Source Dataset: " + csv_path)
    print("================================================================================\n")

    spark = SparkSession.builder \
        .appName("ChicagoCrimeArrestPredictionML") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        # Step 1: Ingest Data
        print("[STEP 1] Loading CSV Dataset into Spark DataFrame...")
        df_raw = spark.read.csv(csv_path, header=True, inferSchema=True)
        total_records = df_raw.count()
        print(f"         -> Total records loaded: {total_records:,}")

        # Step 2: Preprocessing & Feature Engineering
        print("\n[STEP 2] Preprocessing & Feature Engineering...")
        # Cast boolean flags to integers (0 or 1)
        df_clean = df_raw.filter(col("arrest").isNotNull() & col("primary_type").isNotNull()) \
            .withColumn("label", when(col("arrest") == "true", 1.0).otherwise(0.0)) \
            .withColumn("domestic_num", when(col("domestic") == "true", 1.0).otherwise(0.0)) \
            .withColumn("beat_num", col("beat").cast("double")) \
            .withColumn("district_num", col("district").cast("double")) \
            .withColumn("ward_num", col("ward").cast("double")) \
            .withColumn("comm_area_num", col("community_area").cast("double"))

        # String indexing for high-cardinality categorical variables
        indexer_crime = StringIndexer(inputCol="primary_type", outputCol="primary_type_idx", handleInvalid="keep")
        indexer_loc = StringIndexer(inputCol="location_description", outputCol="location_desc_idx", handleInvalid="keep")

        df_indexed = indexer_crime.fit(df_clean).transform(df_clean)
        df_indexed = indexer_loc.fit(df_indexed).transform(df_indexed)

        # Feature vector assembly
        feature_cols = [
            "primary_type_idx",
            "location_desc_idx",
            "domestic_num",
            "beat_num",
            "district_num",
            "ward_num",
            "comm_area_num"
        ]

        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
        df_ml = assembler.transform(df_indexed).select("features", "label", "primary_type", "district")

        arrest_count = df_ml.filter(col("label") == 1.0).count()
        unapprehended_count = df_ml.filter(col("label") == 0.0).count()
        print(f"         -> Features assembled: {feature_cols}")
        print(f"         -> Label distribution: Arrested = {arrest_count:,} ({arrest_count/total_records*100:.2f}%), Open = {unapprehended_count:,}")

        # Step 3: Train / Test Split (80 / 20)
        print("\n[STEP 3] Splitting Dataset into Training (80%) and Testing (20%) Sets...")
        train_df, test_df = df_ml.randomSplit([0.8, 0.2], seed=42)
        print(f"         -> Training set count: {train_df.count():,}")
        print(f"         -> Testing set count : {test_df.count():,}")

        # Evaluators
        eval_acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
        eval_prec = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision")
        eval_rec = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall")
        eval_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
        eval_roc = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")

        # Step 4: Model 1 - Logistic Regression
        print("\n[STEP 4] Training Model 1: Logistic Regression...")
        t0 = time.time()
        lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=20, regParam=0.1)
        lr_model = lr.fit(train_df)
        lr_train_time = time.time() - t0

        lr_preds = lr_model.transform(test_df)
        lr_acc = eval_acc.evaluate(lr_preds)
        lr_prec = eval_prec.evaluate(lr_preds)
        lr_rec = eval_rec.evaluate(lr_preds)
        lr_f1 = eval_f1.evaluate(lr_preds)
        lr_roc = eval_roc.evaluate(lr_preds)

        print(f"         -> Training Time: {lr_train_time:.2f} seconds")
        print(f"         -> Accuracy     : {lr_acc*100:.2f}%")
        print(f"         -> Precision    : {lr_prec*100:.2f}%")
        print(f"         -> Recall       : {lr_rec*100:.2f}%")
        print(f"         -> F1-Score     : {lr_f1*100:.2f}%")
        print(f"         -> ROC-AUC      : {lr_roc:.4f}")

        # Step 5: Model 2 - Random Forest Classifier
        print("\n[STEP 5] Training Model 2: Random Forest Classifier...")
        t0 = time.time()
        rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=50, maxDepth=8, seed=42)
        rf_model = rf.fit(train_df)
        rf_train_time = time.time() - t0

        rf_preds = rf_model.transform(test_df)
        rf_acc = eval_acc.evaluate(rf_preds)
        rf_prec = eval_prec.evaluate(rf_preds)
        rf_rec = eval_rec.evaluate(rf_preds)
        rf_f1 = eval_f1.evaluate(rf_preds)
        rf_roc = eval_roc.evaluate(rf_preds)

        print(f"         -> Training Time: {rf_train_time:.2f} seconds")
        print(f"         -> Accuracy     : {rf_acc*100:.2f}%")
        print(f"         -> Precision    : {rf_prec*100:.2f}%")
        print(f"         -> Recall       : {rf_rec*100:.2f}%")
        print(f"         -> F1-Score     : {rf_f1*100:.2f}%")
        print(f"         -> ROC-AUC      : {rf_roc:.4f}")

        # Compute Confusion Matrix for Random Forest
        tp = rf_preds.filter((col("label") == 1.0) & (col("prediction") == 1.0)).count()
        fp = rf_preds.filter((col("label") == 0.0) & (col("prediction") == 1.0)).count()
        tn = rf_preds.filter((col("label") == 0.0) & (col("prediction") == 0.0)).count()
        fn = rf_preds.filter((col("label") == 1.0) & (col("prediction") == 0.0)).count()

        print("\n[STEP 6] Random Forest Confusion Matrix:")
        print(f"         +---------------------+---------------------+")
        print(f"         | TN: {tn:15d} | FP: {fp:15d} |")
        print(f"         +---------------------+---------------------+")
        print(f"         | FN: {fn:15d} | TP: {tp:15d} |")
        print(f"         +---------------------+---------------------+")

        # Feature Importances
        importances = rf_model.featureImportances.toArray().tolist()
        feat_imp_dict = dict(zip(feature_cols, importances))
        print("\n[STEP 7] Feature Importance Ranking (Random Forest):")
        sorted_feats = sorted(feat_imp_dict.items(), key=lambda x: x[1], reverse=True)
        for rank, (feat, imp) in enumerate(sorted_feats, 1):
            print(f"         {rank}. {feat:<20} : {imp*100:6.2f}%")

        # Export metrics for visualization & report
        os.makedirs(output_dir, exist_ok=True)
        metrics_file = os.path.join(output_dir, "ml_metrics_summary.json")
        summary_payload = {
            "total_records": total_records,
            "train_count": train_df.count(),
            "test_count": test_df.count(),
            "logistic_regression": {
                "accuracy": round(lr_acc, 4),
                "precision": round(lr_prec, 4),
                "recall": round(lr_rec, 4),
                "f1_score": round(lr_f1, 4),
                "roc_auc": round(lr_roc, 4),
                "training_time_sec": round(lr_train_time, 2)
            },
            "random_forest": {
                "accuracy": round(rf_acc, 4),
                "precision": round(rf_prec, 4),
                "recall": round(rf_rec, 4),
                "f1_score": round(rf_f1, 4),
                "roc_auc": round(rf_roc, 4),
                "training_time_sec": round(rf_train_time, 2),
                "confusion_matrix": {"tn": tn, "fp": fp, "fn": fn, "tp": tp},
                "feature_importance": feat_imp_dict
            }
        }

        with open(metrics_file, "w") as f:
            json.dump(summary_payload, f, indent=4)
        print(f"\n[✓] Saved ML metrics summary to: {metrics_file}")

        print("\n================================================================================")
        print(" [SUCCESS] PySpark Machine Learning Pipeline Completed Successfully!")
        print("================================================================================")

    finally:
        spark.stop()

if __name__ == "__main__":
    csv_file = sys.argv[1] if len(sys.argv) > 1 else "dataset/chicago_crimes_clean.csv"
    run_pyspark_pipeline(csv_file)
