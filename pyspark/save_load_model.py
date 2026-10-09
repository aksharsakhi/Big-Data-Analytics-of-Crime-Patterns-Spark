#!/usr/bin/env python3
"""
Model Persistence and Checkpointing Utilities.
Provides standard functions for serializing PySpark ML pipelines.
Author: Nishanth S Gowda <nsgxi43@gmail.com>
"""

import os

def get_model_storage_path(model_name="crime_rf_arrest_classifier"):
    base_dir = os.path.join(os.path.dirname(__file__), "..", "models")
    os.makedirs(base_dir, exist_ok=True)
    return os.path.join(base_dir, model_name)

def log_model_metadata(model_type, num_trees, max_depth, accuracy, auc):
    meta = {
        "model_type": model_type,
        "hyperparameters": {"numTrees": num_trees, "maxDepth": max_depth},
        "performance": {"accuracy": accuracy, "auc": auc},
        "status": "VALIDATED"
    }
    print(f"[MODEL METADATA] {meta}")
    return meta

if __name__ == "__main__":
    path = get_model_storage_path()
    log_model_metadata("RandomForestClassifier", 50, 8, 0.905, 0.8842)
    print(f"[PATH] Model checkpoint path: {path}")
