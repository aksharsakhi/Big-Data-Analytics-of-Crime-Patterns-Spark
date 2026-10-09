#!/usr/bin/env python3
"""
Class Imbalance Diagnostic for Crime Arrest Classification.
Author: Nishanth S Gowda <nsgxi43@gmail.com>
"""

import os
import pandas as pd

DATASET = os.path.join(os.path.dirname(__file__), "..", "dataset", "chicago_crimes_clean.csv")

def audit_class_imbalance():
    df = pd.read_csv(DATASET)
    counts = df["arrest"].value_counts()
    total = len(df)
    print("[CLASS IMBALANCE AUDIT]")
    print(f"Total Records: {total}")
    for label, count in counts.items():
        pct = (count / total) * 100
        label_str = "Arrest (1)" if str(label).lower() == "true" else "No Arrest (0)"
        print(f"  {label_str:<15}: {count} ({pct:.2f}%)")
    ratio = counts[False] / counts[True]
    print(f"Imbalance Ratio: {ratio:.2f}:1 (Open Cases vs Arrests)")
    print("Recommendation: Use ROC-AUC and Weighted F1-score rather than simple accuracy.")

if __name__ == "__main__":
    audit_class_imbalance()
