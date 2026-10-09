#!/usr/bin/env python3
"""
Feature Correlation Heatmap Generator.
Computes Pearson correlation coefficients across numerical attributes.
"""

import os
import pandas as pd
import numpy as np

DATASET = os.path.join(os.path.dirname(__file__), "..", "dataset", "chicago_crimes_clean.csv")

def compute_correlation():
    df = pd.read_csv(DATASET)
    num_cols = ["beat", "district", "ward", "community_area", "year"]
    available = [c for c in num_cols if c in df.columns]
    corr = df[available].corr()
    print("[PEARSON CORRELATION MATRIX]")
    print(corr.round(3))

if __name__ == "__main__":
    compute_correlation()
