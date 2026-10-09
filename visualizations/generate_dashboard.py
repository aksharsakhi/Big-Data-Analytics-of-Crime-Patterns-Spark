#!/usr/bin/env python3
"""
Executive Summary Dashboard Generator.
Compiles 4 analytical charts into a unified 2x2 presentation dashboard.
Author: Nishanth S Gowda <nsgxi43@gmail.com>
"""

import os
import matplotlib.pyplot as plt
import matplotlib.image as mpimg

VIZ_DIR = os.path.dirname(__file__)

def generate_dashboard():
    fig, axs = plt.subplots(2, 2, figsize=(16, 11), dpi=200)
    fig.suptitle("City of Chicago Crime Patterns - Big Data Analytics Executive Dashboard\nCourse: 23CSE352 | Review 3 | Apache Spark & PySpark MLlib", fontsize=16, fontweight='bold', y=0.98)

    figs = [
        ("fig1_district_crime_distribution.png", "A. District Crime Hotspot Concentration"),
        ("fig2_crime_type_arrest_rate.png", "B. Crime Category Volume vs Arrest Clearance"),
        ("fig3_model_confusion_matrix_roc.png", "C. Machine Learning Evaluation (Confusion Matrix & ROC)"),
        ("fig4_feature_importance.png", "D. Random Forest Feature Importance Weights")
    ]

    for idx, (fname, title) in enumerate(figs):
        row = idx // 2
        col = idx % 2
        img_path = os.path.join(VIZ_DIR, fname)
        if os.path.exists(img_path):
            img = mpimg.imread(img_path)
            axs[row, col].imshow(img)
            axs[row, col].axis('off')
            axs[row, col].set_title(title, fontsize=12, fontweight='bold', pad=8)

    plt.tight_layout()
    out_file = os.path.join(VIZ_DIR, "executive_crime_analytics_dashboard.png")
    plt.savefig(out_file, bbox_inches='tight', dpi=200)
    print(f"[OK] Generated executive dashboard: {out_file}")

if __name__ == "__main__":
    generate_dashboard()
