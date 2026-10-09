# PySpark MLlib Predictive Analytics Pipeline

**Author & Module Lead:** Nishanth S Gowda (`nsgxi43@gmail.com`)  
**Course:** 23CSE352 -- Big Data Analytics (Project Review 3)

---

## 📌 Problem Formulation
Supervised binary classification forecasting incident apprehension outcome:
$$\hat{y} = f(\vec{x}) \in \{0.0, 1.0\}$$

### Feature Vector Formulation:
$$\vec{x} = [\text{TypeIdx}, \text{LocIdx}, \text{Domestic}, \text{Beat}, \text{District}, \text{Ward}, \text{Community}]^T$$

## 🏆 Model Performance Summary:
- **Logistic Regression ($L_2$ Regularized):**
  - Accuracy: **87.20%** | ROC-AUC: **0.8256**
- **Random Forest ($50$ Trees, Depth $8$):**
  - Accuracy: **90.50%** | Weighted F1: **89.60%** | ROC-AUC: **0.8842**
  - Specificity: **97.60%** ($1,708$ True Negatives vs $42$ False Positives)

## 📁 Module Scripts:
- `crime_ml_pyspark.py`: Main MLlib feature engineering and model training pipeline.
- `tune_hyperparameters.py`: Grid search exploration over tree depth and count.
- `cross_validate.py`: 3-Fold cross-validation stability analysis.
- `benchmark_inference.py`: Prediction throughput and latency benchmarking.
- `check_class_imbalance.py`: Incident clearance class distribution audit.
