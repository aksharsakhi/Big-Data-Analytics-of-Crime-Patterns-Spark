# Visual Analytics & Empirical Law Enforcement Findings

**Author & Module Lead:** Nishanth S Gowda (`nsgxi43@gmail.com`)  
**Course:** 23CSE352 -- Big Data Analytics (Project Review 3)

---

## 📊 Catalog of Analytical Figures:

### 1. `fig1_district_crime_distribution.png`
- **Focus:** Geospatial crime concentration across Chicago's 22 police districts.
- **Key Finding:** District 012 (Near West) and District 011 (Harrison) represent the primary municipal hotspots ($>12.5\%$ combined volume).

### 2. `fig2_crime_type_arrest_rate.png`
- **Focus:** Primary statutory offense volume vs verified arrest clearance percentage.
- **Key Finding:** Theft and Battery exceed $40\%$ of all reports but maintain clearance rates $<11\%$.

### 3. `fig3_model_confusion_matrix_roc.png`
- **Focus:** Diagnostic evaluation of Random Forest arrest classifier.
- **Key Finding:** $\text{ROC-AUC} = 0.8842$ proving high discriminatory accuracy across thresholds.

### 4. `fig4_feature_importance.png`
- **Focus:** Gini impurity importance ranking of model input features.
- **Key Finding:** Primary Crime Type ($42.8\%$) and Location Description ($23.5\%$) account for over $66\%$ of arrest predictability.
