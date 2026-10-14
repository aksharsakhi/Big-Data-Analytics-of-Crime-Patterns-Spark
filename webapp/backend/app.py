"""
Flask Web Application Backend for Big Data Crime Analytics.
Provides REST API endpoints for database querying, ML predictions, RDD simulation,
and static frontend serving.
"""

import os
import sys
from flask import Flask, jsonify, request, send_from_directory

# Ensure backend modules are on path
sys.path.insert(0, os.path.dirname(__file__))

import database
import ml_engine
import rdd_engine

FRONTEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "frontend"))

app = Flask(__name__, static_folder=FRONTEND_DIR, static_url_path="")

# Initialize database on startup
database.init_db()

@app.route("/")
def index():
    return send_from_directory(FRONTEND_DIR, "index.html")

@app.route("/api/kpis")
def get_kpis():
    data = database.get_kpis()
    return jsonify(data)

@app.route("/api/hotspots")
def get_hotspots():
    limit = request.args.get("limit", default=10, type=int)
    data = database.get_district_hotspots(limit=limit)
    return jsonify({"districts": data})

@app.route("/api/crime-types")
def get_crime_types():
    limit = request.args.get("limit", default=12, type=int)
    data = database.get_crime_type_breakdown(limit=limit)
    return jsonify({"crime_types": data})

@app.route("/api/crimes")
def get_crimes():
    q = request.args.get("q", default="", type=str)
    district = request.args.get("district", default="", type=str)
    primary_type = request.args.get("primary_type", default="", type=str)
    arrest = request.args.get("arrest", default="", type=str)
    limit = request.args.get("limit", default=25, type=int)
    offset = request.args.get("offset", default=0, type=int)

    result = database.search_crimes(
        query=q,
        district=district,
        primary_type=primary_type,
        arrest=arrest,
        limit=min(100, max(1, limit)),
        offset=max(0, offset)
    )
    return jsonify(result)

@app.route("/api/crimes/<incident_id>")
def get_crime_by_id(incident_id):
    rec = database.get_crime_by_id(incident_id)
    if not rec:
        return jsonify({"error": "Incident ID not found"}), 404
    return jsonify({"incident": rec})

@app.route("/api/predict", methods=["POST"])
def predict():
    payload = request.get_json(force=True) if request.is_json else request.form.to_dict()
    if not payload:
        return jsonify({"error": "No input data provided"}), 400

    result = ml_engine.predict_arrest(payload)
    return jsonify(result)

@app.route("/api/diagnostics")
def get_diagnostics():
    data = ml_engine.get_model_diagnostics()
    return jsonify(data)

@app.route("/api/rdd-engine")
def get_rdd_engine():
    data = rdd_engine.get_rdd_pipeline_metadata()
    return jsonify(data)

@app.route("/api/rubric")
def get_rubric():
    return jsonify({
        "course": "23CSE352: Big Data Analytics",
        "review": "Project Review 3 (15 Marks)",
        "project_title": "Big Data Analytics of Crime Patterns using Apache Spark & PySpark",
        "team_members": [
            {
                "name": "Sheela Akshar Sakhi",
                "roll_no": "CB.SC.U4CSE23547",
                "specialization": "Distributed Data Engineering Lead",
                "rubric_assigned": "Scala + Spark RDD Analytics (10 Marks)",
                "key_artifacts": ["CrimeAnalyticsRDD.scala", "spark_rdd_script.scala", "build.sbt", "run_scala_spark.sh"]
            },
            {
                "name": "Nishanth S Gowda",
                "roll_no": "CB.SC.U4CSE23538",
                "specialization": "Machine Learning & Visuals Lead",
                "rubric_assigned": "PySpark ML (3 Marks) + Visual Analytics (2 Marks)",
                "key_artifacts": ["crime_ml_pyspark.py", "generate_visualizations.py", "run_pyspark_ml.sh", "run_visualizations.sh"]
            }
        ],
        "rubrics": [
            {
                "criteria": "Scala + Spark RDD Processing & Architecture",
                "marks": 10,
                "lead": "Sheela Akshar Sakhi",
                "evidence": "RDD creation, transformations (filter, map, flatMap), reduceByKey, actions (count, take), partitions, DAG lineage, 22.8x in-memory cache"
            },
            {
                "criteria": "PySpark Machine Learning Model",
                "marks": 3,
                "lead": "Nishanth S Gowda",
                "evidence": "StringIndexer + VectorAssembler (7 features), Random Forest vs Logistic Regression, 90.50% Acc, 0.8842 ROC-AUC, Confusion Matrix"
            },
            {
                "criteria": "Data Visualization & Real-World Results",
                "marks": 2,
                "lead": "Nishanth S Gowda",
                "evidence": "4 high-resolution analytical figures, Chicago district hotspot map, clearance rates, interactive full-stack app"
            }
        ]
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    print(f"\n==================================================================")
    print(f" 🚀 Crime Analytics Full-Stack Application Running!")
    print(f" 👉 Open in Browser: http://localhost:{port}")
    print(f"==================================================================\n")
    app.run(host="0.0.0.0", port=port, debug=False)
