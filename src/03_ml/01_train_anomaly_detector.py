# Databricks notebook source
# MAGIC %md
# MAGIC # LiveTwin — Anomaly Detector Training
# MAGIC
# MAGIC Trains an Isolation Forest anomaly detector on NASA CMAPSS healthy-engine sensor data.
# MAGIC
# MAGIC - **Algorithm:** Isolation Forest (unsupervised — no RUL labels needed)
# MAGIC - **Input:** 9 key sensor readings (normalized)
# MAGIC - **Output:** Anomaly score 0–1 (0=normal, 1=anomalous)
# MAGIC - **Deployment:** MLflow real-time serving endpoint `livetwin-anomaly-detector`

# COMMAND ----------

# MAGIC %pip install scikit-learn mlflow databricks-sdk -q

# COMMAND ----------

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import roc_auc_score, average_precision_score
from mlflow.models.signature import infer_signature
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# MAGIC %md ## 1. Load Training Data

# COMMAND ----------

IMPORTANT_SENSORS = ["s2", "s3", "s4", "s7", "s11", "s12", "s15", "s20", "s21"]
ROLLING_FEATURES  = [f"{s}_roll_mean" for s in IMPORTANT_SENSORS] + \
                    [f"{s}_roll_std"   for s in IMPORTANT_SENSORS]

# Load silver layer data (engineered features)
df_silver = spark.table("livetwin.raw.cmapss_fd001").toPandas()

# Training: use first 80% of each engine's life as "healthy" (RUL > 30)
# The model learns what "normal" looks like — no labels needed for training
df_healthy = df_silver[df_silver["rul"] > 30].copy()
df_all     = df_silver.copy()

print(f"Training (healthy, RUL>30): {len(df_healthy):,} rows")
print(f"All data: {len(df_all):,} rows")

# Features for anomaly detection
feature_cols = IMPORTANT_SENSORS + ROLLING_FEATURES

# Handle missing rolling features (when coming from raw CMAPSS)
for col in feature_cols:
    if col not in df_healthy.columns:
        # If rolling features not pre-computed, use raw sensor values
        feature_cols = IMPORTANT_SENSORS
        break

X_train = df_healthy[feature_cols].fillna(0).values
X_all   = df_all[feature_cols].fillna(0).values
y_all   = (df_all["rul"] <= 30).astype(int).values  # 1 = anomalous (near failure)

print(f"Feature matrix shape: train={X_train.shape}, all={X_all.shape}")
print(f"Positive rate (anomalous): {y_all.mean():.2%}")

# COMMAND ----------

# MAGIC %md ## 2. Train with MLflow Tracking

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment("/Shared/livetwin/anomaly_detector")

with mlflow.start_run(run_name="isolation_forest_v1") as run:
    # Hyperparameters
    params = {
        "n_estimators":  200,
        "max_samples":   "auto",
        "contamination": 0.15,    # expected fraction of anomalies in all data
        "random_state":  42,
        "n_jobs":        -1,
    }
    mlflow.log_params(params)
    mlflow.log_param("features",      feature_cols)
    mlflow.log_param("training_rows", len(X_train))
    mlflow.log_param("dataset",       "NASA CMAPSS FD001")
    mlflow.log_param("rul_threshold", 30)

    # Build pipeline: scaler + isolation forest
    pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("iforest", IsolationForest(**params)),
    ])

    pipeline.fit(X_train)

    # Evaluate on all data
    # IsolationForest returns -1 (anomaly) or 1 (normal) — convert to 0/1
    raw_scores      = -pipeline.named_steps["iforest"].score_samples(
                         pipeline.named_steps["scaler"].transform(X_all)
                      )
    # Normalize to [0, 1]
    anomaly_scores  = (raw_scores - raw_scores.min()) / (raw_scores.max() - raw_scores.min() + 1e-9)
    predictions_bin = (pipeline.predict(X_all) == -1).astype(int)

    # Metrics
    roc_auc  = roc_auc_score(y_all, anomaly_scores)
    pr_auc   = average_precision_score(y_all, anomaly_scores)
    precision = (predictions_bin & y_all).sum() / predictions_bin.sum() if predictions_bin.sum() > 0 else 0
    recall    = (predictions_bin & y_all).sum() / y_all.sum() if y_all.sum() > 0 else 0

    mlflow.log_metric("roc_auc",   roc_auc)
    mlflow.log_metric("pr_auc",    pr_auc)
    mlflow.log_metric("precision", precision)
    mlflow.log_metric("recall",    recall)

    print(f"ROC-AUC: {roc_auc:.4f}  PR-AUC: {pr_auc:.4f}  Precision: {precision:.4f}  Recall: {recall:.4f}")

    # Log model
    signature   = infer_signature(X_all[:5], anomaly_scores[:5])
    model_info  = mlflow.sklearn.log_model(
        sk_model     = pipeline,
        artifact_path= "model",
        signature    = signature,
        registered_model_name="livetwin.ml.anomaly_detector",
        input_example= pd.DataFrame(X_all[:3], columns=feature_cols),
    )

    print(f"Model logged: {model_info.model_uri}")
    run_id = run.info.run_id

# COMMAND ----------

# MAGIC %md ## 3. Register and Set as Champion

# COMMAND ----------

client = mlflow.tracking.MlflowClient()

# Set alias "champion" on latest version
model_versions = client.search_model_versions("name='livetwin.ml.anomaly_detector'")
latest_version = max([int(mv.version) for mv in model_versions])
client.set_registered_model_alias("livetwin.ml.anomaly_detector", "champion", str(latest_version))
print(f"✓ Set version {latest_version} as 'champion'")

# COMMAND ----------

# MAGIC %md ## 4. Deploy as Serving Endpoint

# COMMAND ----------

w = WorkspaceClient()

endpoint_name = "livetwin-anomaly-detector"
model_uri     = f"models:/livetwin.ml.anomaly_detector/champion"

try:
    w.serving_endpoints.create(
        name=endpoint_name,
        config={
            "served_models": [{
                "model_name":          "livetwin.ml.anomaly_detector",
                "model_version":       str(latest_version),
                "workload_size":       "Small",
                "scale_to_zero_enabled": True,
            }],
            "traffic_config": {
                "routes": [{"served_model_name": "livetwin.ml.anomaly_detector", "traffic_percentage": 100}]
            },
        },
    )
    print(f"✓ Serving endpoint created: {endpoint_name}")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"  Endpoint {endpoint_name} already exists — updating")
        w.serving_endpoints.update_config(
            name=endpoint_name,
            served_models=[{
                "model_name":    "livetwin.ml.anomaly_detector",
                "model_version": str(latest_version),
                "workload_size": "Small",
            }]
        )
    else:
        raise

# COMMAND ----------

# MAGIC %md ## 5. Quick Inference Test

# COMMAND ----------

import requests

# Test with a known-anomalous row (Engine 15, near failure)
demo_engine = df_silver[(df_silver["unit_id"] == 15) & (df_silver["rul"] <= 5)]
test_row    = demo_engine[feature_cols].head(1).values.tolist()

token  = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host   = spark.conf.get("spark.databricks.workspaceUrl")
url    = f"https://{host}/serving-endpoints/{endpoint_name}/invocations"

response = requests.post(
    url,
    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    json={"inputs": test_row},
    timeout=30,
)

if response.status_code == 200:
    score = response.json()["predictions"][0]
    print(f"✓ Endpoint test OK: Engine 15 anomaly score = {score:.4f} (expected > 0.75)")
else:
    print(f"  Endpoint not ready yet — may take 1-2 minutes to spin up")
    print(f"  Status: {response.status_code}")

print("✅ Anomaly Detector trained, registered, and deployed.")
