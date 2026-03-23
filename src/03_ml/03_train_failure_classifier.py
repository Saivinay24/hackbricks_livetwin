# Databricks notebook source
# MAGIC %md
# MAGIC # LiveTwin — Failure Mode Classifier Training
# MAGIC
# MAGIC Trains a Random Forest classifier to predict failure mode type.
# MAGIC
# MAGIC - **Algorithm:** Random Forest (multi-class classification)
# MAGIC - **Classes:** bearing_degradation, thermal_degradation, vibration, fuel_system, compressor_degradation
# MAGIC - **Input:** 9 key sensor readings + rolling features
# MAGIC - **Output:** `{failure_mode: str, confidence: float, probabilities: dict}`
# MAGIC - **Training data:** Weakly-supervised labels from CMAPSS + synthetic augmentation
# MAGIC - **Deployment:** MLflow real-time serving endpoint `livetwin-failure-classifier`

# COMMAND ----------

# MAGIC %pip install scikit-learn mlflow databricks-sdk -q

# COMMAND ----------

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
from mlflow.models.signature import infer_signature
from databricks.sdk import WorkspaceClient

FAILURE_MODES = [
    "bearing_degradation",
    "thermal_degradation",
    "vibration",
    "fuel_system",
    "compressor_degradation",
]

IMPORTANT_SENSORS = ["s2", "s3", "s4", "s7", "s11", "s12", "s15", "s20", "s21"]

# COMMAND ----------

# MAGIC %md ## 1. Generate Weakly-Supervised Labels
# MAGIC
# MAGIC NASA CMAPSS FD001 has one fault mode (HPC degradation = compressor_degradation).
# MAGIC FD003 has two fault modes. For the 5-class problem we use sensor signature rules
# MAGIC derived from the incident report database to assign labels, then train the model.

# COMMAND ----------

df_raw = spark.table("livetwin.raw.cmapss_fd001").toPandas()

# Rename sensor_X → sX if needed
if "sensor_2" in df_raw.columns:
    rename_map = {f"sensor_{i}": f"s{i}" for i in range(1, 22)}
    df_raw = df_raw.rename(columns=rename_map)

def assign_failure_label(row: pd.Series) -> str:
    """
    Rule-based weak labeling from sensor signature patterns.
    Based on patterns extracted from the 50 synthetic incident reports.

    These rules approximate domain knowledge:
    - Bearing: s2 ↑, s4 ↑, s11 ↓
    - Thermal:  s3 ↑, s4 ↑, s7 ↑
    - Vibration: s2 ↑, s20 ↑
    - Fuel: s12 ↓, s4 ↑
    - Compressor: s7 ↓, s11 ↓ (HPC degradation — primary mode in FD001)
    """
    s2, s3, s4, s7 = row.get("s2",0), row.get("s3",0), row.get("s4",0), row.get("s7",0)
    s11, s12       = row.get("s11",0), row.get("s12",0)
    s20            = row.get("s20",0)
    cycle          = row.get("cycle", 0)
    rul            = row.get("rul", 999)

    # Only label degraded engines (RUL < 50 cycles = near failure)
    if rul > 50:
        return "healthy"  # filtered out in training

    # Compressor degradation: pressure ratio drop (s7↓, s11↓, s12↑)
    if s7 < 0 and s11 < 0:
        return "compressor_degradation"

    # Thermal degradation: temperature rise (s3↑, s4↑)
    if s3 > 0 and s4 > 0 and s7 > 0:
        return "thermal_degradation"

    # Bearing degradation: temperature + pressure anomalies (s2↑, s4↑, s11↓)
    if s2 > 0 and s4 > 0 and s11 < 0:
        return "bearing_degradation"

    # Fuel system: fuel efficiency drop (s12↓)
    if s12 < 0:
        return "fuel_system"

    # Vibration: s20 elevation
    if s20 > 0:
        return "vibration"

    # Default for FD001: compressor (known single fault mode)
    return "compressor_degradation"

# COMMAND ----------

# Normalize sensors around zero (per-engine normalization)
def normalize_sensors(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for s in IMPORTANT_SENSORS:
        if s in df.columns:
            col_mean = df.groupby("unit_id")[s].transform("mean")
            col_std  = df.groupby("unit_id")[s].transform("std").replace(0, 1)
            df[f"{s}_norm"] = (df[s] - col_mean) / col_std
    return df

df_norm = normalize_sensors(df_raw)

# Use normalized values for labeling
norm_sensors = [f"{s}_norm" for s in IMPORTANT_SENSORS if f"{s}_norm" in df_norm.columns]
if norm_sensors:
    for s in IMPORTANT_SENSORS:
        if f"{s}_norm" in df_norm.columns:
            df_norm[s] = df_norm[f"{s}_norm"]  # replace with normalized for labeling

df_norm["failure_label"] = df_norm.apply(assign_failure_label, axis=1)

# Keep only degraded samples (RUL < 50) for classification
df_degraded = df_norm[df_norm["rul"] < 50].copy()
df_degraded = df_degraded[df_degraded["failure_label"] != "healthy"]

print(f"Degraded samples: {len(df_degraded):,}")
print(f"Label distribution:\n{df_degraded['failure_label'].value_counts()}")

# COMMAND ----------

# Add synthetic augmentation for underrepresented classes
# Generate noise-perturbed copies to balance classes
np.random.seed(42)
MIN_SAMPLES_PER_CLASS = 800

augmented = []
for mode in FAILURE_MODES:
    class_data = df_degraded[df_degraded["failure_label"] == mode]
    n_needed   = max(0, MIN_SAMPLES_PER_CLASS - len(class_data))
    if n_needed > 0 and len(class_data) > 0:
        extra = class_data.sample(n_needed, replace=True, random_state=42).copy()
        for s in IMPORTANT_SENSORS:
            if s in extra.columns:
                extra[s] += np.random.normal(0, 0.05, len(extra))  # small Gaussian noise
        augmented.append(extra)
        print(f"  Augmented {mode}: +{n_needed} synthetic samples")

if augmented:
    df_train_full = pd.concat([df_degraded] + augmented, ignore_index=True)
else:
    df_train_full = df_degraded.copy()

print(f"Final training set: {len(df_train_full):,} rows")
print(f"Final distribution:\n{df_train_full['failure_label'].value_counts()}")

# COMMAND ----------

# MAGIC %md ## 2. Prepare Features + Train

# COMMAND ----------

feature_cols = IMPORTANT_SENSORS + ["cycle"]
feature_cols = [c for c in feature_cols if c in df_train_full.columns]

X = df_train_full[feature_cols].fillna(0).values
y = df_train_full["failure_label"].values

le = LabelEncoder()
le.fit(FAILURE_MODES)
y_enc = le.transform(y)

X_train, X_test, y_train, y_test = train_test_split(X, y_enc, test_size=0.2, stratify=y_enc, random_state=42)
print(f"Train: {len(X_train):,}  Test: {len(X_test):,}")

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment("/Shared/livetwin/failure_classifier")

with mlflow.start_run(run_name="random_forest_v1") as run:
    params = {
        "n_estimators":   300,
        "max_depth":      12,
        "min_samples_leaf": 5,
        "class_weight":   "balanced",
        "n_jobs":         -1,
        "random_state":   42,
    }
    mlflow.log_params(params)
    mlflow.log_param("classes",   FAILURE_MODES)
    mlflow.log_param("features",  feature_cols)
    mlflow.log_param("train_rows", len(X_train))
    mlflow.log_param("dataset",   "NASA CMAPSS FD001 + synthetic augmentation")

    pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("rf",     RandomForestClassifier(**params)),
    ])
    pipeline.fit(X_train, y_train)

    # Evaluation
    y_pred      = pipeline.predict(X_test)
    y_pred_prob = pipeline.predict_proba(X_test)
    accuracy    = (y_pred == y_test).mean()

    # Per-class metrics
    report = classification_report(y_test, y_pred, target_names=le.classes_, output_dict=True)
    for mode in FAILURE_MODES:
        if mode in report:
            mlflow.log_metric(f"f1_{mode[:8]}",        report[mode]["f1-score"])
            mlflow.log_metric(f"prec_{mode[:8]}",      report[mode]["precision"])
            mlflow.log_metric(f"rec_{mode[:8]}",       report[mode]["recall"])

    mlflow.log_metric("accuracy",       accuracy)
    mlflow.log_metric("macro_f1",       report["macro avg"]["f1-score"])

    print(f"Accuracy: {accuracy:.4f}  Macro F1: {report['macro avg']['f1-score']:.4f}")
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred, target_names=le.classes_))

    # ── Custom PyFunc wrapper ─────────────────────────────────────────────────
    import mlflow.pyfunc

    class FailureClassifierModel(mlflow.pyfunc.PythonModel):
        def __init__(self, pipeline, label_encoder, feature_cols, failure_modes):
            self.pipeline      = pipeline
            self.le            = label_encoder
            self.feature_cols  = feature_cols
            self.failure_modes = failure_modes

        def predict(self, context, model_input):
            X      = model_input.values if hasattr(model_input, "values") else model_input
            preds  = self.pipeline.predict(X)
            probs  = self.pipeline.predict_proba(X)
            labels = self.le.inverse_transform(preds)
            confs  = probs.max(axis=1)

            prob_dicts = [
                {self.le.classes_[i]: float(p) for i, p in enumerate(prob_row)}
                for prob_row in probs
            ]
            return pd.DataFrame({
                "failure_mode":   labels,
                "confidence":     confs,
                "probabilities":  [str(d) for d in prob_dicts],
            })

    clf_model   = FailureClassifierModel(pipeline, le, feature_cols, FAILURE_MODES)
    sample_in   = pd.DataFrame(X_test[:3], columns=feature_cols)
    sample_out  = clf_model.predict(None, sample_in)

    signature   = infer_signature(sample_in, sample_out)
    model_info  = mlflow.pyfunc.log_model(
        artifact_path         = "model",
        python_model          = clf_model,
        signature             = signature,
        registered_model_name = "livetwin.ml.failure_classifier",
        input_example         = sample_in,
    )

    print(f"\nModel logged: {model_info.model_uri}")
    print(f"Sample predictions:\n{sample_out}")
    run_id = run.info.run_id

# COMMAND ----------

# MAGIC %md ## 3. Register Champion + Deploy

# COMMAND ----------

client = mlflow.tracking.MlflowClient()
model_versions = client.search_model_versions("name='livetwin.ml.failure_classifier'")
latest_version = max([int(mv.version) for mv in model_versions])
client.set_registered_model_alias("livetwin.ml.failure_classifier", "champion", str(latest_version))
print(f"✓ Version {latest_version} set as champion")

w = WorkspaceClient()
endpoint_name = "livetwin-failure-classifier"

try:
    w.serving_endpoints.create(
        name=endpoint_name,
        config={
            "served_models": [{
                "model_name":            "livetwin.ml.failure_classifier",
                "model_version":         str(latest_version),
                "workload_size":         "Small",
                "scale_to_zero_enabled": True,
            }],
            "traffic_config": {
                "routes": [{"served_model_name": "livetwin.ml.failure_classifier", "traffic_percentage": 100}]
            },
        },
    )
    print(f"✓ Endpoint created: {endpoint_name}")
except Exception as e:
    if "already exists" in str(e).lower():
        w.serving_endpoints.update_config(
            name=endpoint_name,
            served_models=[{
                "model_name":    "livetwin.ml.failure_classifier",
                "model_version": str(latest_version),
                "workload_size": "Small",
            }]
        )
        print(f"✓ Endpoint updated: {endpoint_name}")
    else:
        raise

print("✅ Failure Classifier trained, registered, and deployed.")
