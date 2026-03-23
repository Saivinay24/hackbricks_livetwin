# Databricks notebook source
# MAGIC %md
# MAGIC # LiveTwin — RUL Predictor Training
# MAGIC
# MAGIC Trains a Gradient Boosting Regressor to predict Remaining Useful Life (hours).
# MAGIC
# MAGIC - **Algorithm:** Gradient Boosting Regressor (XGBoost-style via sklearn)
# MAGIC - **Input:** 9 key sensor readings + rolling features + cycle number
# MAGIC - **Output:** `{rul_hours, lower_bound, upper_bound}` (with 90% confidence interval)
# MAGIC - **Ground Truth:** NASA CMAPSS labeled RUL (max_cycle - current_cycle)
# MAGIC - **Deployment:** MLflow real-time serving endpoint `livetwin-rul-predictor`

# COMMAND ----------

# MAGIC %pip install scikit-learn xgboost mlflow databricks-sdk -q

# COMMAND ----------

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from mlflow.models.signature import infer_signature
from databricks.sdk import WorkspaceClient

CYCLES_PER_HOUR = 1.0   # CMAPSS: 1 cycle ≈ 1 hour of operation for turbofan

# COMMAND ----------

# MAGIC %md ## 1. Feature Engineering

# COMMAND ----------

IMPORTANT_SENSORS = ["s2", "s3", "s4", "s7", "s11", "s12", "s15", "s20", "s21"]

def engineer_rul_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build rich feature set for RUL prediction:
    - Raw sensor values (9 key sensors)
    - Rolling mean/std per engine over 30-cycle window
    - Rate of change (slope) over 10-cycle window
    - Normalized cycle fraction (cycle / max_cycle)
    - Op settings (3 features)
    """
    df = df.sort_values(["unit_id", "cycle"]).copy()

    # Rolling features
    for s in IMPORTANT_SENSORS:
        df[f"{s}_roll_mean_30"] = (
            df.groupby("unit_id")[s]
            .transform(lambda x: x.rolling(30, min_periods=1).mean())
        )
        df[f"{s}_roll_std_30"] = (
            df.groupby("unit_id")[s]
            .transform(lambda x: x.rolling(30, min_periods=1).std().fillna(0))
        )
        df[f"{s}_slope_10"] = (
            df.groupby("unit_id")[s]
            .transform(lambda x: x.rolling(10, min_periods=2)
                       .apply(lambda w: np.polyfit(range(len(w)), w, 1)[0] if len(w) > 1 else 0))
            .fillna(0)
        )

    # Cycle fraction
    max_cycles = df.groupby("unit_id")["cycle"].transform("max")
    df["cycle_fraction"] = df["cycle"] / max_cycles

    return df

# COMMAND ----------

# Load CMAPSS
df_raw = spark.table("livetwin.raw.cmapss_fd001").toPandas()

# Ensure sensor columns are present
sensor_cols_raw = [f"sensor_{i}" for i in range(1, 22)]
if "sensor_2" in df_raw.columns:
    # Rename sensor_X → sX for consistency
    rename_map = {f"sensor_{i}": f"s{i}" for i in range(1, 22)}
    df_raw = df_raw.rename(columns=rename_map)

print("Engineering features...")
df_feat = engineer_rul_features(df_raw)
print(f"Feature engineering complete: {df_feat.shape}")

# Cap RUL at 125 cycles (standard CMAPSS practice: cap at piece-wise linear threshold)
RUL_CAP = 125
df_feat["rul_capped"] = df_feat["rul"].clip(upper=RUL_CAP)

# Feature columns
base_features    = IMPORTANT_SENSORS
roll_features    = [f"{s}_roll_mean_30" for s in IMPORTANT_SENSORS] + \
                   [f"{s}_roll_std_30"  for s in IMPORTANT_SENSORS] + \
                   [f"{s}_slope_10"     for s in IMPORTANT_SENSORS]
op_features      = ["op_setting_1", "op_setting_2", "op_setting_3"]
meta_features    = ["cycle", "cycle_fraction"]

# Use only columns that exist in df_feat
all_feature_cols = [c for c in (base_features + roll_features + op_features + meta_features)
                    if c in df_feat.columns]

X = df_feat[all_feature_cols].fillna(0).values
y = df_feat["rul_capped"].values  # target: capped RUL in cycles ≈ hours

print(f"Feature columns ({len(all_feature_cols)}): {all_feature_cols[:10]}...")
print(f"X shape: {X.shape}, y shape: {y.shape}")
print(f"RUL range: {y.min():.0f} – {y.max():.0f} hours")

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
print(f"Train: {X_train.shape[0]:,}, Test: {X_test.shape[0]:,}")

# COMMAND ----------

# MAGIC %md ## 2. Train with MLflow Tracking

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment("/Shared/livetwin/rul_predictor")

with mlflow.start_run(run_name="gbr_rul_v1") as run:
    params = {
        "n_estimators":     500,
        "learning_rate":    0.05,
        "max_depth":        5,
        "min_samples_leaf": 20,
        "subsample":        0.8,
        "random_state":     42,
    }
    mlflow.log_params(params)
    mlflow.log_param("features",       all_feature_cols)
    mlflow.log_param("rul_cap",        RUL_CAP)
    mlflow.log_param("dataset",        "NASA CMAPSS FD001")
    mlflow.log_param("cycles_per_hour", CYCLES_PER_HOUR)

    # Pipeline
    pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("gbr",    GradientBoostingRegressor(**params)),
    ])
    pipeline.fit(X_train, y_train)

    # Evaluation
    y_pred       = pipeline.predict(X_test)
    mae          = mean_absolute_error(y_test, y_pred)
    rmse         = np.sqrt(mean_squared_error(y_test, y_pred))
    r2           = r2_score(y_test, y_pred)

    # Score within ±13 cycles (standard CMAPSS metric)
    within_13    = np.mean(np.abs(y_pred - y_test) <= 13)

    mlflow.log_metric("mae",       mae)
    mlflow.log_metric("rmse",      rmse)
    mlflow.log_metric("r2",        r2)
    mlflow.log_metric("within_13", within_13)

    print(f"MAE: {mae:.2f} hrs  RMSE: {rmse:.2f} hrs  R²: {r2:.4f}  Within±13: {within_13:.2%}")

    # ── Confidence Interval via Quantile Forest ──────────────────────────────
    # Train lower/upper quantile models for 90% CI
    from sklearn.ensemble import GradientBoostingRegressor as GBR

    lower_model = Pipeline([
        ("scaler", StandardScaler()),
        ("gbr",    GBR(**{**params, "loss": "quantile", "alpha": 0.05})),
    ])
    upper_model = Pipeline([
        ("scaler", StandardScaler()),
        ("gbr",    GBR(**{**params, "loss": "quantile", "alpha": 0.95})),
    ])
    lower_model.fit(X_train, y_train)
    upper_model.fit(X_train, y_train)

    # Package all three models into a single custom MLflow model
    import mlflow.pyfunc

    class RULPredictorModel(mlflow.pyfunc.PythonModel):
        """
        Custom MLflow model that returns:
        {rul_hours: float, lower_bound: float, upper_bound: float}
        """
        def __init__(self, center, lower, upper, feature_cols, cycles_per_hour):
            self.center         = center
            self.lower          = lower
            self.upper          = upper
            self.feature_cols   = feature_cols
            self.cycles_per_hour = cycles_per_hour

        def predict(self, context, model_input):
            X  = model_input.values if hasattr(model_input, "values") else model_input
            c  = self.center.predict(X) * self.cycles_per_hour
            lo = self.lower.predict(X)  * self.cycles_per_hour
            hi = self.upper.predict(X)  * self.cycles_per_hour
            # Ensure ordering: lo <= c <= hi
            lo = np.minimum(lo, c)
            hi = np.maximum(hi, c)
            return pd.DataFrame({
                "rul_hours":        c.clip(0),
                "lower_bound":      lo.clip(0),
                "upper_bound":      hi.clip(0),
            })

    rul_model   = RULPredictorModel(pipeline, lower_model, upper_model, all_feature_cols, CYCLES_PER_HOUR)
    sample_input= pd.DataFrame(X_test[:3], columns=all_feature_cols)
    sample_output = rul_model.predict(None, sample_input)

    signature   = infer_signature(sample_input, sample_output)
    model_info  = mlflow.pyfunc.log_model(
        artifact_path   = "model",
        python_model    = rul_model,
        signature       = signature,
        registered_model_name = "livetwin.ml.rul_predictor",
        input_example   = sample_input,
    )

    print(f"Model logged: {model_info.model_uri}")
    print(f"Sample output:\n{sample_output}")
    run_id = run.info.run_id

# COMMAND ----------

# MAGIC %md ## 3. Register Champion + Deploy Endpoint

# COMMAND ----------

client = mlflow.tracking.MlflowClient()

model_versions = client.search_model_versions("name='livetwin.ml.rul_predictor'")
latest_version = max([int(mv.version) for mv in model_versions])
client.set_registered_model_alias("livetwin.ml.rul_predictor", "champion", str(latest_version))
print(f"✓ Version {latest_version} set as champion")

w = WorkspaceClient()
endpoint_name = "livetwin-rul-predictor"

try:
    w.serving_endpoints.create(
        name=endpoint_name,
        config={
            "served_models": [{
                "model_name":            "livetwin.ml.rul_predictor",
                "model_version":         str(latest_version),
                "workload_size":         "Small",
                "scale_to_zero_enabled": True,
            }],
            "traffic_config": {
                "routes": [{"served_model_name": "livetwin.ml.rul_predictor", "traffic_percentage": 100}]
            },
        },
    )
    print(f"✓ Endpoint created: {endpoint_name}")
except Exception as e:
    if "already exists" in str(e).lower():
        w.serving_endpoints.update_config(
            name=endpoint_name,
            served_models=[{
                "model_name":    "livetwin.ml.rul_predictor",
                "model_version": str(latest_version),
                "workload_size": "Small",
            }]
        )
        print(f"✓ Endpoint updated: {endpoint_name}")
    else:
        raise

print("✅ RUL Predictor trained, registered, and deployed.")
