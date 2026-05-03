import os
import polars as pl
import mlflow
import mlflow.sklearn

from deltalake import DeltaTable
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score, accuracy_score, f1_score, roc_auc_score
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier


FEATURES_PATH = "../lakehouse/gold/features"
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment("flight-delay-lab3")

gold_table = DeltaTable(FEATURES_PATH)
gold_version = gold_table.version()

df = pl.read_delta(FEATURES_PATH).to_pandas()

features = ["origin", "dest", "route", "airline", "distance", "hour", "day_of_week", "month", "dep_delay"]
categorical_features = ["origin", "dest", "route", "airline"]
numeric_features = ["distance", "hour", "day_of_week", "month", "dep_delay"]

X = df[features]
y_reg = df["arr_delay"]
y_clf = df["is_delayed"]

print("=== TRAINING START ===")
print(f"MLflow URI: {MLFLOW_TRACKING_URI}")
print(f"Gold feature table version: {gold_version}")
print(f"Dataset shape: {df.shape}")
print(f"Delay rate: {y_clf.mean():.4f}")


preprocessor = ColumnTransformer([
    ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_features),
    ("num", "passthrough", numeric_features),
])


def train_regression_model(name, model):
    if mlflow.active_run():
        mlflow.end_run()

    X_train, X_test, y_train, y_test = train_test_split(X, y_reg, test_size=0.2, random_state=42)

    pipeline = Pipeline([("preprocessor", preprocessor), ("model", model)])

    with mlflow.start_run(run_name=name):
        pipeline.fit(X_train, y_train)
        preds = pipeline.predict(X_test)

        mae = mean_absolute_error(y_test, preds)
        rmse = mean_squared_error(y_test, preds) ** 0.5
        r2 = r2_score(y_test, preds)

        mlflow.log_params({
            "task": "regression",
            "model_name": name,
            "gold_table_version": gold_version,
            "target": "arr_delay",
            "features": ",".join(features),
        })

        mlflow.log_metrics({"mae": mae, "rmse": rmse, "r2": r2})
        mlflow.sklearn.log_model(pipeline, artifact_path="model")

        print(f"[REG] {name}: MAE={mae:.3f}, RMSE={rmse:.3f}, R2={r2:.3f}")

    if hasattr(pipeline.named_steps["model"], "feature_importances_"):
        importances = pipeline.named_steps["model"].feature_importances_

        for i, val in enumerate(sorted(importances, reverse=True)[:5]):
            mlflow.log_metric(f"feature_importance_{i}", float(val))


def train_classification_model(name, model):
    if mlflow.active_run():
        mlflow.end_run()

    X_train, X_test, y_train, y_test = train_test_split(
        X, y_clf, test_size=0.2, random_state=42, stratify=y_clf
    )

    pipeline = Pipeline([("preprocessor", preprocessor), ("model", model)])

    with mlflow.start_run(run_name=name):
        pipeline.fit(X_train, y_train)
        preds = pipeline.predict(X_test)

        acc = accuracy_score(y_test, preds)
        f1 = f1_score(y_test, preds)

        mlflow.log_params({
            "task": "classification",
            "model_name": name,
            "gold_table_version": gold_version,
            "target": "is_delayed",
            "features": ",".join(features),
        })

        mlflow.log_metrics({"accuracy": acc, "f1": f1})

        if hasattr(pipeline.named_steps["model"], "predict_proba"):
            probs = pipeline.predict_proba(X_test)[:, 1]
            auc = roc_auc_score(y_test, probs)
            mlflow.log_metric("roc_auc", auc)
            print(f"[CLF] {name}: ACC={acc:.3f}, F1={f1:.3f}, AUC={auc:.3f}")
        else:
            print(f"[CLF] {name}: ACC={acc:.3f}, F1={f1:.3f}")

        mlflow.sklearn.log_model(pipeline, artifact_path="model")


if __name__ == "__main__":
    train_regression_model("linear_regression", LinearRegression())

    train_regression_model(
        "random_forest_regressor",
        RandomForestRegressor(n_estimators=50, max_depth=12, random_state=42, n_jobs=-1),
    )

    train_classification_model(
        "logistic_regression",
        LogisticRegression(max_iter=500, n_jobs=-1),
    )

    train_classification_model(
        "random_forest_classifier",
        RandomForestClassifier(
            n_estimators=50,
            max_depth=12,
            random_state=42,
            n_jobs=-1,
            class_weight="balanced",
        ),
    )

    print("=== TRAINING FINISHED ===")
