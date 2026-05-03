import polars as pl
import mlflow
import mlflow.sklearn

from deltalake import DeltaTable

from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    accuracy_score,
    f1_score,
    roc_auc_score,
)
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier


FEATURES_PATH = "../lakehouse/gold/features"
MLFLOW_TRACKING_URI = "http://localhost:5000"

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment("flight-delay-lab3")

gold_table = DeltaTable(FEATURES_PATH)
gold_version = gold_table.version()

df = pl.read_delta(FEATURES_PATH).to_pandas()

features = [
    "origin",
    "dest",
    "route",
    "airline",
    "Distance",
    "hour",
    "day_of_week",
    "month",
    "DepDelay",
]

categorical_features = ["origin", "dest", "route", "airline"]
numeric_features = ["Distance", "hour", "day_of_week", "month", "DepDelay"]

X = df[features]

y_reg = df["ArrDelay"]
y_clf = df["is_delayed"]

preprocessor = ColumnTransformer(
    transformers=[
        ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_features),
        ("num", "passthrough", numeric_features),
    ]
)


def train_regression_model(name, model):
    X_train, X_test, y_train, y_test = train_test_split(
        X, y_reg, test_size=0.2, random_state=42
    )

    pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", model),
        ]
    )

    with mlflow.start_run(run_name=name):
        pipeline.fit(X_train, y_train)
        preds = pipeline.predict(X_test)

        mae = mean_absolute_error(y_test, preds)
        mse = mean_squared_error(y_test, preds)
        rmse = mse ** 0.5
        r2 = r2_score(y_test, preds)

        mlflow.log_param("task", "regression")
        mlflow.log_param("model_name", name)
        mlflow.log_param("gold_table_version", gold_version)
        mlflow.log_param("target", "ArrDelay")
        mlflow.log_param("features", ",".join(features))

        mlflow.log_metric("mae", mae)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2", r2)

        mlflow.sklearn.log_model(pipeline, artifact_path="model")

        print(f"[REG] {name}: MAE={mae:.3f}, RMSE={rmse:.3f}, R2={r2:.3f}")


def train_classification_model(name, model):
    X_train, X_test, y_train, y_test = train_test_split(
        X, y_clf, test_size=0.2, random_state=42, stratify=y_clf
    )

    pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", model),
        ]
    )

    with mlflow.start_run(run_name=name):
        pipeline.fit(X_train, y_train)
        preds = pipeline.predict(X_test)

        acc = accuracy_score(y_test, preds)
        f1 = f1_score(y_test, preds)

        mlflow.log_param("task", "classification")
        mlflow.log_param("model_name", name)
        mlflow.log_param("gold_table_version", gold_version)
        mlflow.log_param("target", "is_delayed")
        mlflow.log_param("features", ",".join(features))

        mlflow.log_metric("accuracy", acc)
        mlflow.log_metric("f1", f1)

        if hasattr(pipeline.named_steps["model"], "predict_proba"):
            probs = pipeline.predict_proba(X_test)[:, 1]
            auc = roc_auc_score(y_test, probs)
            mlflow.log_metric("roc_auc", auc)
            print(f"[CLF] {name}: ACC={acc:.3f}, F1={f1:.3f}, AUC={auc:.3f}")
        else:
            print(f"[CLF] {name}: ACC={acc:.3f}, F1={f1:.3f}")

        mlflow.sklearn.log_model(pipeline, artifact_path="model")


if __name__ == "__main__":
    print(f"Gold feature table version: {gold_version}")
    print(f"Dataset shape: {df.shape}")

    train_regression_model(
        "linear_regression",
        LinearRegression(),
    )

    train_regression_model(
        "random_forest_regressor",
        RandomForestRegressor(
            n_estimators=50,
            max_depth=12,
            random_state=42,
            n_jobs=-1,
        ),
    )

    train_classification_model(
        "logistic_regression",
        LogisticRegression(
            max_iter=500,
            n_jobs=-1,
        ),
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

    print("Training finished")
