import json
import numpy as np
import pandas as pd
import xgboost as xgb
from pathlib import Path
from sklearn.metrics import classification_report


params = {
    "objective": "multi:softprob",   # probabilities (better)
    "num_class": 7,
    "eval_metric": "mlogloss",
    "tree_method": "hist",           # FAST & memory efficient
    "max_depth": 8,
    "eta": 0.05,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "nthread": -1,
}


train_files = sorted(Path("./dataset_split/train").glob("*.csv"))
train_dfs = []

for file in train_files:
    print("Reading:", file)
    for chunk in pd.read_csv(file, header=0, chunksize=200_000):
        chunk = chunk.apply(pd.to_numeric, errors="coerce")
        chunk = chunk.dropna(subset=["label"])
        if len(chunk) > 0:
            train_dfs.append(chunk)

train_df = pd.concat(train_dfs, ignore_index=True)
feature_cols = train_df.columns.drop("label")

X_train = train_df[feature_cols].values
y_train = train_df["label"].values.astype(int)


dtrain = xgb.DMatrix(X_train, label=y_train)
model = xgb.train(params, dtrain, num_boost_round=50)

save_path = Path("./model/xgb_model.json")
save_path.parent.mkdir(exist_ok=True, parents=True)
model.save_model(save_path)

feature_cols_list = feature_cols.tolist()
with open("./model/feature_cols.json", "w") as f:
    json.dump(feature_cols_list, f)

y_true_all, y_pred_all = [], []

test_files = sorted(Path("./dataset_split/test").glob("*.csv"))
for file in test_files:
    print("Evaluating on:", file)
    for chunk in pd.read_csv(file, header=0, chunksize=200_000):
        chunk = chunk.apply(pd.to_numeric, errors="coerce")
        chunk = chunk.dropna(subset=["label"])
        if len(chunk) == 0:
            continue

        X_test = chunk[feature_cols].values
        y_true = chunk["label"].values.astype(int)

        dtest = xgb.DMatrix(X_test)
        preds = model.predict(dtest)
        y_pred = preds.argmax(axis=1)

        y_true_all.append(y_true)
        y_pred_all.append(y_pred)

y_true_all = np.concatenate(y_true_all)
y_pred_all = np.concatenate(y_pred_all)

print(classification_report(y_true_all, y_pred_all))