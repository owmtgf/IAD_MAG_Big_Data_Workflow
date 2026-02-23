import pandas as pd
import numpy as np
import ipaddress
from sklearn.preprocessing import StandardScaler


TO_DROP_COLS = [
    "ts",
    "uid",
    "local_orig",
    "local_resp",
    "missed_bytes",
    "tunnel_parents",
    "service",
    "detailed-label",
]

NUMERIC_COLS = [
    'id.orig_p', 
    'id.resp_p', 
    'duration', 
    'orig_bytes', 
    'resp_bytes',
    'orig_pkts', 
    'orig_ip_bytes', 
    'resp_pkts', 
    'resp_ip_bytes',
]

TO_LOG_COLS = [
    "duration", 
    "orig_bytes", "resp_bytes",
    "orig_pkts", "resp_pkts",
    "orig_ip_bytes", "resp_ip_bytes",
]

FEATURES_MAPPING = {
    "conn_state": {name: i for i, name in enumerate(['S0', 'SHR', 'REJ', 'SH', 'RSTO', 'S3', 'OTH', 'RSTR', 'S1', 'SF', 'RSTOS0', 'S2', 'RSTRH'])},
    "hist_has_data": {name: i for i, name in enumerate([False, True])},
    "hist_has_fin": {name: i for i, name in enumerate([False, True])},
    "hist_has_rst": {name: i for i, name in enumerate([False, True])},
    "hist_has_syn": {name: i for i, name in enumerate([False, True])},
    "orig_ephemeral": {name: i for i, name in enumerate([False, True])},
    "orig_is_private": {name: i for i, name in enumerate([False, True])},
    "proto": {name: i for i, name in enumerate(['icmp', 'tcp', 'udp'])},
    "resp_is_private": {name: i for i, name in enumerate([False, True])},
    "resp_well_known": {name: i for i, name in enumerate([False, True])},
    "label": {
        'Benign': 0,
        'Malicious': 1,
        'Malicious   C&C': 2,
        'Malicious   Attack': 3,
        'Malicious   FileDownload': 4,
        'Malicious   PartOfAHorizontalPortScan': 5,
        'Malicious   DDoS': 6,
    },
}

def map_features(df: pd.DataFrame, feature_mapping: dict) -> pd.DataFrame: 

    df_out = df.copy()
    
    for feature, mapping in feature_mapping.items():
        if not (feature in df_out.columns): 
            print(f"[map_features] Feature '{feature}' not found in DataFrame. Skipping...")
            continue
        assert isinstance(mapping, dict), (
            f"[map_features] Mapping for feature '{feature}' must be a dict"
        )
        
        existing_values = set(df_out[feature].unique())
        mapping_keys = set(mapping.keys())
        
        invalid_keys = mapping_keys - existing_values
        if len(invalid_keys) != 0: 
            print(
                f"[map_features] Mapping for feature '{feature}' contains labels "
                f"not present in data: {invalid_keys}"
            )
        
        df_out[feature] = df_out[feature].map(
            lambda x: mapping.get(x, x)
        )
    
    return df_out


def replace_unknowns_with_nan(df: pd.DataFrame, placeholders, columns=None) -> pd.DataFrame:

    df_out = df.copy()

    if columns is None:
        columns = df_out.columns

    for col in columns:
        if col not in df_out.columns:
            print(f"[replace_unknowns_with_nan] Column '{col}' not found. Skipping...")
            continue

        df_out[col] = df_out[col].replace(list(placeholders), np.nan)

    return df_out


def drop_features(df: pd.DataFrame, feature_names: list) -> pd.DataFrame:
    
    missing_cols = [col for col in feature_names if col not in df.columns]
    assert len(missing_cols) == 0, (
        f"[drop_features] The following columns do not exist in DataFrame: {missing_cols}"
    )
    
    return df.copy().drop(columns=feature_names)

    
def drop_rows_with_nan(df: pd.DataFrame, subset: list = None) -> pd.DataFrame:
    if subset is None:
        subset = df.columns.tolist()
    df_out = df.dropna(subset=subset).copy()
    return df_out


def convert_ip_adresses(df: pd.DataFrame):

    def _is_private_ip(ip):
        try:
            return ipaddress.ip_address(ip).is_private
        except:
            return None
        
    df_out = df.copy()

    df_out["orig_is_private"] = df_out["id.orig_h"].apply(_is_private_ip)
    df_out["resp_is_private"] = df_out["id.resp_h"].apply(_is_private_ip)
    df_out = df_out.drop(columns=["id.orig_h", "id.resp_h"])

    return df_out


def convert_ports(df: pd.DataFrame):
    df_out = df.copy()
    df_out["resp_well_known"] = df_out["id.resp_p"] < 1024
    df_out["orig_ephemeral"] = df_out["id.orig_p"] >= 49152

    df_out = df_out.drop(columns=["id.orig_p", "id.resp_p"])

    return df_out


def log_features(df: pd.DataFrame, features: list):

    df_out = df.copy()

    for feature in features:

        df_out[feature] = pd.to_numeric(df_out[feature], errors="coerce")

        df_out[f"{feature}_log"] = np.log1p(df_out[feature].clip(lower=0))

    df_out = df_out.drop(columns=features)

    return df_out


def unify_numeric_columns(df: pd.DataFrame, numeric_cols: list) -> pd.DataFrame:
    df_out = df.copy()

    for col in numeric_cols:
        if col not in df_out.columns:
            print(f"[unify_numeric_columns] Column '{col}' not in DataFrame. Skipping...")
            continue

        # Convert everything to numeric, coerce errors to NaN
        df_out[col] = pd.to_numeric(df_out[col], errors='coerce')

    return df_out


def convert_history(df: pd.DataFrame):

    df_out = df.copy()

    hist = df_out["history"].fillna("")

    df_out["hist_has_syn"]  = hist.str.contains("S", regex=False)
    df_out["hist_has_fin"]  = hist.str.contains("F", regex=False)
    df_out["hist_has_rst"]  = hist.str.contains("R", regex=False)
    df_out["hist_has_data"] = hist.str.contains("D", regex=False)

    df_out = df_out.drop(columns=["history"])

    return df_out


def run_through_pipeline(df: pd.DataFrame):

    df_out = df.copy()

    df_out = drop_features(df_out, feature_names=TO_DROP_COLS)

    placeholders = {"-", "unknown"}
    df_out = replace_unknowns_with_nan(df_out, placeholders=placeholders)

    df_out = convert_ip_adresses(df_out)

    df_out = convert_ports(df_out)

    df_out = convert_history(df_out)

    df_out = unify_numeric_columns(df_out, numeric_cols=NUMERIC_COLS)

    df_out = drop_rows_with_nan(df_out)

    df_out = log_features(df_out, features=TO_LOG_COLS)

    df_out = map_features(df_out, feature_mapping=FEATURES_MAPPING)

    return df_out