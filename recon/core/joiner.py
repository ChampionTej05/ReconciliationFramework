from __future__ import annotations
import pandas as pd
import numpy as np

JOIN_TYPE_MAP = {
    'inner': 'inner',
    'left': 'left',
    'right': 'right',
    'outer': 'outer',
}

def build_join_key(df: pd.DataFrame, keys: list[str], key_name: str = 'recon_key') -> pd.DataFrame:
    df[key_name] = df[keys].astype(str).agg('|'.join, axis=1)
    return df

def join(A: pd.DataFrame, B: pd.DataFrame, keys: list[str], how, key_name, prefix_A="_A", prefix_B='_B') -> pd.DataFrame:
    if key_name:
        A = build_join_key(A, keys, key_name)
        B = build_join_key(B, keys, key_name)
        on = [key_name]
    else:
        on = keys
    suffixes=(f"{prefix_A}", f"{prefix_B}")
    # Use merge with explicit suffixes to avoid collisions
    df = A.merge(B, how=JOIN_TYPE_MAP.get(how, 'outer'), on=on, suffixes=suffixes)
    # === NEW: coalesce original join key columns back to unsuffixed names ===
    # Prefer A_<col>, fall back to B_<col>. If both exist and are equal, either is fine.
    for k in keys:
        a_col = f"{k}{prefix_A}"
        b_col = f"{k}{prefix_B}"
        if k not in df.columns:  # only create if not already present (i.e., when using recon_key)
            if a_col in df.columns and b_col in df.columns:
                df[k] = df[a_col].where(df[a_col].notna(), df[b_col])
            elif a_col in df.columns:
                df[k] = df[a_col]
            elif b_col in df.columns:
                df[k] = df[b_col]
    # === coverage flags (unchanged) ===
    # Coverage flags 
    if prefix_A and prefix_B:
        df['only_in_A'] = df.filter(like=prefix_B).isna().all(axis=1)
        df['only_in_B'] = df.filter(like=prefix_A).isna().all(axis=1)
        df['in_both'] = ~(df['only_in_A'] | df['only_in_B'])

    # === NEW: fill NaN for numeric columns with 0.0 ===
    num_cols = df.select_dtypes(include=[np.number]).columns
    df[num_cols] = df[num_cols].fillna(0.0)
    return df
