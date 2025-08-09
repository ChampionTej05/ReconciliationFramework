from __future__ import annotations
import pandas as pd

def sanitize(df: pd.DataFrame, spec: dict) -> pd.DataFrame:
    spec = spec or {}
    rename = (spec.get('rename') or {})
    df = df.rename(columns=rename)
    if spec.get('normalize', {}).get('trim_strings'):
        for c in df.select_dtypes(include=['object', 'string']).columns:
            df[c] = df[c].astype(str).str.strip()
    for up in (spec.get('normalize', {}).get('upper_case') or []):
        if up in df.columns:
            df[up] = df[up].astype(str).str.upper()
    select = spec.get('select')
    if select:
        df = df[[c for c in select if c in df.columns]]
    return df
