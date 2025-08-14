from __future__ import annotations
import pandas as pd

def sanitize(df: pd.DataFrame, spec: SanitizeCfg | None) -> pd.DataFrame:
    if spec is None:
        return df
    rename = getattr(spec, 'rename', {}) or {}
    df = df.rename(columns=rename)
    normalize_cfg = getattr(spec, 'normalize', {}) or {}
    if normalize_cfg.get('trim_strings'):
        for c in df.select_dtypes(include=['object', 'string']).columns:
            df[c] = df[c].astype(str).str.strip()
    for up in normalize_cfg.get('upper_case', []):
        if up in df.columns:
            df[up] = df[up].astype(str).str.upper()
    select = getattr(spec, 'select', None)
    if select:
        df = df[[c for c in select if c in df.columns]]
    return df
