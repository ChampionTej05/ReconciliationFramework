from __future__ import annotations
import math
import pandas as pd

_DEF_MIN_BASE = 1e-8

def _rel_match(a: pd.Series, b: pd.Series, tol_pct: float, min_base: float=_DEF_MIN_BASE) -> pd.Series:
    base = pd.concat([a.abs(), b.abs()], axis=1).max(axis=1).clip(lower=min_base)
    return (a - b).abs() <= (base * tol_pct)

def _abs_match(a: pd.Series, b: pd.Series, tol_abs: float) -> pd.Series:
    return (a - b).abs() <= tol_abs

def _rounded_match(a: pd.Series, b: pd.Series, decimals: int) -> pd.Series:
    return a.round(decimals).eq(b.round(decimals))

def reconcile(df: pd.DataFrame, rules: dict, recon_cols_section: str = 'numeric', prefix_A='_A', prefix_B='_B') -> pd.DataFrame:
    df = df.copy()
    per_col_flags = []
    for rule in (rules.get(recon_cols_section) or []):
        col = rule['column']
        a = df[f"{col}{prefix_A}"] if f"{col}{prefix_A}" in df.columns else df[col]
        b = df[f"{col}{prefix_B}"] if f"{col}{prefix_B}" in df.columns else df[col]
        # deltas
        df[f"delta_{col}"] = b - a
        df[f"abs_delta_{col}"] = (b - a).abs()
        df[f"pct_delta_{col}"] = (b - a) / (a.replace(0, _DEF_MIN_BASE))
        # comparator
        comp = rule.get('comparator', 'relative')
        if comp == 'relative':
            tol = float(rule.get('tol_pct', 0.0))
            flag = _rel_match(a, b, tol, float(rule.get('min_base', _DEF_MIN_BASE)))
        elif comp == 'absolute':
            flag = _abs_match(a, b, float(rule.get('tol_abs', 0.0)))
        elif comp == 'rounded':
            flag = _rounded_match(a, b, int(rule.get('round', 2)))
        else:
            flag = a.eq(b)
        df[f"match_{col}"] = flag
        per_col_flags.append(f"match_{col}")
    if per_col_flags:
        df['match_flag'] = df[per_col_flags].all(axis=1)
    else:
        df['match_flag'] = True
    return df

# Optional non-numeric API (signature only)

def reconcile_non_numeric(df: pd.DataFrame, rules: dict, prefix_A='A_', prefix_B='B_') -> pd.DataFrame:
    """Compare strings/attributes (exact, case-insensitive, normalized)."""
    raise NotImplementedError
