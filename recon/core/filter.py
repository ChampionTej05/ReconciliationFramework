from __future__ import annotations
import pandas as pd

_OPS = {
    'eq': lambda s, v: s == v,
    'ne': lambda s, v: s != v,
    'gt': lambda s, v: s > v,
    'ge': lambda s, v: s >= v,
    'lt': lambda s, v: s < v,
    'le': lambda s, v: s <= v,
    'in': lambda s, v: s.isin(v),
    'not_in': lambda s, v: ~s.isin(v),
    'is_null': lambda s, v: s.isna(),
    'not_null': lambda s, v: ~s.isna(),
}

def apply_filters(df: pd.DataFrame, predicates: list[dict] | None) -> pd.DataFrame:
    if not predicates:
        return df
    mask = pd.Series([True] * len(df), index=df.index)
    for p in predicates:
        col, op, val = p['col'], p['op'], p.get('value')
        if op not in _OPS or col not in df.columns:
            continue
        mask &= _OPS[op](df[col], val)
    return df[mask]
