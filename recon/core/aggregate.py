from __future__ import annotations
import pandas as pd
from typing import Dict

_AGG_MAP = {
    'sum': 'sum',
    'count': 'count',
    'min': 'min',
    'max': 'max',
    'first': 'first',
    'last': 'last',
    'nunique': 'nunique',
}

def aggregate(df: pd.DataFrame, spec: Dict) -> pd.DataFrame:
    if not spec:
        return df
    group_by = spec.get('group_by') or []
    metrics = spec.get('metrics') or {}
    if not group_by:
        return df
    agg_dict = {}
    for col, cfg in metrics.items():
        agg = cfg.get('agg', 'sum')
        agg_dict[col] = _AGG_MAP.get(agg, 'sum')
    g = df.groupby(group_by, dropna=False, as_index=False).agg(agg_dict)
    return g

# Optional (signature only)

def pivot_table(df: pd.DataFrame, index: list[str], columns: list[str], values: str, aggfunc: str = 'sum') -> pd.DataFrame:
    """Optional: implement if needed."""
    raise NotImplementedError
