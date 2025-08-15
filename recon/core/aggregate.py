from __future__ import annotations
import pandas as pd
from typing import Dict

import logging
log = logging.getLogger(__name__)

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
    log.debug("aggregate: entering with spec=%s", spec)
    if not spec:
        return df
    group_by = getattr(spec, 'group_by', []) 
    metrics = getattr(spec, 'metrics', {})
    log.info("aggregate: group_by=%s, metrics=%s", group_by, list(metrics.keys()))
    if not group_by:
        log.warning("aggregate: no group_by provided; returning input frame unchanged")
        return df
    agg_dict = {}
    for col, cfg in metrics.items():
        agg = cfg.get('agg', 'sum')
        agg_dict[col] = _AGG_MAP.get(agg, 'sum')
    g = df.groupby(group_by, dropna=False, as_index=False).agg(agg_dict)
    log.debug("aggregate: result shape=%s", getattr(g, 'shape', None))
    # DEBUG: uncomment to inspect a sample
    # log.debug("aggregate head:\n%s", g.head(5))
    return g

# Optional (signature only)

def pivot_table(df: pd.DataFrame, index: list[str], columns: list[str], values: str, aggfunc: str = 'sum') -> pd.DataFrame:
    """Optional: implement if needed."""
    raise NotImplementedError
