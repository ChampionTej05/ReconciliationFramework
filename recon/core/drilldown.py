# recon/core/drilldown.py
from __future__ import annotations
from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Any, Optional

import pandas as pd

import logging
log = logging.getLogger(__name__)

from .aggregate import aggregate
from .joiner import join
from .reconcile import reconcile
from .report import emit_reports

def _dedup_keep_order(seq):
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            out.append(x); seen.add(x)
    return out

def _adjust_groupby(spec: Optional[Dict[str, Any]], add: List[str] | None, remove: List[str] | None) -> Optional[Dict[str, Any]]:
    if not spec:
        return spec
    spec2 = deepcopy(spec)
    gb = getattr(spec2, "group_by", None) or []
    if remove:
        rem = set(remove)
        gb = [c for c in gb if c not in rem]
    if add:
        gb = _dedup_keep_order(list(gb) + list(add))
    setattr(spec2, "group_by", gb)
    return spec2

def run_drilldown(
    A_prepared: pd.DataFrame,
    B_prepared: pd.DataFrame,
    full_cfg: "RootCfg",
    levels: List["DrillLevel"],
    strategy: str = "add",  # "add" (drill-down) or "remove" (drill-up)
) -> None:
    if not levels:
        return
    log.debug("drilldown: A shape=%s B shape=%s", getattr(A_prepared, 'shape', None), getattr(B_prepared, 'shape', None))
    # DEBUG: uncomment to inspect a small sample during investigation
    # log.debug("A head:\n%s", A_prepared.head(3))
    # log.debug("B head:\n%s", B_prepared.head(3))
    agg_cfg = getattr(full_cfg, "aggregate", None)
    aggA_base = getattr(agg_cfg, "A", None) if agg_cfg else None
    aggB_base = getattr(agg_cfg, "B", None) if agg_cfg else None

    join_cfg = getattr(full_cfg, "join", None)
    report_cfg_base = deepcopy(getattr(full_cfg, "report", None))

    # Determine base output directory using attributes, defaulting to "out/drilldown"
    base_out_dir = "out"
    if report_cfg_base and hasattr(report_cfg_base, "outputs") and report_cfg_base.outputs:
        if hasattr(report_cfg_base.outputs, "dir") and report_cfg_base.outputs.dir:
            base_out_dir = report_cfg_base.outputs.dir
    out_base_dir = Path(base_out_dir) / "drilldown"

    base_join_keys = (getattr(join_cfg, "keys", None) or []) if join_cfg else []
    join_type = (getattr(join_cfg, "type", None) or "outer") if join_cfg else "outer"

    for idx, level in enumerate(levels, start=1):
        # Resolve per-level adds/removes (support both common and per-side)
        add_common = getattr(level, "add", None) if strategy == "add" else None
        rem_common = getattr(level, "remove", None) if strategy == "remove" else None
        A_add = getattr(level, "A_add", None) or add_common or []
        B_add = getattr(level, "B_add", None) or add_common or []
        A_rem = getattr(level, "A_remove", None) or rem_common or []
        B_rem = getattr(level, "B_remove", None) or rem_common or []

        # Adjust group_by for each side
        aggA = _adjust_groupby(aggA_base, add=A_add, remove=A_rem)
        aggB = _adjust_groupby(aggB_base, add=B_add, remove=B_rem)
        log.info("drilldown level %02d: aggA=%s aggB=%s", idx, bool(aggA), bool(aggB))
        # Aggregate at this level
        A_agg = aggregate(A_prepared, aggA)
        B_agg = aggregate(B_prepared, aggB)

        # Determine join keys at this level:
        # base keys + (added dims that exist on both sides)
        common_added = [c for c in A_add if c in B_add]
        # keep only columns that truly exist post-aggregation
        A_cols = set(A_agg.columns)
        B_cols = set(B_agg.columns)
        common_added = [c for c in common_added if (c in A_cols and c in B_cols)]
        level_join_keys = _dedup_keep_order(list(base_join_keys) + common_added)

        # Join directly on columns (no recon_key) so keys appear unsuffixed
        dfJ = join(
            A_agg, B_agg,
            keys=level_join_keys,
            how=join_type,
            key_name=None  # <-- ensure unsuffixed key columns
        )

        # Reconcile
        dfR = reconcile(dfJ, getattr(full_cfg, "reconcile", None))

        # Output directory per level (mutate dataclass copy)
        report_cfg = deepcopy(report_cfg_base)
        if report_cfg and hasattr(report_cfg, "outputs") and report_cfg.outputs is not None:
            if hasattr(report_cfg.outputs, "dir"):
                report_cfg.outputs.dir = str(out_base_dir / f"level_{idx:02d}")

        # Selection (reuse top-level selection if present)
        select_cols = None
        if report_cfg and hasattr(report_cfg, "select") and report_cfg.select is not None:
            if hasattr(report_cfg.select, "keys") and report_cfg.select.keys is not None:
                select_cols = report_cfg.select.keys

        # drilldown will have columns from group by + select_cols
        drilldown_columns = (A_add or []) + (select_cols or [])
        log.debug("drilldown level %02d: columns=%s", idx, drilldown_columns)
        # DEBUG: uncomment to inspect columns
        # log.debug("join keys this level=%s", level_join_keys)
        seen = set()
        # no duplicate columns post drilldown selections
        result_columns = [x for x in drilldown_columns if not (x in seen or seen.add(x))]

        emit_reports(dfR, report_cfg, select_cols=result_columns)