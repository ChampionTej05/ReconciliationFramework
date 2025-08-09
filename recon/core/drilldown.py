# recon/core/drilldown.py
from __future__ import annotations
from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Any, Optional

import pandas as pd

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
    gb = spec2.get("group_by") or []
    if remove:
        rem = set(remove)
        gb = [c for c in gb if c not in rem]
    if add:
        gb = _dedup_keep_order(list(gb) + list(add))
    spec2["group_by"] = gb
    return spec2

def _columns_present(df: pd.DataFrame, cols: List[str]) -> List[str]:
    return [c for c in cols if c in df.columns]

def run_drilldown(
    A_prepared: pd.DataFrame,
    B_prepared: pd.DataFrame,
    full_cfg: Dict[str, Any],
    levels: List[Dict[str, Any]],
    strategy: str = "add",  # "add" (drill-down) or "remove" (drill-up)
) -> None:
    if not levels:
        return
    print("f{A_prepared} \n {B_prepared}", A_prepared, B_prepared )
    agg_cfg = full_cfg.get("aggregate", {})
    aggA_base = agg_cfg.get("A")
    aggB_base = agg_cfg.get("B")
    join_cfg = full_cfg.get("join", {}) or {}
    report_cfg_base = deepcopy(full_cfg.get("report", {}) or {})

    out_base_dir = Path(report_cfg_base.get("outputs", {}).get("dir", "out")) / "drilldown"

    base_join_keys = join_cfg.get("keys", []) or []
    join_type = join_cfg.get("type", "outer")

    for idx, level in enumerate(levels, start=1):
        # Resolve per-level adds/removes (support both common and per-side)
        add_common = level.get("add") if strategy == "add" else None
        rem_common = level.get("remove") if strategy == "remove" else None
        A_add = level.get("A_add") or add_common or []
        B_add = level.get("B_add") or add_common or []
        A_rem = level.get("A_remove") or rem_common or []
        B_rem = level.get("B_remove") or rem_common or []

        # Adjust group_by for each side
        aggA = _adjust_groupby(aggA_base, add=A_add, remove=A_rem)
        aggB = _adjust_groupby(aggB_base, add=B_add, remove=B_rem)
        print("AggA", aggA)
        print("AggB", aggB)
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
        dfR = reconcile(dfJ, full_cfg.get("reconcile", {}))

        # Output directory per level
        report_cfg = deepcopy(report_cfg_base)
        report_cfg.setdefault("outputs", {})
        report_cfg["outputs"]["dir"] = str(out_base_dir / f"level_{idx:02d}")

        # Selection (reuse top-level selection if present)
        select_cols = None
        if report_cfg.get("select") and report_cfg["select"].get("keys"):
            select_cols = report_cfg["select"]["keys"]

        # drilldown will have columns from group by + select_cols 
        drilldown_columns = [A_add + select_cols][0]
        print("Drilldown Columns", drilldown_columns)
        seen = set()
        # no duplicate columns post drilldown selections 
        result_columns = [x for x in drilldown_columns if not (x in seen or seen.add(x))]

        emit_reports(dfR, report_cfg, select_cols=result_columns)