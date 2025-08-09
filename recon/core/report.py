from __future__ import annotations
from pathlib import Path
import json
import pandas as pd

_DEF_FORMATS = ["csv"]

# --- add near the top of report.py ---
def _apply_dataset_labels(df, report_cfg):
    """
    Rename A_/B_ (or _A/_B) columns to user-provided dataset names.
    Returns (renamed_df, rename_map_old_to_new).
    """
    labels = (report_cfg.get("dataset_names") or {})
    a_label = labels.get("A")
    b_label = labels.get("B")
    rename_map = {}

    for col in df.columns:
        # prefix form: A_foo / B_foo
        if a_label and col.startswith("A_"):
            rename_map[col] = f"{a_label}_{col[2:]}"
        elif b_label and col.startswith("B_"):
            rename_map[col] = f"{b_label}_{col[2:]}"
        # suffix form: foo_A / foo_B
        elif a_label and col.endswith("_A"):
            rename_map[col] = f"{a_label}_{col[:-2]}"
        elif b_label and col.endswith("_B"):
            rename_map[col] = f"{b_label}_{col[:-2]}"

    return df.rename(columns=rename_map), rename_map

def _map_select_cols(select_cols, report_cfg):
    """Apply the same relabeling logic to select list (if user still uses A_/B_ names)."""
    if not select_cols:
        return None
    labels = (report_cfg.get("dataset_names") or {})
    a_label = labels.get("A")
    b_label = labels.get("B")

    def _map_one(name: str) -> str:
        if a_label and (name.startswith("A_") or name.endswith("_A")):
            core = name[2:] if name.startswith("A_") else name[:-2]
            return f"{a_label}_{core}"
        if b_label and (name.startswith("B_") or name.endswith("_B")):
            core = name[2:] if name.startswith("B_") else name[:-2]
            return f"{b_label}_{core}"
        return name

    return [_map_one(n) for n in select_cols]

def _write(df: pd.DataFrame, out: Path, name: str, formats: list[str]):
    out.mkdir(parents=True, exist_ok=True)
    if 'csv' in formats:
        df.to_csv(out / f"{name}.csv", index=False)
    if 'parquet' in formats:
        try:
            df.to_parquet(out / f"{name}.parquet", index=False)
        except Exception:
            pass

def emit_reports(df: pd.DataFrame, report_cfg: dict, select_cols: list[str] | None = None, suffix_A='_A', suffix_B='_B'):
    # print("DF post reconcile")
    print(df)
    outdir = Path(report_cfg['outputs']['dir'])
    formats = report_cfg['outputs'].get('formats', _DEF_FORMATS)
        # NEW: relabel A_/B_ columns -> dataset names
    df, rename_map = _apply_dataset_labels(df, report_cfg)

    # NEW: map select_cols through the same logic
    if select_cols:
        select_cols = _map_select_cols(select_cols, report_cfg)

    # Base selections
    if select_cols:
        base = [c for c in select_cols if c in df.columns]
    else:
        base = list(df.columns)
    matched = df[df.get('match_flag', False) == True][base]
    non_matched = df[df.get('match_flag', False) == False][base]
    # diffs = df[[c for c in df.columns if c.startswith('delta_') or c.startswith('abs_delta_') or c.startswith('pct_delta_')] + [c for c in base if c.endswith(suffix_A) or c.endswith(suffix_B)]]
    diff_cols = [c for c in df.columns if c.startswith(('delta_', 'abs_delta_', 'pct_delta_'))]
    # side_cols = [c for c in base if '_' in c]  # simple heuristic to include side-specific cols
    # diffs = df[base]
    diffs = df.copy()[base]
    _write(matched, outdir, 'matched', formats)
    _write(non_matched, outdir, 'non_matched', formats)
    _write(diffs, outdir, 'differences', formats)
    # metrics
    metrics = {
        'total': len(df),
        'matched': int(matched.shape[0]),
        'non_matched': int(non_matched.shape[0]),
    }
    (outdir / 'metrics.json').write_text(json.dumps(metrics, indent=2))
