from __future__ import annotations
import yaml
from pathlib import Path
import pandas as pd
from .io import ReadSpec
from .sanitize import sanitize
from .filter import apply_filters
from .aggregate import aggregate
from .joiner import join
from .reconcile import reconcile
from .report import emit_reports
from .audit import write_audit
from .drilldown import run_drilldown   

_DEF_JOIN_TYPE='outer'

def _load_config(config_path: str) -> dict:
    text = Path(config_path).read_text()
    return yaml.safe_load(text), text


def _load_and_prepare(label: str, cfg: dict) -> pd.DataFrame:
    read = ReadSpec(**{k:v for k,v in cfg.items() if k in ['path','delimiter','encoding','dtypes','header']})
    df = read.read()
    df = sanitize(df, cfg.get('sanitize'))
    df = apply_filters(df, cfg.get('prefilter'))
    return df


def run_job(config_path: str, out_dir: str, backend_name: str = 'pandas'):
    cfg, cfg_text = _load_config(config_path)
    job = cfg.get('job', {})
    A_cfg = cfg.get('inputs', {}).get('A')
    B_cfg = cfg.get('inputs', {}).get('B')
    if A_cfg is None or B_cfg is None:
        raise ValueError('Config must define inputs.A and inputs.B')
    
    suffix_A = f"_A"
    suffix_B = f"_B"
    A = _load_and_prepare('A', A_cfg)
    B = _load_and_prepare('B', B_cfg)
    # Aggregate separately
    A_agg = aggregate(A, cfg.get('aggregate', {}).get('A'))
    B_agg= aggregate(B, cfg.get('aggregate', {}).get('B'))
    # Join
    join_cfg = cfg.get('join', {})
    keys = join_cfg.get('keys') or []
    join_type = join_cfg.get('type', _DEF_JOIN_TYPE)
    key_name = join_cfg.get('key_name')

    df = join(A_agg, B_agg, keys=keys, how=join_type, key_name=key_name,prefix_A=suffix_A, prefix_B=suffix_B)
    # print(df)
    # Reconcile
    df = reconcile(df, cfg.get('reconcile', {}),recon_cols_section='numeric',prefix_A=suffix_A, prefix_B=suffix_B)
    # Reports
    # print("select cols ", (cfg.get('report', {}).get('select', {}).get('keys') if cfg.get('report') else None))
    emit_reports(df, cfg.get('report', {}), select_cols=(cfg.get('report', {}).get('select', {}).get('keys') if cfg.get('report') else None), suffix_A=suffix_A, suffix_B=suffix_B)
    # Audit
    write_audit(out_dir, cfg_text)
    # === NEW: Drill-down (iterative un-group) ===
    # ...
    # === Drill paths ===
    dd = (cfg.get('drilldown') or {})
    if dd.get('enabled') and dd.get('levels'):
        strategy = dd.get('strategy', 'add')  # default to drill-down
        run_drilldown(A, B, cfg, dd['levels'], strategy=strategy)
    return df
