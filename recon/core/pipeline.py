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
from .config import RootCfg

import logging
log = logging.getLogger(__name__)

import time
from contextlib import contextmanager

@contextmanager
def _stage(name: str):
    start = time.perf_counter()
    log.info("%s: start", name)
    try:
        yield
    finally:
        dur = time.perf_counter() - start
        log.info("%s: done in %.3fs", name, dur)


_DEF_JOIN_TYPE='outer'

def _load_config(path: str) -> tuple[RootCfg, str]:
    text = Path(path).read_text()
    raw = yaml.safe_load(text) or {}
    # Prefer a factory that recursively builds nested dataclasses
    if hasattr(RootCfg, 'from_dict') and callable(getattr(RootCfg, 'from_dict')):
        return RootCfg.from_dict(raw), text
    return RootCfg(**raw), text


def _load_and_prepare(label: str, cfg) -> pd.DataFrame:
    # cfg is a dataclass (e.g., InputCfg) with attributes like path, delimiter, encoding, dtypes, header
    read = ReadSpec(
        path=cfg.path,
        delimiter=getattr(cfg, 'delimiter', None),
        encoding=getattr(cfg, 'encoding', None),
        dtypes=getattr(cfg, 'dtypes', None),
        header=getattr(cfg, 'header', None),
    )
    df = read.read()
    df = sanitize(df, getattr(cfg, 'sanitize', None))
    df = apply_filters(df, getattr(cfg, 'prefilter', None))
    return df


def run_job(config_path: str, out_dir: str, backend_name: str = 'pandas'):
    try:
        log.info("Run started: backend=%s, out_dir=%s", backend_name, out_dir)
        cfg, cfg_text = _load_config(config_path)
        log.debug("config loaded from %s", config_path)
        job = getattr(cfg, 'job', None)
        if job : 
            log.info(" **** Running Reconciliation Framework for %s **** ", getattr(job,'name', ''))
        inputs = getattr(cfg, 'inputs', None)
        if inputs is None:
            raise ValueError('Config must define inputs')
        log.debug("inputs resolved: %s", inputs)
        A_cfg = getattr(inputs, 'A', None)
        B_cfg = getattr(inputs, 'B', None)
        log.debug("input A: %s", A_cfg)
        log.debug("input B: %s", B_cfg)
        if A_cfg is None or B_cfg is None:
            raise ValueError('Config must define inputs.A and inputs.B')
        
        suffix_A = f"_A"
        suffix_B = f"_B"
        with _stage("read+prep A"):
            A = _load_and_prepare('A', A_cfg)
            log.debug("A shape: %s", getattr(A, 'shape', None))
            # DEBUG: uncomment to sample rows during investigation
            # log.debug("A head:\n%s", A.head(5))
        with _stage("read+prep B"):
            B = _load_and_prepare('B', B_cfg)
            log.debug("B shape: %s", getattr(B, 'shape', None))
            # DEBUG: uncomment to sample rows during investigation
            # log.debug("B head:\n%s", B.head(5))
        # Aggregate separately
        with _stage("aggregate"):
            A_agg = aggregate(A, getattr(getattr(cfg, 'aggregate', None), 'A', None))
            B_agg = aggregate(B, getattr(getattr(cfg, 'aggregate', None), 'B', None))
            log.debug("A_agg shape: %s; B_agg shape: %s", getattr(A_agg, 'shape', None), getattr(B_agg, 'shape', None))
        # Join
        join_cfg = getattr(cfg, 'join', None)
        keys = (getattr(join_cfg, 'keys', None) or []) if join_cfg else []
        join_type = (getattr(join_cfg, 'type', None) or _DEF_JOIN_TYPE) if join_cfg else _DEF_JOIN_TYPE
        key_name = getattr(join_cfg, 'key_name', None) if join_cfg else None
        log.info("join: keys=%s, how=%s, key_name=%s", keys, join_type, key_name)
        with _stage("join"):
            df = join(A_agg, B_agg, keys=keys, how=join_type, key_name=key_name, prefix_A=suffix_A, prefix_B=suffix_B)
            log.debug("joined shape: %s", getattr(df, 'shape', None))
            # DEBUG: uncomment to sample rows during investigation
            # log.debug("joined head:\n%s", df.head(5))
        # Reconcile
        with _stage("reconcile"):
            df = reconcile(
                df,
                getattr(cfg, 'reconcile', None),
                recon_cols_section='numeric',
                prefix_A=suffix_A,
                prefix_B=suffix_B,
            )
        log.debug("post-reconcile shape: %s", getattr(df, 'shape', None))
        # Reports
        report_cfg = getattr(cfg, 'report', None)
        select_keys = None
        if report_cfg is not None and getattr(report_cfg, 'select', None) is not None:
            if getattr(report_cfg.select, 'keys', None) is not None:
                select_keys = report_cfg.select.keys
        with _stage("emit_reports"):
            emit_reports(df, report_cfg, select_cols=select_keys, suffix_A=suffix_A, suffix_B=suffix_B)
        # Audit
        with _stage("audit"):
            write_audit(out_dir, cfg_text)
        # === NEW: Drill-down (iterative un-group) ===
        # ...
        # === Drill paths ===
        dd = getattr(cfg, 'drilldown', None)
        if dd and getattr(dd, 'enabled', False) and getattr(dd, 'levels', None):
            log.info("drilldown: enabled strategy=%s levels=%d", getattr(dd, 'strategy', 'add'), len(dd.levels))
            with _stage("drilldown"):
                run_drilldown(A, B, cfg, dd.levels, strategy=getattr(dd, 'strategy', 'add'))
        else:
            log.info("drilldown: disabled")
        log.info("Run completed: backend=%s", backend_name)
        return df
    except Exception:
        log.exception("Pipeline failed: config_path=%s", config_path)
        raise
