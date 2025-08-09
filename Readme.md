# ReconciliationFramework

A config‑driven, pure‑Python framework for reconciling two datasets (typically CSVs). Define reconciliation jobs in YAML—no rewrites per use case. The pipeline is composable: sanitize → filter → aggregate → join → reconcile → report → (optional) drill‑down.

---

## Introduction

ReconciliationFramework enables robust, auditable, and scalable reconciliation of two datasets (e.g., CSV files) using a YAML-based configuration. No need to rewrite scripts per use case.

---

## Objectives

- **Config over code:** Define new reconciliations via YAML, not scripts.
- **Deterministic & auditable:** Stable outputs, audit and metrics for every run.
- **Scalable on a laptop:** Built for ~5–6M rows × up to 10 columns per file.
- **Extensible:** Plug in new comparators, backends, drill‑down strategies.

---

## What It Does

- **Inputs:** Reads CSVs (A & B), enforces dtypes, normalizes headers.
- **Sanitizing:** Rename, select, uppercase/trim, consistent column naming.
- **Filtering:** Vectorized predicates (eq/in/gt/…).
- **Aggregation:** Per‑side `group_by` & metrics (sum, count, nunique, …).
- **Join:** inner/left/right/full; coalesces join keys to unsuffixed columns.
- **Reconciliation:** Numeric columns with absolute/relative/rounded tolerance; deltas + flags.
- **Reporting:** `matched.csv`, `non_matched.csv`, `differences.csv`, `metrics.json`.
- **Drill‑down:** Optional “add‑dimensions” levels (e.g., add `trade_id` → `asof_date`) to pinpoint variance.
- **Labels:** Optionally relabel `A_`/`B_` columns to friendly dataset names in reports.

---

## Quick Start

1. **Setup Git Repo and Python Env if you haven't already**
    ```bash
    cd project_root
    python -m venv .venv
    source .venv/bin/activate         # Windows: .venv\Scripts\activate
    pip install -r requirements.txt   
    ```

2. **(Optional) Prepare sample data**

   Put CSVs under `sample/`. The provided `example.yaml` expects `sample/fdcs.csv` and `sample/calc.csv`.

3. **Run a recon job**

   Always run as a module so package imports work:
   ```bash
   python -m recon.cli --config configs/example.yaml --out out --backend pandas
   ```
   Outputs land under `out/...`:
   - `matched.csv`
   - `non_matched.csv`
   - `differences.csv`
   - `metrics.json`
   - `audit.json`
   - If drill‑down enabled: per‑level folders under `out/.../drilldown/level_XX`

---

## YAML Configuration Guide

A recon job is entirely defined in YAML. Anatomy:

```yaml
job:
  name: "example_fdcs_calc_8cols"
  backend: pandas
  join_type: outer
  timezone: "Asia/Kolkata"

inputs:
  A:                           # Dataset A (left)
    path: "./sample/fdcs.csv"
    delimiter: ","
    encoding: "utf-8"
    dtypes:                    # enforce types (date/float/string)
      trade_id: "string"
      book: "string"
      ccy: "string"
      currencyAmount: "float64"
      asof_date: "date"
    sanitize:                  # standardize columns
      rename: { TRD_ID: trade_id, CCY: ccy, BAL: currencyAmount, ASOF: asof_date }
      select: [trade_id, book, ccy, currencyAmount, asof_date]
      normalize:
        trim_strings: true
        upper_case: [book, ccy]

  B:                           # Dataset B (right)
    path: "./sample/calc.csv"
    dtypes:
      trade_id: "string"
      book: "string"
      ccy: "string"
      currencyAmount: "float64"
      asof_date: "date"
    sanitize:
      rename: { TRADE: trade_id, CURRENCY: ccy, currency_amount: currencyAmount, ASOF: asof_date }
      select: [trade_id, book, ccy, currencyAmount, asof_date]
      normalize:
        trim_strings: true
        upper_case: [book, ccy]

filters:
  A:
    - { col: "ccy", op: "in", value: ["USD", "EUR"] }
  B:
    - { col: "ccy", op: "in", value: ["USD", "EUR"] }

aggregate:
  A:
    group_by: [book, ccy]      # each side can differ if needed
    metrics: { currencyAmount: { agg: "sum" } }
  B:
    group_by: [book, ccy]
    metrics: { currencyAmount: { agg: "sum" } }

join:
  keys: [book, ccy]            # join directly on columns -> unsuffixed keys in results
  key_name: null               # (set to name to synthesize a single hashed key ex: recon_key)
  type: outer

reconcile:
  numeric:
    - column: currencyAmount   # compare A vs B for this column
      comparator: relative
      tol_pct: 0.002           # 0.2% tolerance
      min_base: 1e-8
    - column: USDAmount
      comparator: relative
      tol_pct: 0.002
      min_base: 1e-8

report:
  outputs:
    dir: "out/example"
    formats: ["csv", "parquet"]
  dataset_names:               # relabel A_/B_ columns in exports (optional)
    A: "FDCS"
    B: "CALC"
  select:
    keys:
      - book
      - ccy
      - FDCS_currencyAmount
      - CALC_currencyAmount
      - delta_currencyAmount
      - abs_delta_currencyAmount
      - match_currencyAmount
      - match_flag

drilldown:
  enabled: true
  strategy: add                # “add” = drill-down (more granular); “remove” = drill-up
  levels:
    - add: [trade_id]          # base [book, ccy] → add trade_id
    - add: [trade_id, asof_date]  # then add asof_date
```

---

## How to Adapt to Any Two Datasets

1. **Map columns:** Use `sanitize.rename` to align different schemas to a common set.
2. **Pick the grain:** Set `aggregate.*.group_by` to the keys you consider equivalent (e.g., `[desk, ccy]`).
3. **Choose join keys:** Usually the same as `group_by`; if not, list the natural business keys here.
4. **Define recon columns:** List one or more numeric columns under `reconcile.numeric` with tolerances.
5. **Reports:** Use `dataset_names` to print friendly headers (e.g., `FDCS_balance`, `CALC_balance`) and `select.keys` to control visible columns in outputs.
6. **Drill:** Add levels with `strategy: add` to progressively include more dimensions (e.g., trade, counterparty, date).

> **Tip:** If one side lacks a column you “add” for drill‑down, the framework will still run; it only joins on the intersection that exists on both sides.

---

## Behavior Notes

- After join, missing numerics are filled as `0.0` so deltas compute (no NaNs).
- Keys used for join are added back to the result without `A_`/`B_` suffixes.
- `differences.csv` shows deltas (`delta_*`, `abs_delta_*`, `pct_delta_*`) and side‑specific fields.
- `metrics.json` includes basic counts; extend as you like.

---

## Troubleshooting

- **ImportError: attempted relative import with no known parent package**
  - Run with `python -m recon.cli ...` from repo root, or switch imports to absolute (`from recon.core.pipeline import run_job`).
- **Large files**
  - Prefer Pandas with pyarrow dtypes; if you hit memory ceilings, swap in a Polars backend (hooks are scaffolded).
