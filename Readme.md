# ![Python Version](https://img.shields.io/badge/python-3.8%2B-blue) ![License](https://img.shields.io/badge/license-MIT-green) ![Build Status](https://img.shields.io/badge/build-passing-brightgreen)

<details>
<summary><strong>Table of Contents</strong></summary>

- [Introduction](#introduction)
- [Objectives](#objectives)
- [What It Does](#what-it-does)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Example Outputs](#example-outputs)
- [YAML Configuration Guide](#yaml-configuration-guide)
  - [Inputs](#inputs)
  - [Filters](#filters)
  - [Aggregation](#aggregation)
  - [Join](#join)
  - [Reconcile](#reconcile)
  - [Report](#report)
  - [Drilldown](#drilldown)
- [How to Adapt to Any Two Datasets](#how-to-adapt-to-any-two-datasets)
- [Extending the Framework](#extending-the-framework)
- [Detailed Architecture & System Design](#detailed-architecture--system-design)
- [CLI Help](#cli-help)
- [Contributing](#contributing)
- [License](#license)

</details>

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

## Installation

1. **Clone the repository**

   ```bash
   git clone https://github.com/yourusername/ReconciliationFramework.git
   cd ReconciliationFramework
   ```

2. **Create and activate a virtual environment**

   ```bash
   python -m venv .venv
   source .venv/bin/activate         # Windows: .venv\Scripts\activate
   ```

3. **Install required packages**

   ```bash
   pip install -r requirements.txt
   ```

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

## Example Outputs

### Sample CSV snippet (`matched.csv`)

```csv
book,ccy,FDCS_currencyAmount,CALC_currencyAmount,delta_currencyAmount,abs_delta_currencyAmount,match_currencyAmount,match_flag
Desk1,USD,1000.0,1000.5,-0.5,0.5,True,1
Desk2,EUR,2000.0,1999.0,1.0,1.0,True,1
```

### Sample JSON snippet (`metrics.json`)

```json
{
  "total_rows_A": 10000,
  "total_rows_B": 10000,
  "matched_rows": 9500,
  "non_matched_rows": 500
}
```

### Folder structure for drilldown outputs

```
out/
└── example/
    ├── matched.csv
    ├── non_matched.csv
    ├── differences.csv
    ├── metrics.json
    ├── audit.json
    └── drilldown/
        ├── level_01/
        └── level_02/
```

---

## YAML Configuration Guide

### Inputs

```yaml
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
```

### Filters

```yaml
filters:
  A:
    - { col: "ccy", op: "in", value: ["USD", "EUR"] }
  B:
    - { col: "ccy", op: "in", value: ["USD", "EUR"] }
```

### Aggregation

```yaml
aggregate:
  A:
    group_by: [book, ccy]      # each side can differ if needed
    metrics: { currencyAmount: { agg: "sum" } }
  B:
    group_by: [book, ccy]
    metrics: { currencyAmount: { agg: "sum" } }
```

### Join

```yaml
join:
  keys: [book, ccy]            # join directly on columns -> unsuffixed keys in results
  key_name: null               # (set to name to synthesize a single hashed key ex: recon_key)
  type: outer
```

### Reconcile

```yaml
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
```

### Report

```yaml
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
```

### Drilldown

```yaml
drilldown:
  enabled: true
  strategy: add                # “add” = drill-down (more granular); “remove” = drill-up
  levels:
    - add: [trade_id]          # base [book, ccy] → add trade_id
    - add: [trade_id, asof_date]  # then add asof_date
```

### Full YAML Example

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

## Extending the Framework

The framework is designed to be extensible. You can add your own components as follows:

- **New Comparators:** Implement custom comparator functions for numeric reconciliation (e.g., tolerance types beyond absolute and relative). Register them in the comparator module to be used in YAML.

- **New Backends:** Add support for alternative data processing backends (e.g., Polars, Dask). Implement the required interfaces for data loading, filtering, aggregation, and joining.

- **New Filters:** Develop additional filter predicates beyond the built-in ones (eq, in, gt, etc.) by extending the filtering logic. Register your filters so they can be specified in the YAML config.

Refer to the developer documentation and source code comments for integration details.

---
## Detailed Architecture & System Design

This section provides a comprehensive overview of the internal architecture, design principles, and data flow of the ReconciliationFramework. It explains how the framework processes data from input ingestion to report generation, how the YAML configuration drives execution, and where extensibility points exist. For a full detailed document, see [recon/Readme_architecture.md](recon/Readme_architecture.md).

---

## CLI Help

Run the following command to see available options and usage:

```bash
python -m recon.cli --help
```

Example output:

```
Usage: cli.py [OPTIONS]

Options:
  --config PATH        Path to YAML configuration file  [required]
  --out DIRECTORY      Output directory for reports     [default: out]
  --backend [pandas]   Backend to use for processing     [default: pandas]
  --verbose            Enable verbose logging
  --help               Show this message and exit.
```

---

## Contributing

I welcome contributions! Please follow these guidelines:

- **Coding Style:** Follow PEP 8 style conventions. Use black or similar formatter for consistency.

- **Branch Naming:** Use descriptive branch names such as `feature/add-new-comparator` or `bugfix/fix-join-logic`.

- **Development Install:** For development, install the package in editable mode:

  ```bash
  pip install -e .
  ```

- Submit pull requests with clear descriptions and tests.

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

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
