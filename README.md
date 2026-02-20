# ETL Performance Benchmark: Rust vs Python

This repository benchmarks a **typical ETL transform stage** where:

- Extract is shared (`pipeline/extract/generate_dummy_data.py`)
- Load is shared (`pipeline/load/load_sqlite.py`)
- Only transform differs:
  - Python (`process_python/process.py`)
  - Rust (`process_rust/src/main.rs`)

## Pipeline shape

1. Generate deterministic dummy raw + dimension data
2. Run transform in Python and Rust with identical logic
3. Validate outputs are byte-for-byte identical
4. Load each output into SQLite tables
5. Save benchmark metrics in `bench/results/`

```mermaid
flowchart LR
    A["Extract: generate_dummy_data.py"] --> B["events.csv"]
    A --> C["dim_products.csv"]
    A --> D["dim_countries.csv"]

    B --> E["Python Transform: process_python/process.py"]
    C --> E
    D --> E

    B --> F["Rust Transform: process_rust/main.rs"]
    C --> F
    D --> F

    E --> G["python_output.csv"]
    F --> H["rust_output.csv"]

    G --> I["validate_outputs.py"]
    H --> I

    G --> J["load_sqlite.py -> fact_sales_python"]
    H --> K["load_sqlite.py -> fact_sales_rust"]
    J --> L["warehouse.db"]
    K --> L

    E -. metrics .-> M["run_bench.py"]
    F -. metrics .-> M
    M --> N["bench results JSON/CSV"]
```

## Transform logic (identical in both implementations)

- Parse and type-cast rows with null/error handling
- Filter rows (`status == COMPLETE`, positive amount/quantity, valid keys)
- Deduplicate by `event_id` (keep highest `event_version`, then latest `event_ts`)
- Join with product dimension (`product_id -> category, margin_bps, weight_grams`)
- Join with country dimension (`country -> fx_to_usd_ppm, risk_bps, tax_bps`)
- Derive multi-step metrics (discount, tax, USD conversion, profit, risk-adjusted revenue)
- Derive bucketed features (`time_bucket`, `order_size_bucket`, heavy item flag)
- Compute customer-day spend and VIP order flag
- Aggregate by `event_date`, `customer_tier`, `category`, `country`, `time_bucket`, `order_size_bucket`

```mermaid
flowchart TB
    A["Input Events + Dimensions"] --> B["Parse + Type Cast + Null Handling"]
    B --> C["Filter Invalid / Non-COMPLETE Rows"]
    C --> D["Dedup by event_id (version, event_ts)"]
    D --> E["Join Product Dimension"]
    D --> F["Join Country Dimension"]
    E --> G["Financial Derivations (discount, tax, FX, profit, risk)"]
    F --> G
    G --> H["Feature Engineering (time bucket, order size, heavy item)"]
    H --> I["Customer-Day Spend Rollup"]
    I --> J["VIP Flag + Final Aggregation"]
    J --> K["Canonical Sorted Output CSV"]
```

## Quick start

```bash
make compare
```

Optional tuning:

```bash
make compare ROWS=1000000 RUNS=7 SEED=42
```

## Output artifacts

- Raw data: `data/raw/events.csv`, `data/raw/dim_products.csv`, `data/raw/dim_countries.csv`
- Processed outputs:
  - `data/processed/python_output.csv`
  - `data/processed/rust_output.csv`
- SQLite load target: `data/load/warehouse.db`
- Benchmark results: `bench/results/bench_<timestamp>.json` and `.csv`

## Notes for fair comparison

- Rust compile time is excluded from timed runs (pre-built in release mode)
- Python and Rust runs alternate order per iteration to reduce cache/order bias
- Validation step enforces identical final output
- Benchmark report now includes CPU and RAM metrics per run:
  - `cpu_util_pct`
  - `peak_rss_kb`, `peak_rss_mb`
