SHELL := /bin/zsh

PYTHON ?= python3
ROWS ?= 200000
RUNS ?= 5
SEED ?= 42

RAW_EVENTS := data/raw/events.csv
RAW_PRODUCT_DIM := data/raw/dim_products.csv
RAW_COUNTRY_DIM := data/raw/dim_countries.csv
PY_OUT := data/processed/python_output.csv
RS_OUT := data/processed/rust_output.csv
DB := data/load/warehouse.db

.PHONY: compare generate transform-python transform-rust validate load clean

compare:
	$(PYTHON) bench/run_bench.py --rows $(ROWS) --runs $(RUNS) --seed $(SEED)

generate:
	$(PYTHON) pipeline/extract/generate_dummy_data.py --rows $(ROWS) --seed $(SEED) --out-dir data/raw

transform-python:
	$(PYTHON) process_python/process.py $(RAW_EVENTS) $(RAW_PRODUCT_DIM) $(RAW_COUNTRY_DIM) $(PY_OUT)

transform-rust:
	cargo build --release --manifest-path process_rust/Cargo.toml
	./process_rust/target/release/process_rust $(RAW_EVENTS) $(RAW_PRODUCT_DIM) $(RAW_COUNTRY_DIM) $(RS_OUT)

validate:
	$(PYTHON) bench/validate_outputs.py --python-output $(PY_OUT) --rust-output $(RS_OUT)

load:
	$(PYTHON) pipeline/load/load_sqlite.py --input $(PY_OUT) --db $(DB) --table fact_sales_python
	$(PYTHON) pipeline/load/load_sqlite.py --input $(RS_OUT) --db $(DB) --table fact_sales_rust

clean:
	rm -f data/raw/events.csv data/raw/dim_products.csv data/raw/dim_countries.csv
	rm -f data/processed/python_output.csv data/processed/rust_output.csv
	rm -f data/load/warehouse.db
	rm -f bench/results/*.json bench/results/*.csv
