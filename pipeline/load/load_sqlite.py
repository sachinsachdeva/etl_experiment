#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
import sqlite3
from pathlib import Path

TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def parse_int(value: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def validate_table_name(table: str) -> None:
    if not TABLE_RE.match(table):
        raise ValueError(f"Invalid table name: {table}")


def load_to_sqlite(input_csv: Path, db_path: Path, table: str) -> int:
    validate_table_name(table)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.execute(
            f"""
            CREATE TABLE {table} (
                event_date TEXT NOT NULL,
                customer_tier TEXT NOT NULL,
                category TEXT NOT NULL,
                country TEXT NOT NULL,
                time_bucket TEXT NOT NULL,
                order_size_bucket TEXT NOT NULL,
                order_count INTEGER NOT NULL,
                vip_customer_orders INTEGER NOT NULL,
                total_quantity INTEGER NOT NULL,
                total_net_usd_cents INTEGER NOT NULL,
                total_profit_usd_cents INTEGER NOT NULL,
                total_risk_adjusted_usd_cents INTEGER NOT NULL,
                avg_item_price_usd_cents INTEGER NOT NULL,
                heavy_item_orders INTEGER NOT NULL
            )
            """
        )

        insert_sql = (
            f"INSERT INTO {table} ("
            "event_date, customer_tier, category, country, time_bucket, order_size_bucket, "
            "order_count, vip_customer_orders, total_quantity, total_net_usd_cents, "
            "total_profit_usd_cents, total_risk_adjusted_usd_cents, avg_item_price_usd_cents, heavy_item_orders"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )

        inserted = 0
        with input_csv.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows: list[tuple[object, ...]] = []
            for row in reader:
                rows.append(
                    (
                        (row.get("event_date", "") or "").strip(),
                        (row.get("customer_tier", "") or "").strip(),
                        (row.get("category", "") or "").strip(),
                        (row.get("country", "") or "").strip(),
                        (row.get("time_bucket", "") or "").strip(),
                        (row.get("order_size_bucket", "") or "").strip(),
                        parse_int(row.get("order_count", "")),
                        parse_int(row.get("vip_customer_orders", "")),
                        parse_int(row.get("total_quantity", "")),
                        parse_int(row.get("total_net_usd_cents", "")),
                        parse_int(row.get("total_profit_usd_cents", "")),
                        parse_int(row.get("total_risk_adjusted_usd_cents", "")),
                        parse_int(row.get("avg_item_price_usd_cents", "")),
                        parse_int(row.get("heavy_item_orders", "")),
                    )
                )
                if len(rows) >= 10_000:
                    conn.executemany(insert_sql, rows)
                    inserted += len(rows)
                    rows.clear()

            if rows:
                conn.executemany(insert_sql, rows)
                inserted += len(rows)

        conn.commit()
        return inserted
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load processed CSV into SQLite")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--table", type=str, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    inserted = load_to_sqlite(args.input, args.db, args.table)
    print(f"loaded table={args.table} rows={inserted} db={args.db}")


if __name__ == "__main__":
    main()
