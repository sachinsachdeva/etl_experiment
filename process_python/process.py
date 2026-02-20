#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

VALID_TIERS = {"bronze", "silver", "gold", "platinum"}


def parse_int(value: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def clamp_int(value: int, low: int, high: int) -> int:
    if value < low:
        return low
    if value > high:
        return high
    return value


def round_div(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        return 0
    if numerator <= 0:
        return 0
    return (numerator + denominator // 2) // denominator


def parse_event_hour(event_ts: str) -> int:
    if len(event_ts) < 13 or event_ts[10] != "T":
        return -1
    hour = parse_int(event_ts[11:13])
    if 0 <= hour <= 23:
        return hour
    return -1


def time_bucket_from_hour(hour: int) -> str:
    if 0 <= hour < 6:
        return "night"
    if 6 <= hour < 12:
        return "morning"
    if 12 <= hour < 18:
        return "afternoon"
    if 18 <= hour < 24:
        return "evening"
    return "unknown"


def order_size_bucket(quantity: int) -> str:
    if quantity <= 1:
        return "single"
    if quantity <= 3:
        return "small_multi"
    return "bulk"


def load_product_dim(dim_path: Path) -> dict[int, tuple[str, int, int]]:
    product_map: dict[int, tuple[str, int, int]] = {}
    with dim_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            product_id = parse_int(row.get("product_id", ""))
            if product_id <= 0:
                continue

            category = (row.get("category", "") or "").strip().lower() or "unknown"
            margin_bps = clamp_int(parse_int(row.get("margin_bps", "")), 0, 9500)
            weight_grams = clamp_int(parse_int(row.get("weight_grams", "")), 1, 20000)
            product_map[product_id] = (category, margin_bps, weight_grams)
    return product_map


def load_country_dim(dim_path: Path) -> dict[str, tuple[int, int, int]]:
    country_map: dict[str, tuple[int, int, int]] = {}
    with dim_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            country = (row.get("country", "") or "").strip().upper()
            if not country:
                continue

            fx_to_usd_ppm = clamp_int(parse_int(row.get("fx_to_usd_ppm", "")), 1, 2_500_000)
            risk_bps = clamp_int(parse_int(row.get("risk_bps", "")), 1, 20_000)
            tax_bps = clamp_int(parse_int(row.get("tax_bps", "")), 0, 5_000)
            country_map[country] = (fx_to_usd_ppm, risk_bps, tax_bps)
    return country_map


def transform(
    events_path: Path,
    product_dim_path: Path,
    country_dim_path: Path,
    output_path: Path,
) -> tuple[int, int, int]:
    product_map = load_product_dim(product_dim_path)
    country_map = load_country_dim(country_dim_path)

    dedup: dict[str, dict[str, object]] = {}

    raw_rows = 0
    filtered_rows = 0

    with events_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_rows += 1

            event_id = (row.get("event_id", "") or "").strip()
            if not event_id:
                continue

            event_version = parse_int(row.get("event_version", ""))
            event_ts = (row.get("event_ts", "") or "").strip()
            event_date = (row.get("event_date", "") or "").strip()
            customer_id = parse_int(row.get("customer_id", ""))
            product_id = parse_int(row.get("product_id", ""))
            amount_cents = parse_int(row.get("amount_cents", ""))
            quantity = parse_int(row.get("quantity", ""))
            discount_bps = clamp_int(parse_int(row.get("discount_bps", "")), 0, 5000)
            shipping_cents = clamp_int(parse_int(row.get("shipping_cents", "")), 0, 25000)
            status = (row.get("status", "") or "").strip().upper()
            country = (row.get("country", "") or "").strip().upper()
            customer_tier = (row.get("customer_tier", "") or "").strip().lower()

            if status != "COMPLETE" or amount_cents <= 0 or quantity <= 0:
                continue
            if customer_id <= 0 or product_id <= 0 or not event_date or not event_ts:
                continue

            filtered_rows += 1
            if customer_tier not in VALID_TIERS:
                customer_tier = "unknown"

            candidate = {
                "event_version": event_version,
                "event_ts": event_ts,
                "event_date": event_date,
                "customer_id": customer_id,
                "product_id": product_id,
                "amount_cents": amount_cents,
                "quantity": quantity,
                "discount_bps": discount_bps,
                "shipping_cents": shipping_cents,
                "country": country,
                "customer_tier": customer_tier,
            }

            current = dedup.get(event_id)
            should_replace = False
            if current is None:
                should_replace = True
            else:
                cur_version = int(current["event_version"])
                cur_ts = str(current["event_ts"])
                if event_version > cur_version or (event_version == cur_version and event_ts > cur_ts):
                    should_replace = True

            if should_replace:
                dedup[event_id] = candidate

    customer_day_spend: dict[tuple[str, int], int] = {}
    enriched_rows: list[dict[str, object]] = []

    for row in dedup.values():
        event_date = str(row["event_date"])
        event_ts = str(row["event_ts"])
        customer_id = int(row["customer_id"])
        product_id = int(row["product_id"])
        quantity = int(row["quantity"])
        amount_cents = int(row["amount_cents"])
        discount_bps = int(row["discount_bps"])
        shipping_cents = int(row["shipping_cents"])
        country = str(row["country"])
        customer_tier = str(row["customer_tier"])

        category, margin_bps, weight_grams = product_map.get(product_id, ("unknown", 2500, 500))
        fx_to_usd_ppm, risk_bps, tax_bps = country_map.get(country, (1_000_000, 10_000, 0))

        gross_local_cents = amount_cents * quantity + shipping_cents
        discount_local_cents = round_div(gross_local_cents * discount_bps, 10_000)
        taxable_local_cents = max(gross_local_cents - discount_local_cents, 0)
        tax_local_cents = round_div(taxable_local_cents * tax_bps, 10_000)
        net_local_cents = taxable_local_cents + tax_local_cents

        net_usd_cents = round_div(net_local_cents * fx_to_usd_ppm, 1_000_000)
        cost_usd_cents = round_div(net_usd_cents * (10_000 - margin_bps), 10_000)
        profit_usd_cents = net_usd_cents - cost_usd_cents
        risk_adjusted_usd_cents = round_div(net_usd_cents * risk_bps, 10_000)

        hour = parse_event_hour(event_ts)
        time_bucket = time_bucket_from_hour(hour)
        size_bucket = order_size_bucket(quantity)
        heavy_item_order = 1 if weight_grams * quantity >= 5_000 else 0

        customer_day_key = (event_date, customer_id)
        customer_day_spend[customer_day_key] = customer_day_spend.get(customer_day_key, 0) + net_usd_cents

        enriched_rows.append(
            {
                "event_date": event_date,
                "customer_id": customer_id,
                "customer_tier": customer_tier,
                "category": category,
                "country": country,
                "time_bucket": time_bucket,
                "size_bucket": size_bucket,
                "quantity": quantity,
                "net_usd_cents": net_usd_cents,
                "profit_usd_cents": profit_usd_cents,
                "risk_adjusted_usd_cents": risk_adjusted_usd_cents,
                "heavy_item_order": heavy_item_order,
            }
        )

    aggregated: dict[tuple[str, str, str, str, str, str], list[int]] = {}

    for row in enriched_rows:
        event_date = str(row["event_date"])
        customer_id = int(row["customer_id"])
        customer_tier = str(row["customer_tier"])
        category = str(row["category"])
        country = str(row["country"])
        time_bucket = str(row["time_bucket"])
        size_bucket = str(row["size_bucket"])
        quantity = int(row["quantity"])
        net_usd_cents = int(row["net_usd_cents"])
        profit_usd_cents = int(row["profit_usd_cents"])
        risk_adjusted_usd_cents = int(row["risk_adjusted_usd_cents"])
        heavy_item_order = int(row["heavy_item_order"])

        vip_customer_order = 1 if customer_day_spend.get((event_date, customer_id), 0) >= 50_000 else 0

        key = (event_date, customer_tier, category, country, time_bucket, size_bucket)
        bucket = aggregated.setdefault(key, [0, 0, 0, 0, 0, 0, 0, 0])

        bucket[0] += 1
        bucket[1] += vip_customer_order
        bucket[2] += quantity
        bucket[3] += net_usd_cents
        bucket[4] += profit_usd_cents
        bucket[5] += risk_adjusted_usd_cents
        bucket[6] += quantity
        bucket[7] += heavy_item_order

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(
            [
                "event_date",
                "customer_tier",
                "category",
                "country",
                "time_bucket",
                "order_size_bucket",
                "order_count",
                "vip_customer_orders",
                "total_quantity",
                "total_net_usd_cents",
                "total_profit_usd_cents",
                "total_risk_adjusted_usd_cents",
                "avg_item_price_usd_cents",
                "heavy_item_orders",
            ]
        )

        for key, metrics in sorted(aggregated.items(), key=lambda item: item[0]):
            (
                event_date,
                customer_tier,
                category,
                country,
                time_bucket,
                size_bucket,
            ) = key
            (
                order_count,
                vip_customer_orders,
                total_quantity,
                total_net_usd_cents,
                total_profit_usd_cents,
                total_risk_adjusted_usd_cents,
                total_items,
                heavy_item_orders,
            ) = metrics

            avg_item_price_usd_cents = round_div(total_net_usd_cents, total_items)

            writer.writerow(
                [
                    event_date,
                    customer_tier,
                    category,
                    country,
                    time_bucket,
                    size_bucket,
                    order_count,
                    vip_customer_orders,
                    total_quantity,
                    total_net_usd_cents,
                    total_profit_usd_cents,
                    total_risk_adjusted_usd_cents,
                    avg_item_price_usd_cents,
                    heavy_item_orders,
                ]
            )

    return raw_rows, filtered_rows, len(aggregated)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Python ETL transform")
    parser.add_argument("events_csv", type=Path)
    parser.add_argument("product_dim_csv", type=Path)
    parser.add_argument("country_dim_csv", type=Path)
    parser.add_argument("output_csv", type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raw_rows, filtered_rows, aggregate_rows = transform(
        args.events_csv,
        args.product_dim_csv,
        args.country_dim_csv,
        args.output_csv,
    )
    print(
        "python transform completed | "
        f"raw_rows={raw_rows} filtered_rows={filtered_rows} aggregate_rows={aggregate_rows} "
        f"output={args.output_csv}"
    )


if __name__ == "__main__":
    main()
