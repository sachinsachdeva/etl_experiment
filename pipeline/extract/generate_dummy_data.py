#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

CATEGORIES = [
    "electronics",
    "apparel",
    "home",
    "grocery",
    "sports",
    "books",
    "beauty",
    "toys",
]

COUNTRY_FACTORS: dict[str, tuple[int, int, int]] = {
    "US": (1_000_000, 10_000, 850),
    "CA": (740_000, 10_150, 500),
    "GB": (1_260_000, 10_200, 2_000),
    "IN": (12_000, 10_800, 1_800),
    "DE": (1_080_000, 9_950, 1_900),
    "FR": (1_090_000, 10_050, 2_000),
    "AU": (660_000, 10_250, 1_000),
    "SG": (740_000, 9_900, 900),
}

CUSTOMER_TIERS = ["bronze", "silver", "gold", "platinum"]
TIER_WEIGHTS = [0.45, 0.30, 0.18, 0.07]

PAYMENT_METHODS = ["card", "bank_transfer", "wallet", "upi", "cod"]
PAYMENT_WEIGHTS = [0.58, 0.12, 0.16, 0.10, 0.04]

STATUSES = ["COMPLETE", "PENDING", "CANCELLED"]
STATUS_WEIGHTS = [0.74, 0.17, 0.09]


def maybe_bad_int(value: int, rng: random.Random, empty_rate: float = 0.01, bad_rate: float = 0.005) -> str:
    r = rng.random()
    if r < empty_rate:
        return ""
    if r < empty_rate + bad_rate:
        return "bad"
    return str(value)


def maybe_bad_status(status: str, rng: random.Random) -> str:
    r = rng.random()
    if r < 0.008:
        return ""
    if r < 0.012:
        return "INVALID"
    return status


def maybe_bad_country(country: str, rng: random.Random) -> str:
    r = rng.random()
    if r < 0.004:
        return ""
    if r < 0.008:
        return "ZZ"
    return country


def maybe_bad_tier(tier: str, rng: random.Random) -> str:
    r = rng.random()
    if r < 0.008:
        return ""
    if r < 0.012:
        return "diamond"
    return tier


def maybe_bad_event_ts(event_ts: str, rng: random.Random) -> str:
    r = rng.random()
    if r < 0.004:
        return ""
    if r < 0.008:
        return event_ts.replace("T", " ")
    return event_ts


def generate_product_dim(path: Path, num_products: int, rng: random.Random) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "category", "margin_bps", "weight_grams"])
        for product_id in range(1, num_products + 1):
            writer.writerow(
                [
                    product_id,
                    rng.choice(CATEGORIES),
                    rng.randint(1_600, 7_200),
                    rng.randint(120, 4_500),
                ]
            )


def generate_country_dim(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["country", "fx_to_usd_ppm", "risk_bps", "tax_bps"])
        for country in sorted(COUNTRY_FACTORS):
            fx_to_usd_ppm, risk_bps, tax_bps = COUNTRY_FACTORS[country]
            writer.writerow([country, fx_to_usd_ppm, risk_bps, tax_bps])


def generate_events(path: Path, rows: int, num_products: int, rng: random.Random) -> None:
    start_date = date(2025, 1, 1)
    countries = sorted(COUNTRY_FACTORS)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "event_id",
                "event_version",
                "event_ts",
                "event_date",
                "customer_id",
                "product_id",
                "amount_cents",
                "quantity",
                "discount_bps",
                "shipping_cents",
                "status",
                "country",
                "customer_tier",
                "payment_method",
            ]
        )

        for i in range(rows):
            if i > 0 and rng.random() < 0.08:
                base = rng.randint(0, i - 1)
                event_id = f"E{base:012d}"
                event_version = rng.randint(1, 8)
            else:
                event_id = f"E{i:012d}"
                event_version = 1

            event_date = start_date + timedelta(days=rng.randint(0, 89))
            event_ts = (
                f"{event_date.isoformat()}T"
                f"{rng.randint(0, 23):02d}:{rng.randint(0, 59):02d}:{rng.randint(0, 59):02d}"
            )
            customer_id = rng.randint(1, 120_000)
            product_id = rng.randint(1, num_products)
            amount_cents = rng.randint(100, 50_000)
            quantity = rng.randint(1, 10)
            discount_bps = rng.randint(0, 3_500)
            shipping_cents = rng.randint(0, 2_500)
            status = rng.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0]
            country = rng.choice(countries)
            customer_tier = rng.choices(CUSTOMER_TIERS, weights=TIER_WEIGHTS, k=1)[0]
            payment_method = rng.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS, k=1)[0]

            writer.writerow(
                [
                    event_id,
                    event_version,
                    maybe_bad_event_ts(event_ts, rng),
                    event_date.isoformat(),
                    customer_id,
                    product_id,
                    maybe_bad_int(amount_cents, rng),
                    maybe_bad_int(quantity, rng),
                    maybe_bad_int(discount_bps, rng),
                    maybe_bad_int(shipping_cents, rng, empty_rate=0.008, bad_rate=0.004),
                    maybe_bad_status(status, rng),
                    maybe_bad_country(country, rng),
                    maybe_bad_tier(customer_tier, rng),
                    payment_method,
                ]
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate deterministic ETL dummy data")
    parser.add_argument("--rows", type=int, default=200_000, help="Number of event rows")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument(
        "--num-products", type=int, default=5_000, help="Product dimension cardinality"
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/raw"),
        help="Directory for generated CSV files",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.rows <= 0:
        raise ValueError("--rows must be > 0")
    if args.num_products <= 0:
        raise ValueError("--num-products must be > 0")

    out_dir = args.out_dir
    events_path = out_dir / "events.csv"
    product_dim_path = out_dir / "dim_products.csv"
    country_dim_path = out_dir / "dim_countries.csv"

    rng = random.Random(args.seed)
    generate_product_dim(product_dim_path, args.num_products, rng)
    generate_country_dim(country_dim_path)
    generate_events(events_path, args.rows, args.num_products, rng)

    print(f"Generated {args.rows} event rows -> {events_path}")
    print(f"Generated {args.num_products} product dimension rows -> {product_dim_path}")
    print(f"Generated {len(COUNTRY_FACTORS)} country dimension rows -> {country_dim_path}")


if __name__ == "__main__":
    main()
