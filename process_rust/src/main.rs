use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::Path;

#[derive(Clone)]
struct ProductDim {
    category: String,
    margin_bps: i64,
    weight_grams: i64,
}

#[derive(Clone)]
struct CountryDim {
    fx_to_usd_ppm: i64,
    risk_bps: i64,
    tax_bps: i64,
}

#[derive(Clone)]
struct EventRecord {
    event_version: i64,
    event_ts: String,
    event_date: String,
    customer_id: i64,
    product_id: i64,
    amount_cents: i64,
    quantity: i64,
    discount_bps: i64,
    shipping_cents: i64,
    country: String,
    customer_tier: String,
}

#[derive(Clone)]
struct DerivedRecord {
    event_date: String,
    customer_id: i64,
    customer_tier: String,
    category: String,
    country: String,
    time_bucket: String,
    order_size_bucket: String,
    quantity: i64,
    net_usd_cents: i64,
    profit_usd_cents: i64,
    risk_adjusted_usd_cents: i64,
    heavy_item_order: i64,
}

#[derive(Default)]
struct AggregateRecord {
    order_count: i64,
    vip_customer_orders: i64,
    total_quantity: i64,
    total_net_usd_cents: i64,
    total_profit_usd_cents: i64,
    total_risk_adjusted_usd_cents: i64,
    total_items: i64,
    heavy_item_orders: i64,
}

fn parse_i64(value: &str) -> i64 {
    value.trim().parse::<i64>().unwrap_or(0)
}

fn clamp_i64(value: i64, low: i64, high: i64) -> i64 {
    if value < low {
        low
    } else if value > high {
        high
    } else {
        value
    }
}

fn round_div(numerator: i64, denominator: i64) -> i64 {
    if numerator <= 0 || denominator <= 0 {
        return 0;
    }
    (numerator + (denominator / 2)) / denominator
}

fn parse_event_hour(event_ts: &str) -> i64 {
    if event_ts.len() < 13 {
        return -1;
    }

    if event_ts.as_bytes()[10] != b'T' {
        return -1;
    }

    let hour = parse_i64(&event_ts[11..13]);
    if (0..=23).contains(&hour) {
        hour
    } else {
        -1
    }
}

fn time_bucket_from_hour(hour: i64) -> String {
    if (0..6).contains(&hour) {
        "night".to_string()
    } else if (6..12).contains(&hour) {
        "morning".to_string()
    } else if (12..18).contains(&hour) {
        "afternoon".to_string()
    } else if (18..24).contains(&hour) {
        "evening".to_string()
    } else {
        "unknown".to_string()
    }
}

fn order_size_bucket(quantity: i64) -> String {
    if quantity <= 1 {
        "single".to_string()
    } else if quantity <= 3 {
        "small_multi".to_string()
    } else {
        "bulk".to_string()
    }
}

fn split_csv_line(line: &str) -> Vec<&str> {
    line.trim_end_matches(&['\r', '\n'][..]).split(',').collect()
}

fn load_product_dim(dim_path: &Path) -> io::Result<HashMap<i64, ProductDim>> {
    let file = File::open(dim_path)?;
    let reader = BufReader::new(file);

    let mut product_map = HashMap::new();

    for (idx, line_res) in reader.lines().enumerate() {
        let line = line_res?;
        if idx == 0 || line.trim().is_empty() {
            continue;
        }

        let cols = split_csv_line(&line);
        if cols.len() < 4 {
            continue;
        }

        let product_id = parse_i64(cols[0]);
        if product_id <= 0 {
            continue;
        }

        let category_raw = cols[1].trim().to_ascii_lowercase();
        let category = if category_raw.is_empty() {
            "unknown".to_string()
        } else {
            category_raw
        };

        let margin_bps = clamp_i64(parse_i64(cols[2]), 0, 9500);
        let weight_grams = clamp_i64(parse_i64(cols[3]), 1, 20_000);

        product_map.insert(
            product_id,
            ProductDim {
                category,
                margin_bps,
                weight_grams,
            },
        );
    }

    Ok(product_map)
}

fn load_country_dim(dim_path: &Path) -> io::Result<HashMap<String, CountryDim>> {
    let file = File::open(dim_path)?;
    let reader = BufReader::new(file);

    let mut country_map = HashMap::new();

    for (idx, line_res) in reader.lines().enumerate() {
        let line = line_res?;
        if idx == 0 || line.trim().is_empty() {
            continue;
        }

        let cols = split_csv_line(&line);
        if cols.len() < 4 {
            continue;
        }

        let country = cols[0].trim().to_ascii_uppercase();
        if country.is_empty() {
            continue;
        }

        let fx_to_usd_ppm = clamp_i64(parse_i64(cols[1]), 1, 2_500_000);
        let risk_bps = clamp_i64(parse_i64(cols[2]), 1, 20_000);
        let tax_bps = clamp_i64(parse_i64(cols[3]), 0, 5_000);

        country_map.insert(
            country,
            CountryDim {
                fx_to_usd_ppm,
                risk_bps,
                tax_bps,
            },
        );
    }

    Ok(country_map)
}

fn transform(
    events_path: &Path,
    product_dim_path: &Path,
    country_dim_path: &Path,
    output_path: &Path,
) -> io::Result<(i64, i64, i64)> {
    let product_map = load_product_dim(product_dim_path)?;
    let country_map = load_country_dim(country_dim_path)?;

    let input_file = File::open(events_path)?;
    let reader = BufReader::new(input_file);

    let mut dedup: HashMap<String, EventRecord> = HashMap::new();

    let mut raw_rows = 0_i64;
    let mut filtered_rows = 0_i64;

    for (idx, line_res) in reader.lines().enumerate() {
        let line = line_res?;
        if idx == 0 || line.trim().is_empty() {
            continue;
        }

        raw_rows += 1;
        let cols = split_csv_line(&line);
        if cols.len() < 14 {
            continue;
        }

        let event_id = cols[0].trim();
        if event_id.is_empty() {
            continue;
        }

        let event_version = parse_i64(cols[1]);
        let event_ts = cols[2].trim();
        let event_date = cols[3].trim();
        let customer_id = parse_i64(cols[4]);
        let product_id = parse_i64(cols[5]);
        let amount_cents = parse_i64(cols[6]);
        let quantity = parse_i64(cols[7]);
        let discount_bps = clamp_i64(parse_i64(cols[8]), 0, 5000);
        let shipping_cents = clamp_i64(parse_i64(cols[9]), 0, 25_000);
        let status = cols[10].trim().to_ascii_uppercase();
        let country = cols[11].trim().to_ascii_uppercase();

        let customer_tier_raw = cols[12].trim().to_ascii_lowercase();
        let customer_tier = match customer_tier_raw.as_str() {
            "bronze" | "silver" | "gold" | "platinum" => customer_tier_raw,
            _ => "unknown".to_string(),
        };

        if status != "COMPLETE" || amount_cents <= 0 || quantity <= 0 {
            continue;
        }
        if customer_id <= 0 || product_id <= 0 || event_date.is_empty() || event_ts.is_empty() {
            continue;
        }

        filtered_rows += 1;

        let candidate = EventRecord {
            event_version,
            event_ts: event_ts.to_string(),
            event_date: event_date.to_string(),
            customer_id,
            product_id,
            amount_cents,
            quantity,
            discount_bps,
            shipping_cents,
            country,
            customer_tier,
        };

        let should_replace = match dedup.get(event_id) {
            Some(current) => {
                candidate.event_version > current.event_version
                    || (candidate.event_version == current.event_version
                        && candidate.event_ts > current.event_ts)
            }
            None => true,
        };

        if should_replace {
            dedup.insert(event_id.to_string(), candidate);
        }
    }

    let mut customer_day_spend: HashMap<(String, i64), i64> = HashMap::new();
    let mut enriched_rows: Vec<DerivedRecord> = Vec::with_capacity(dedup.len());

    for record in dedup.values() {
        let product = product_map.get(&record.product_id).cloned().unwrap_or(ProductDim {
            category: "unknown".to_string(),
            margin_bps: 2500,
            weight_grams: 500,
        });

        let country_factor = country_map
            .get(&record.country)
            .cloned()
            .unwrap_or(CountryDim {
                fx_to_usd_ppm: 1_000_000,
                risk_bps: 10_000,
                tax_bps: 0,
            });

        let gross_local_cents = record.amount_cents * record.quantity + record.shipping_cents;
        let discount_local_cents = round_div(gross_local_cents * record.discount_bps, 10_000);
        let taxable_local_cents = std::cmp::max(gross_local_cents - discount_local_cents, 0);
        let tax_local_cents = round_div(taxable_local_cents * country_factor.tax_bps, 10_000);
        let net_local_cents = taxable_local_cents + tax_local_cents;

        let net_usd_cents = round_div(net_local_cents * country_factor.fx_to_usd_ppm, 1_000_000);
        let cost_usd_cents = round_div(net_usd_cents * (10_000 - product.margin_bps), 10_000);
        let profit_usd_cents = net_usd_cents - cost_usd_cents;
        let risk_adjusted_usd_cents = round_div(net_usd_cents * country_factor.risk_bps, 10_000);

        let hour = parse_event_hour(&record.event_ts);
        let time_bucket = time_bucket_from_hour(hour);
        let size_bucket = order_size_bucket(record.quantity);
        let heavy_item_order = if product.weight_grams * record.quantity >= 5_000 {
            1
        } else {
            0
        };

        let customer_day_key = (record.event_date.clone(), record.customer_id);
        *customer_day_spend.entry(customer_day_key).or_insert(0) += net_usd_cents;

        enriched_rows.push(DerivedRecord {
            event_date: record.event_date.clone(),
            customer_id: record.customer_id,
            customer_tier: record.customer_tier.clone(),
            category: product.category,
            country: record.country.clone(),
            time_bucket,
            order_size_bucket: size_bucket,
            quantity: record.quantity,
            net_usd_cents,
            profit_usd_cents,
            risk_adjusted_usd_cents,
            heavy_item_order,
        });
    }

    let mut aggregated: HashMap<(String, String, String, String, String, String), AggregateRecord> =
        HashMap::new();

    for row in &enriched_rows {
        let vip_customer_order = match customer_day_spend.get(&(row.event_date.clone(), row.customer_id)) {
            Some(total) if *total >= 50_000 => 1,
            _ => 0,
        };

        let key = (
            row.event_date.clone(),
            row.customer_tier.clone(),
            row.category.clone(),
            row.country.clone(),
            row.time_bucket.clone(),
            row.order_size_bucket.clone(),
        );

        let entry = aggregated.entry(key).or_default();
        entry.order_count += 1;
        entry.vip_customer_orders += vip_customer_order;
        entry.total_quantity += row.quantity;
        entry.total_net_usd_cents += row.net_usd_cents;
        entry.total_profit_usd_cents += row.profit_usd_cents;
        entry.total_risk_adjusted_usd_cents += row.risk_adjusted_usd_cents;
        entry.total_items += row.quantity;
        entry.heavy_item_orders += row.heavy_item_order;
    }

    let mut rows: Vec<_> = aggregated.into_iter().collect();
    rows.sort_by(|a, b| {
        a.0 .0
            .cmp(&b.0 .0)
            .then(a.0 .1.cmp(&b.0 .1))
            .then(a.0 .2.cmp(&b.0 .2))
            .then(a.0 .3.cmp(&b.0 .3))
            .then(a.0 .4.cmp(&b.0 .4))
            .then(a.0 .5.cmp(&b.0 .5))
    });

    let output_file = File::create(output_path)?;
    let mut writer = BufWriter::new(output_file);

    writeln!(
        writer,
        "event_date,customer_tier,category,country,time_bucket,order_size_bucket,order_count,vip_customer_orders,total_quantity,total_net_usd_cents,total_profit_usd_cents,total_risk_adjusted_usd_cents,avg_item_price_usd_cents,heavy_item_orders"
    )?;

    for ((event_date, customer_tier, category, country, time_bucket, order_size_bucket), agg) in rows {
        let avg_item_price_usd_cents = round_div(agg.total_net_usd_cents, agg.total_items);

        writeln!(
            writer,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            event_date,
            customer_tier,
            category,
            country,
            time_bucket,
            order_size_bucket,
            agg.order_count,
            agg.vip_customer_orders,
            agg.total_quantity,
            agg.total_net_usd_cents,
            agg.total_profit_usd_cents,
            agg.total_risk_adjusted_usd_cents,
            avg_item_price_usd_cents,
            agg.heavy_item_orders
        )?;
    }

    Ok((raw_rows, filtered_rows, dedup.len() as i64))
}

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 5 {
        eprintln!(
            "Usage: {} <events_csv> <product_dim_csv> <country_dim_csv> <output_csv>",
            args.get(0).map_or("process_rust", String::as_str)
        );
        std::process::exit(1);
    }

    let events_path = Path::new(&args[1]);
    let product_dim_path = Path::new(&args[2]);
    let country_dim_path = Path::new(&args[3]);
    let output_path = Path::new(&args[4]);

    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let (raw_rows, filtered_rows, dedup_rows) =
        transform(events_path, product_dim_path, country_dim_path, output_path)?;

    println!(
        "rust transform completed | raw_rows={} filtered_rows={} dedup_rows={} output={}",
        raw_rows,
        filtered_rows,
        dedup_rows,
        output_path.display()
    );

    Ok(())
}
