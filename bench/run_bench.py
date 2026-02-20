#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import statistics
import subprocess
import tempfile
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ETL benchmark for Python vs Rust")
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--python-bin", type=str, default="python3")
    return parser.parse_args()


def csv_rows(path: Path) -> int:
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        return sum(1 for _ in reader)


def _read_tail(path: Path, max_chars: int) -> str:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    return text[-max_chars:]


def timed_run(cmd: list[str], cwd: Path) -> dict[str, float | int | str | None]:
    started = time.perf_counter()
    system = platform.system().lower()

    with tempfile.NamedTemporaryFile(prefix="etl_bench_stdout_", delete=False) as out_tmp:
        stdout_path = Path(out_tmp.name)
    with tempfile.NamedTemporaryFile(prefix="etl_bench_stderr_", delete=False) as err_tmp:
        stderr_path = Path(err_tmp.name)

    return_code: int
    rusage = None

    try:
        with stdout_path.open("wb") as out_f, stderr_path.open("wb") as err_f:
            proc = subprocess.Popen(cmd, cwd=cwd, stdout=out_f, stderr=err_f)

            if hasattr(os, "wait4"):
                _, status, rusage = os.wait4(proc.pid, 0)
                return_code = os.waitstatus_to_exitcode(status)
            else:
                return_code = proc.wait()

        wall_sec = time.perf_counter() - started

        stdout_tail = _read_tail(stdout_path, 1000)
        stderr_tail = _read_tail(stderr_path, 1500)

        if return_code != 0:
            raise RuntimeError(
                "Command failed\n"
                f"cmd={' '.join(cmd)}\n"
                f"exit_code={return_code}\n"
                f"stdout={stdout_tail}\n"
                f"stderr={stderr_tail}"
            )

        user_sec = float(rusage.ru_utime) if rusage is not None else None
        sys_sec = float(rusage.ru_stime) if rusage is not None else None

        if rusage is not None:
            raw_rss = int(rusage.ru_maxrss)
            peak_rss_kb = int(raw_rss / 1024) if system == "darwin" else raw_rss
        else:
            peak_rss_kb = None

        cpu_util_pct = None
        if user_sec is not None and sys_sec is not None and wall_sec > 0:
            cpu_util_pct = ((user_sec + sys_sec) / wall_sec) * 100.0

        return {
            "cmd": " ".join(cmd),
            "wall_sec": wall_sec,
            "user_sec": user_sec,
            "sys_sec": sys_sec,
            "cpu_util_pct": cpu_util_pct,
            "peak_rss_kb": peak_rss_kb,
            "peak_rss_mb": (peak_rss_kb / 1024.0) if peak_rss_kb is not None else None,
        }
    finally:
        stdout_path.unlink(missing_ok=True)
        stderr_path.unlink(missing_ok=True)


def summarize(records: list[dict[str, float | int | str | None]]) -> dict[str, dict[str, float]]:
    by_variant: dict[str, list[dict[str, float | int | str | None]]] = defaultdict(list)
    for record in records:
        by_variant[str(record["variant"])].append(record)

    out: dict[str, dict[str, float]] = {}
    for variant, items in by_variant.items():
        wall = [float(item["wall_sec"]) for item in items]
        thr = [float(item["throughput_rows_per_sec"]) for item in items]

        cpu = [float(item["cpu_util_pct"]) for item in items if item["cpu_util_pct"] is not None]
        rss_mb = [float(item["peak_rss_mb"]) for item in items if item["peak_rss_mb"] is not None]

        out[variant] = {
            "runs": float(len(items)),
            "wall_mean_sec": statistics.mean(wall),
            "wall_median_sec": statistics.median(wall),
            "wall_min_sec": min(wall),
            "wall_max_sec": max(wall),
            "throughput_mean_rows_per_sec": statistics.mean(thr),
            "throughput_median_rows_per_sec": statistics.median(thr),
            "cpu_mean_pct": statistics.mean(cpu) if cpu else 0.0,
            "cpu_max_pct": max(cpu) if cpu else 0.0,
            "peak_rss_mean_mb": statistics.mean(rss_mb) if rss_mb else 0.0,
            "peak_rss_max_mb": max(rss_mb) if rss_mb else 0.0,
        }

    if "python" in out and "rust" in out:
        comparison: dict[str, float] = {}

        if out["rust"]["wall_mean_sec"] > 0:
            comparison["python_over_rust_speedup"] = (
                out["python"]["wall_mean_sec"] / out["rust"]["wall_mean_sec"]
            )

        if out["rust"]["cpu_mean_pct"] > 0:
            comparison["python_over_rust_cpu_ratio"] = (
                out["python"]["cpu_mean_pct"] / out["rust"]["cpu_mean_pct"]
            )

        if out["rust"]["peak_rss_mean_mb"] > 0:
            comparison["python_over_rust_ram_ratio"] = (
                out["python"]["peak_rss_mean_mb"] / out["rust"]["peak_rss_mean_mb"]
            )

        if comparison:
            out["comparison"] = comparison

    return out


def main() -> None:
    args = parse_args()
    if args.rows <= 0 or args.runs <= 0:
        raise ValueError("--rows and --runs must be > 0")

    root = Path(__file__).resolve().parents[1]

    events = root / "data/raw/events.csv"
    product_dim = root / "data/raw/dim_products.csv"
    country_dim = root / "data/raw/dim_countries.csv"
    py_out = root / "data/processed/python_output.csv"
    rs_out = root / "data/processed/rust_output.csv"

    results_dir = root / "bench/results"
    results_dir.mkdir(parents=True, exist_ok=True)

    python_cmd = [
        args.python_bin,
        "process_python/process.py",
        str(events),
        str(product_dim),
        str(country_dim),
        str(py_out),
    ]
    rust_bin = root / "process_rust/target/release/process_rust"
    rust_cmd = [
        str(rust_bin),
        str(events),
        str(product_dim),
        str(country_dim),
        str(rs_out),
    ]

    print("[1/6] Generating deterministic input data")
    timed_run(
        [
            args.python_bin,
            "pipeline/extract/generate_dummy_data.py",
            "--rows",
            str(args.rows),
            "--seed",
            str(args.seed),
            "--out-dir",
            "data/raw",
        ],
        cwd=root,
    )

    input_rows = csv_rows(events)
    print(f"Input rows: {input_rows}")

    print("[2/6] Pre-building Rust release binary (not included in benchmark)")
    timed_run(["cargo", "build", "--release", "--manifest-path", "process_rust/Cargo.toml"], cwd=root)
    if not rust_bin.exists():
        raise RuntimeError(f"Expected Rust binary not found: {rust_bin}")

    print("[3/6] Running timed transform benchmarks")
    records: list[dict[str, float | int | str | None]] = []

    for run_idx in range(1, args.runs + 1):
        order = ["python", "rust"] if run_idx % 2 == 1 else ["rust", "python"]

        for variant in order:
            if variant == "python":
                cmd = python_cmd
                out_path = py_out
            else:
                cmd = rust_cmd
                out_path = rs_out

            if out_path.exists():
                out_path.unlink()

            metrics = timed_run(cmd, cwd=root)
            output_rows = csv_rows(out_path)
            throughput = output_rows / float(metrics["wall_sec"]) if float(metrics["wall_sec"]) > 0 else 0.0

            record = {
                "run": run_idx,
                "variant": variant,
                "wall_sec": float(metrics["wall_sec"]),
                "user_sec": metrics["user_sec"],
                "sys_sec": metrics["sys_sec"],
                "cpu_util_pct": metrics["cpu_util_pct"],
                "peak_rss_kb": metrics["peak_rss_kb"],
                "peak_rss_mb": metrics["peak_rss_mb"],
                "output_rows": output_rows,
                "throughput_rows_per_sec": throughput,
            }
            records.append(record)

            cpu_text = f"{float(record['cpu_util_pct']):.1f}%" if record["cpu_util_pct"] is not None else "n/a"
            rss_text = f"{float(record['peak_rss_mb']):.2f}MB" if record["peak_rss_mb"] is not None else "n/a"

            print(
                f"run={run_idx} variant={variant} wall={record['wall_sec']:.4f}s "
                f"rows={output_rows} throughput={throughput:.2f} rows/s "
                f"cpu={cpu_text} peak_rss={rss_text}"
            )

    print("[4/6] Validating outputs")
    timed_run(
        [
            args.python_bin,
            "bench/validate_outputs.py",
            "--python-output",
            str(py_out),
            "--rust-output",
            str(rs_out),
        ],
        cwd=root,
    )

    print("[5/6] Loading outputs into SQLite")
    db_path = root / "data/load/warehouse.db"
    if db_path.exists():
        db_path.unlink()

    timed_run(
        [
            args.python_bin,
            "pipeline/load/load_sqlite.py",
            "--input",
            str(py_out),
            "--db",
            str(db_path),
            "--table",
            "fact_sales_python",
        ],
        cwd=root,
    )

    timed_run(
        [
            args.python_bin,
            "pipeline/load/load_sqlite.py",
            "--input",
            str(rs_out),
            "--db",
            str(db_path),
            "--table",
            "fact_sales_rust",
        ],
        cwd=root,
    )

    print("[6/6] Writing benchmark reports")
    summary = summarize(records)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    json_path = results_dir / f"bench_{ts}.json"
    csv_path = results_dir / f"bench_{ts}.csv"

    payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "rows": args.rows,
        "runs": args.runs,
        "seed": args.seed,
        "platform": platform.platform(),
        "records": records,
        "summary": summary,
    }

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "run",
            "variant",
            "wall_sec",
            "user_sec",
            "sys_sec",
            "cpu_util_pct",
            "peak_rss_kb",
            "peak_rss_mb",
            "output_rows",
            "throughput_rows_per_sec",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print("Benchmark complete")
    print(f"JSON report: {json_path}")
    print(f"CSV report:  {csv_path}")

    if "python" in summary and "rust" in summary:
        py = summary["python"]
        rs = summary["rust"]
        print(
            "python summary | "
            f"wall_mean={py['wall_mean_sec']:.4f}s cpu_mean={py['cpu_mean_pct']:.1f}% "
            f"peak_rss_mean={py['peak_rss_mean_mb']:.2f}MB"
        )
        print(
            "rust summary   | "
            f"wall_mean={rs['wall_mean_sec']:.4f}s cpu_mean={rs['cpu_mean_pct']:.1f}% "
            f"peak_rss_mean={rs['peak_rss_mean_mb']:.2f}MB"
        )

    if "comparison" in summary and "python_over_rust_speedup" in summary["comparison"]:
        speedup = summary["comparison"]["python_over_rust_speedup"]
        print(f"Mean speedup (Python/Rust): {speedup:.3f}x")


if __name__ == "__main__":
    main()
