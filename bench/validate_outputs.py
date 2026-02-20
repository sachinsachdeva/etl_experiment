#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
from pathlib import Path


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def row_count(path: Path) -> int:
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        return sum(1 for _ in reader)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Python and Rust outputs")
    parser.add_argument("--python-output", type=Path, required=True)
    parser.add_argument("--rust-output", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    py_count = row_count(args.python_output)
    rs_count = row_count(args.rust_output)
    py_hash = file_sha256(args.python_output)
    rs_hash = file_sha256(args.rust_output)

    print(
        "python_output",
        f"rows={py_count}",
        f"sha256={py_hash}",
        sep=" | ",
    )
    print(
        "rust_output",
        f"rows={rs_count}",
        f"sha256={rs_hash}",
        sep=" | ",
    )

    if py_count != rs_count:
        raise SystemExit("Validation failed: row counts differ")
    if py_hash != rs_hash:
        raise SystemExit("Validation failed: file hashes differ")

    print("Validation passed: outputs are identical")


if __name__ == "__main__":
    main()
