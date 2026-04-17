#!/usr/bin/env python3
"""
Quality report for features_prepared_plan.csv (one row per repository).

Usage:
  python inspect_features_prepared.py
  python inspect_features_prepared.py --path path/to/features_prepared_plan.csv
  python inspect_features_prepared.py --chunksize 500000
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect prepared features CSV.")
    parser.add_argument(
        "--path",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "data" / "features_prepared_plan.csv",
        help="Path to features CSV (default: <repo>/data/features_prepared_plan.csv)",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=300_000,
        help="Rows per chunk when streaming the file (default: 300000)",
    )
    args = parser.parse_args()
    path: Path = args.path

    if not path.is_file():
        print(f"ERROR: file not found: {path}", file=sys.stderr)
        return 1

    print("=" * 72)
    print("FEATURES CSV QUALITY REPORT")
    print("=" * 72)
    print(f"File: {path.resolve()}")
    print(f"Size on disk: {path.stat().st_size / (1024 ** 2):.1f} MB")
    print(f"Chunk size: {args.chunksize:,}")
    print()

    total_rows = 0
    null_sum = None
    numeric_cols: list[str] = []
    zero_sum: dict[str, int] = {}
    min_val: dict[str, float] = {}
    max_val: dict[str, float] = {}
    inf_sum: dict[str, int] = {}
    neg_sum: dict[str, int] = {}

    try:
        reader = pd.read_csv(path, chunksize=args.chunksize)
        for i, chunk in enumerate(reader, start=1):
            total_rows += len(chunk)
            if null_sum is None:
                null_sum = chunk.isnull().sum()
                num = chunk.select_dtypes(include=[np.number])
                numeric_cols = list(num.columns)
                for c in numeric_cols:
                    zero_sum[c] = int((num[c] == 0).sum())
                    min_val[c] = float(num[c].min())
                    max_val[c] = float(num[c].max())
                    inf_sum[c] = int(np.isinf(num[c].to_numpy()).sum())
                    neg_sum[c] = int((num[c] < 0).sum())
            else:
                null_sum = null_sum.add(chunk.isnull().sum(), fill_value=0)
                num = chunk.select_dtypes(include=[np.number])
                for c in numeric_cols:
                    if c not in num.columns:
                        continue
                    s = num[c]
                    zero_sum[c] = zero_sum.get(c, 0) + int((s == 0).sum())
                    min_val[c] = min(min_val[c], float(s.min()))
                    max_val[c] = max(max_val[c], float(s.max()))
                    inf_sum[c] = inf_sum.get(c, 0) + int(np.isinf(s.to_numpy()).sum())
                    neg_sum[c] = neg_sum.get(c, 0) + int((s < 0).sum())

            if i % 5 == 0:
                print(f"  ... processed {total_rows:,} rows", flush=True)
    except pd.errors.EmptyDataError:
        print("ERROR: CSV is empty.", file=sys.stderr)
        return 1

    print(f"Total rows: {total_rows:,}")
    print()

    # repo_id duplicates (single column load — small vs full frame)
    if total_rows == 0:
        print("No rows to analyze.")
        return 0

    print("-" * 72)
    print("repo_id uniqueness")
    print("-" * 72)
    rid = pd.read_csv(path, usecols=["repo_id"])
    dup_rows = int(rid["repo_id"].duplicated().sum())
    nunique = rid["repo_id"].nunique()
    print(f"Unique repo_id: {nunique:,}")
    print(f"Duplicate repo_id rows (should be 0): {dup_rows:,}")
    print()

    print("-" * 72)
    print("Missing values (all columns)")
    print("-" * 72)
    null_sum = null_sum.sort_values(ascending=False)
    for col, cnt in null_sum.items():
        if cnt > 0:
            pct = 100.0 * cnt / total_rows
            print(f"  {col}: {int(cnt):,} ({pct:.4f}%)")
    if (null_sum == 0).all():
        print("  (none)")
    print()

    print("-" * 72)
    print("Numeric columns: zeros, negatives, infinities, min, max")
    print("-" * 72)
    # repo_id is numeric but not a feature — skip zero stats for it
    stat_cols = [c for c in numeric_cols if c != "repo_id"]
    print(f"{'column':<28} {'zeros%':>10} {'neg%':>8} {'inf':>6} {'min':>14} {'max':>14}")
    for c in stat_cols:
        zpct = 100.0 * zero_sum[c] / total_rows
        npct = 100.0 * neg_sum.get(c, 0) / total_rows
        print(
            f"{c:<28} {zpct:>9.2f}% {npct:>7.4f}% {inf_sum.get(c, 0):>6d} "
            f"{min_val[c]:>14.4g} {max_val[c]:>14.4g}"
        )
    print()

    # Quick dtype peek from first chunk
    sample = pd.read_csv(path, nrows=5_000)
    print("-" * 72)
    print("dtypes (from first 5000 rows)")
    print("-" * 72)
    print(sample.dtypes.to_string())
    print()

    print("=" * 72)
    print("Done.")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
