"""
Stage 0 — Preprocessing of raw CSVs downloaded from BigQuery.

Transforms raw BigQuery exports into normalized tables for PostgreSQL:
  - events_daily.csv    → events_clean.csv  (fact table, no repo_name)
  - events_daily.csv    → repositories.csv  (merged with metadata)
  - repo_metadata.csv   → merged into repositories.csv

Usage:
  python scripts/preprocess.py [--data-dir data]
"""

import argparse
import glob
import os
import sys

import pandas as pd


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Preprocess raw BigQuery CSVs")
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Directory containing raw CSV files (default: data)",
    )
    return parser.parse_args()


def load_events(data_dir):
    """Load event CSVs into a single DataFrame.

    Supports a single events_daily.csv or multiple sharded files.
    """
    single_file = os.path.join(data_dir, "events_daily.csv")
    if os.path.isfile(single_file):
        print(f"  Loading {single_file}")
        return pd.read_csv(single_file, low_memory=False)

    pattern = os.path.join(data_dir, "events_daily*.csv")
    parts = sorted(glob.glob(pattern))
    if not parts:
        print(
            f"ERROR: No events CSV found in {data_dir}/. "
            "Expected events_daily.csv.",
            file=sys.stderr,
        )
        sys.exit(1)

    frames = []
    for path in parts:
        print(f"  Loading {path}")
        frames.append(pd.read_csv(path, low_memory=False))

    combined = pd.concat(frames, ignore_index=True)
    print(f"  Combined {len(parts)} files -> {len(combined):,} rows")
    return combined


def build_repositories(events_df, data_dir):
    """Build repositories dimension table.

    Extracts unique repos from events, merges with metadata for
    creation dates, and optionally with language info.
    """
    print("\n[2/3] Building repositories table...")

    repos = (
        events_df[["repo_id", "repo_name"]]
        .drop_duplicates(subset=["repo_id"])
        .sort_values("repo_id")
        .reset_index(drop=True)
    )
    print(f"  Found {len(repos):,} unique repositories in events")

    meta_path = os.path.join(data_dir, "repo_metadata.csv")
    if os.path.isfile(meta_path):
        meta = pd.read_csv(meta_path, low_memory=False)
        meta.columns = [c.strip().lower() for c in meta.columns]
        col_map = {"repo_first_seen": "first_seen_at"}
        meta.rename(columns=col_map, inplace=True)
        if "first_seen_at" in meta.columns:
            meta_slim = meta[["repo_id", "first_seen_at"]].drop_duplicates(
                subset=["repo_id"]
            )
            repos = repos.merge(meta_slim, on="repo_id", how="left")
            matched = repos["first_seen_at"].notna().sum()
            print(f"  Merged creation dates: {matched:,} repos matched")
    else:
        print(f"  WARNING: {meta_path} not found — first_seen_at will be NULL")

    if "first_seen_at" not in repos.columns:
        repos["first_seen_at"] = None

    lang_path = os.path.join(data_dir, "repo_languages.csv")
    if os.path.isfile(lang_path):
        langs = pd.read_csv(lang_path, low_memory=False)
        langs.columns = [c.strip().lower() for c in langs.columns]
        if "language_bytes" in langs.columns:
            langs = langs.drop(columns=["language_bytes"])
        langs = langs.drop_duplicates(subset=["repo_name"])
        repos = repos.merge(
            langs[["repo_name", "language"]], on="repo_name", how="left"
        )
        print(f"  Merged language info from {lang_path}")
    else:
        print(f"  INFO: {lang_path} not found — language will be NULL")
        repos["language"] = None

    expected_cols = ["repo_id", "repo_name", "first_seen_at", "language"]
    repos = repos[expected_cols]

    out_path = os.path.join(data_dir, "repositories.csv")
    repos.to_csv(out_path, index=False)
    print(f"  Saved {len(repos):,} repositories -> {out_path}")
    return repos


def build_events_clean(events_df, data_dir):
    """Clean events table: remove repo_name (it's in repositories dimension)."""
    print("\n[3/3] Cleaning events table...")

    keep_cols = [
        "event_type",
        "repo_id",
        "event_date",
        "event_count",
        "unique_actors",
    ]
    events_clean = events_df[keep_cols].copy()

    events_clean["repo_id"] = pd.to_numeric(
        events_clean["repo_id"], errors="coerce"
    )
    events_clean["event_count"] = pd.to_numeric(
        events_clean["event_count"], errors="coerce"
    )
    events_clean["unique_actors"] = pd.to_numeric(
        events_clean["unique_actors"], errors="coerce"
    )
    events_clean.dropna(subset=["repo_id"], inplace=True)
    events_clean["repo_id"] = events_clean["repo_id"].astype(int)
    events_clean["event_count"] = events_clean["event_count"].fillna(0).astype(int)
    events_clean["unique_actors"] = (
        events_clean["unique_actors"].fillna(0).astype(int)
    )

    events_clean = events_clean.groupby(
        ["event_type", "repo_id", "event_date"], as_index=False
    ).agg({"event_count": "sum", "unique_actors": "sum"})

    out_path = os.path.join(data_dir, "events_clean.csv")
    events_clean.to_csv(out_path, index=False)
    print(f"  Saved {len(events_clean):,} event rows -> {out_path}")

    total_events = events_clean["event_count"].sum()
    print(f"  Total individual events represented: {total_events:,}")
    return events_clean


def main():
    args = parse_args()
    data_dir = args.data_dir

    print("=" * 60)
    print("Stage 0 — CSV Preprocessing")
    print("=" * 60)

    print("\n[1/3] Loading raw events...")
    events_df = load_events(data_dir)
    print(f"  Total raw rows: {len(events_df):,}")

    build_repositories(events_df, data_dir)
    build_events_clean(events_df, data_dir)

    print("\n" + "=" * 60)
    print("Preprocessing complete! Output files in:", data_dir)
    print("  - repositories.csv  (dimension table)")
    print("  - events_clean.csv  (fact table)")
    print("=" * 60)


if __name__ == "__main__":
    main()
