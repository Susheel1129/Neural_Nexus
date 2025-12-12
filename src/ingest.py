"""
src/ingest.py
Combine all raw CSVs into canonical staging files:
 - data/staging/raw_combined_all.csv  (all rows)
 - data/staging/raw_combined_day0.csv (day0 rows)
 - data/staging/raw_combined_day1.csv (day1 rows)
 - data/staging/raw_combined_day2.csv (day2 rows)

Behavior:
 - Detect region from parent folder name: Insurance_details_US_<Region>_day
 - Try to detect day index from filename (day0, day1, day2) or from folder name
 - Add columns: source_file, region, batch_date (attempt)
 - Skip files that fail to parse and report them
"""

import os
import glob
import pandas as pd
from pathlib import Path
import re

BASE = Path.cwd()
RAW_DIR = BASE / "data" / "raw"
STAGING = BASE / "data" / "staging"
STAGING.mkdir(parents=True, exist_ok=True)

# patterns
day_pattern = re.compile(r"day\s*0|day0|_0|0\.csv", re.IGNORECASE)
day_index_pattern = re.compile(r"day[ _-]?(\d+)", re.IGNORECASE)
date_like_pattern = re.compile(r"(\d{4}[-_]\d{2}[-_]\d{2})")  # optional

all_rows = []
failed = []
summary = {}

print(f"Scanning raw directory: {RAW_DIR}")

csv_paths = list(RAW_DIR.rglob("*.csv"))
if not csv_paths:
    print("No CSV files found under data/raw. Please check extraction.")
    raise SystemExit(1)

for p in csv_paths:
    try:
        df = pd.read_csv(p, dtype=str)  # read everything as string initially
    except Exception as e:
        failed.append((str(p), str(e)))
        continue

    # region = parent folder name, try to infer
    region = p.parent.name
    # try to pull region name like Insurance_details_US_East_day -> East
    m = re.search(r"Insurance_details_US_([A-Za-z]+)_day", region, re.IGNORECASE)
    if m:
        region_name = m.group(1)
    else:
        # fallback to parent folder last token
        region_name = region

    fname = p.name
    # detect day index from filename first, else from parent folder name
    day_idx = None
    m = day_index_pattern.search(fname)
    if m:
        day_idx = int(m.group(1))
    else:
        m2 = day_index_pattern.search(str(p.parent))
        if m2:
            day_idx = int(m2.group(1))

    # add batch_date if any date pattern in filename
    batch_date = None
    md = date_like_pattern.search(fname)
    if md:
        batch_date = md.group(1).replace("_", "-")

    # add metadata columns
    df["source_file"] = str(p.relative_to(BASE))
    df["region"] = region_name
    df["detected_day"] = day_idx if day_idx is not None else "unknown"
    df["batch_date"] = batch_date

    all_rows.append(df)

# concat all into one
if not all_rows:
    print("No readable CSVs found.")
    raise SystemExit(1)

combined = pd.concat(all_rows, ignore_index=True, sort=False)

# save overall combined
out_all = STAGING / "raw_combined_all.csv"
combined.to_csv(out_all, index=False)
print(f"Wrote combined file: {out_all} ({combined.shape[0]} rows, {combined.shape[1]} cols)")

# split by detected_day if present into day0/day1/day2 files
for day in [0,1,2]:
    sub = combined[combined["detected_day"] == day]
    out = STAGING / f"raw_combined_day{day}.csv"
    if not sub.empty:
        sub.to_csv(out, index=False)
        print(f"Wrote {out} ({sub.shape[0]} rows)")
    else:
        print(f"No rows detected for day{day} — skipping file.")

# also save a simple JSON summary
summary = {
    "total_raw_files_scanned": len(csv_paths),
    "total_rows": int(combined.shape[0]),
    "columns": combined.columns.tolist(),
    "failed_files": [f[0] for f in failed]
}
import json
with open(STAGING / "ingest_summary.json", "w") as fh:
    json.dump(summary, fh, indent=2)

print("Ingest summary written to data/staging/ingest_summary.json")
if failed:
    print("FAILED FILES:")
    for ff, err in failed:
        print(f" - {ff} : {err}")

print("Ingest step complete.")
