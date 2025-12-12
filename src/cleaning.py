"""
src/cleaning.py  -- robust version

What this adds vs earlier:
 - merges duplicate column names that result from normalization
 - avoids deprecated pandas args
 - safely handles series vs dataframe cases when cleaning text
 - same outputs as before: data/staging/cleaned_all.csv, per-day splits, reports/cleaning_summary.json
"""

import pandas as pd
import numpy as np
from pathlib import Path
import hashlib
import json
import re
import datetime

BASE = Path.cwd()
STAGING = BASE / "data" / "staging"
REPORTS = BASE / "reports"
STAGING.mkdir(parents=True, exist_ok=True)
REPORTS.mkdir(parents=True, exist_ok=True)

infile = STAGING / "raw_combined_all.csv"
if not infile.exists():
    raise SystemExit(f"Missing {infile}. Run src/ingest.py first.")

def norm_col(c):
    c = str(c).strip()
    c = re.sub(r"\s+", "_", c)
    c = re.sub(r"[^\w_]", "", c)
    return c.lower()

def compute_md5(values_list):
    s = "||".join([str(v) for v in values_list])
    return hashlib.md5(s.encode()).hexdigest()

# ---------------------------
# Load and normalize headers
# ---------------------------
raw = pd.read_csv(infile, dtype=str, low_memory=False)

# create a mapping from original column index -> normalized name
orig_cols = list(raw.columns)
normed = [norm_col(c) for c in orig_cols]

# if there are duplicate normalized names, coalesce columns with same normalized name
dup_map = {}
for idx, nc in enumerate(normed):
    dup_map.setdefault(nc, []).append(orig_cols[idx])

# Build a new DataFrame with unique normalized columns by coalescing duplicate original columns
cleaned_cols = {}
for norm_name, original_list in dup_map.items():
    if len(original_list) == 1:
        # single original column -> just use as-is
        cleaned_cols[norm_name] = raw[original_list[0]]
    else:
        # multiple original columns normalized to same name -> coalesce (first non-null)
        series_list = [raw[c].astype(str).replace({"nan":"", "None":""}) for c in original_list]
        # create series with first non-empty value per row
        def first_non_empty(row_vals):
            for v in row_vals:
                if v is None:
                    continue
                s = str(v).strip()
                if s != "" and s.lower() not in ("nan","none"):
                    return s
            return None
        # apply row-wise
        stacked = pd.concat(series_list, axis=1)
        coalesced = stacked.apply(lambda r: first_non_empty(list(r)), axis=1)
        cleaned_cols[norm_name] = coalesced

# build dataframe with normalized unique column names
df = pd.DataFrame(cleaned_cols)
input_rows = df.shape[0]

# ---------------------------
# Parse date columns (robust)
# ---------------------------
possible_date_cols = [
    "policy_start_dt","policy_end_dt","payment_date","next_premium_dt",
    "actual_premium_paid_dt","dob","effective_start_dt","effective_end_dt"
]
found_date_cols = [c for c in possible_date_cols if c in df.columns]

for col in found_date_cols:
    # use pandas default strict parsing; coerce errors -> NaT
    df[col] = pd.to_datetime(df[col], errors="coerce")

# ---------------------------
# Numeric conversions
# ---------------------------
num_cols = ["premium_amt","total_policy_amt","premium_amt_paid_tilldate","days_delay"]

for col in num_cols:
    if col in df.columns:
        # remove common noise like commas, currency symbols, spaces
        s = df[col].astype(str).fillna("").str.replace(",", "", regex=False).str.replace("$", "", regex=False).str.strip()
        # empty strings -> NaN
        s = s.replace({"": np.nan, "nan": np.nan, "None": np.nan})
        df[col] = pd.to_numeric(s, errors="coerce")

# ---------------------------
# Text cleaning (safe)
# ---------------------------
text_cols = [
    "country","region","state","city","policy_type",
    "marital_status","customer_name","customer_first_name","customer_last_name","postal_code"
]

for col in text_cols:
    if col in df.columns:
        s = df[col]
        # if accessing returns a DataFrame (shouldn't after coalescing) handle it
        if isinstance(s, pd.DataFrame):
            # coalesce the dataframe into single series of first non-null string
            s = s.fillna("").astype(str).apply(lambda row: next((str(x).strip() for x in row if str(x).strip() not in ("","nan","None")), None), axis=1)
        else:
            s = s.astype(str).str.strip().replace({"nan": None, "None": None, "": None})
        # title-case where appropriate
        df[col] = s.apply(lambda x: x.title() if isinstance(x, str) else x)

# ---------------------------
# Age at policy start
# ---------------------------
if "dob" in df.columns and "policy_start_dt" in df.columns:
    def compute_age(r):
        d1 = r["dob"]
        d2 = r["policy_start_dt"]
        if pd.isnull(d1) or pd.isnull(d2):
            return np.nan
        try:
            return int((d2 - d1).days // 365)
        except:
            return np.nan
    df["age_at_policy_start"] = df.apply(compute_age, axis=1)
else:
    df["age_at_policy_start"] = np.nan

# ---------------------------
# days_delay calculation
# ---------------------------
if "next_premium_dt" in df.columns:
    if "actual_premium_paid_dt" in df.columns:
        df["days_delay"] = (df["actual_premium_paid_dt"] - df["next_premium_dt"]).dt.days
    elif "payment_date" in df.columns:
        df["days_delay"] = (df["payment_date"] - df["next_premium_dt"]).dt.days
    else:
        df["days_delay"] = np.nan
else:
    if "days_delay" not in df.columns:
        df["days_delay"] = np.nan

# ---------------------------
# row_hash
# ---------------------------
priority_cols = [
    "customer_id","policy_id","policy_type","payment_date",
    "premium_amt","region","next_premium_dt","actual_premium_paid_dt"
]
hash_cols = [c for c in priority_cols if c in df.columns]
if not hash_cols:
    hash_cols = list(df.columns)

df["row_hash"] = df[hash_cols].fillna("").astype(str).agg("||".join, axis=1).apply(lambda s: hashlib.md5(s.encode()).hexdigest())

# ---------------------------
# Drop exact duplicates
# ---------------------------
before = df.shape[0]
df = df.drop_duplicates()
after = df.shape[0]
duplicates_dropped = before - after

# ---------------------------
# Write outputs
# ---------------------------
cleaned_all_path = STAGING / "cleaned_all.csv"
df.to_csv(cleaned_all_path, index=False)

if "detected_day" in df.columns:
    for day in sorted(df["detected_day"].dropna().unique(), key=str):
        subset = df[df["detected_day"] == day]
        out = STAGING / f"cleaned_day{day}.csv"
        subset.to_csv(out, index=False)

summary = {
    "input_rows": int(input_rows),
    "cleaned_rows": int(df.shape[0]),
    "duplicates_dropped": int(duplicates_dropped),
    "parsed_date_columns": found_date_cols,
    "hash_columns": hash_cols,
    "cleaned_file": str(cleaned_all_path)
}

with open(REPORTS / "cleaning_summary.json", "w") as fh:
    json.dump(summary, fh, indent=2, default=str)

print("CLEANING COMPLETE")
print(" -> Input rows:", summary["input_rows"])
print(" -> Cleaned rows:", summary["cleaned_rows"])
print(" -> Duplicates dropped:", summary["duplicates_dropped"])
print(" -> Parsed date columns:", summary["parsed_date_columns"])
print("Cleaned file saved to:", cleaned_all_path)
