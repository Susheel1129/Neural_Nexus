# src/standardize_cleaned.py
"""
Standardize the cleaned CSV(s) into the canonical 26-column layout.
Outputs:
 - data/staging/standardized_cleaned_all.csv
 - data/staging/standardized_cleaned_day0.csv / day1 / day2 (if detected_day exists)
 - reports/cleaning_issues.csv   (rows with missing keys or parse problems)
"""

import pandas as pd
from pathlib import Path
import numpy as np
import json
import re

BASE = Path.cwd()
STAGING = BASE / "data" / "staging"
REPORTS = BASE / "reports"
STAGING.mkdir(parents=True, exist_ok=True)
REPORTS.mkdir(parents=True, exist_ok=True)

infile = STAGING / "cleaned_all.csv"
if not infile.exists():
    raise SystemExit("Missing cleaned_all.csv — run src/cleaning.py first")

df = pd.read_csv(infile, dtype=str, low_memory=False)

# Normalize column names (safe)
def norm_col(c):
    if pd.isna(c):
        return c
    c = str(c).strip()
    c = re.sub(r'\s+', '_', c)
    c = re.sub(r'[^\w_]', '', c)
    return c.lower()

df.columns = [norm_col(c) for c in df.columns]

# Canonical columns (26) from your spec / image
canonical = [
    "customer_id",
    "customer_name",
    "customer_segment",
    "marital_status",
    "gender",
    "dob",
    "effective_start_dt",
    "effective_end_dt",
    "policy_type_id",
    "policy_type",
    "policy_type_desc",
    "policy_id",
    "policy_name",
    "premium_amt",
    "policy_term",
    "policy_start_dt",
    "policy_end_dt",
    "next_premium_dt",
    "actual_premium_paid_dt",
    "country",
    "region",
    "state_or_province",
    "city",
    "postal_code",
    "total_policy_amt",
    "premium_amt_paid_tilldate"
]

# Helper: pick first non-null from a list of possible columns
def coalesce(row, candidates):
    for c in candidates:
        if c in row and pd.notna(row[c]) and str(row[c]).strip() not in ("", "nan", "None"):
            return row[c]
    return None

# Mapping strategies: list variant column names that may exist in df -> canonical
candidates_map = {
    "customer_id": ["customer_id", "customer id", "id", "cust_id"],
    "customer_name": ["customer_name","customer name","customer_full_name","customer","customerfirstname","customer_first_name","customer_last_name","customer_last_name","customer_lastname"],
    "customer_segment": ["customer_segment","customer_segment"],
    "marital_status": ["marital_status","maritial_status","maritalstatus","marital_status"],
    "gender": ["gender","sex"],
    "dob": ["dob","date_of_birth","birth_date","data_of_birth"],
    "effective_start_dt": ["effective_start_dt","effective_start_date","effective_start"],
    "effective_end_dt": ["effective_end_dt","effective_end_date","effective_end"],
    "policy_type_id": ["policy_type_id","policy_typeid"],
    "policy_type": ["policy_type","policy type","policy_type_desc"],  # fallback
    "policy_type_desc": ["policy_type_desc","policy type desc","policytypedesc"],
    "policy_id": ["policy_id","policy id","policyid"],
    "policy_name": ["policy_name","policy name"],
    "premium_amt": ["premium_amt","premium_amt","premium amount","premium_amt_paid"],
    "policy_term": ["policy_term","policy term","term"],
    "policy_start_dt": ["policy_start_dt","policy_start_date","policy_start"],
    "policy_end_dt": ["policy_end_dt","policy_end_date","policy_end"],
    "next_premium_dt": ["next_premium_dt","next_premium_date","next_premium"],
    "actual_premium_paid_dt": ["actual_premium_paid_dt","actual_premium_paid_date","actual_premium_date","actual_premium_paid_dt"],
    "country": ["country"],
    "region": ["region"],
    "state_or_province": ["state_or_province","state or province","state","province"],
    "city": ["city"],
    "postal_code": ["postal_code","postal code","postalcode","zip","zipcode"],
    "total_policy_amt": ["total_policy_amt","total_policy_amt","total policy amount","total_policy_amount"],
    "premium_amt_paid_tilldate": ["premium_amt_paid_tilldate","premium_amt_paid_tilldate","premium_amt_paid_till_date","premium_amt_paid_tilldate"]
}

# Build standardized DataFrame
std = pd.DataFrame(index=df.index)

for canon in canonical:
    cands = candidates_map.get(canon, [canon])
    # also include exact match of normalized column names
    cands_norm = []
    for s in cands:
        # normalize each candidate into the same normalization used for df.columns
        sn = s
        sn = re.sub(r'\s+', '_', sn).lower()
        sn = re.sub(r'[^\w_]', '', sn)
        cands_norm.append(sn)
    # coalesce per-row
    std[canon] = df.apply(lambda r: coalesce(r, cands_norm), axis=1)

# Special handling: if customer_name missing but first/last exist, compose
if "customer_first_name" in df.columns or "customer_last_name" in df.columns:
    first = df.get("customer_first_name", pd.Series([None]*len(df)))
    last = df.get("customer_last_name", pd.Series([None]*len(df)))
    composed = (first.fillna("").astype(str).str.strip() + " " + last.fillna("").astype(str).str.strip()).str.strip().replace({"": None})
    # fill into customer_name where empty
    std["customer_name"] = std["customer_name"].fillna(composed)

# Trim strings
for c in std.columns:
    std[c] = std[c].astype(str).replace({"nan": None, "None": None, "None": None})
    std[c] = std[c].apply(lambda x: x.strip() if isinstance(x, str) else x)

# Parse dates to ISO strings for consistency (keep nulls)
date_cols = ["dob","effective_start_dt","effective_end_dt","policy_start_dt","policy_end_dt","next_premium_dt","actual_premium_paid_dt"]
for c in date_cols:
    if c in std.columns:
        std[c] = pd.to_datetime(std[c], errors="coerce").dt.date.astype(object)

# Convert numeric columns to numeric types
num_cols = ["premium_amt","total_policy_amt","premium_amt_paid_tilldate"]
for c in num_cols:
    if c in std.columns:
        std[c] = std[c].astype(str).str.replace(",","").str.replace("$","").str.strip()
        std[c] = pd.to_numeric(std[c].replace({"nan": None, "": None}), errors="coerce")

# Flag issues: missing natural keys
issues = pd.DataFrame(columns=list(std.columns) + ["issue"])
missing_cust = std[std["customer_id"].isnull()]
for idx, row in missing_cust.iterrows():
    r = row.to_dict()
    r["issue"] = "missing_customer_id"
    issues = issues.append(r, ignore_index=True)

missing_policy = std[std["policy_id"].isnull()]
for idx, row in missing_policy.iterrows():
    r = row.to_dict()
    r["issue"] = "missing_policy_id"
    issues = issues.append(r, ignore_index=True)

# Save standardized outputs
std_path = STAGING / "standardized_cleaned_all.csv"
std.to_csv(std_path, index=False)

# day-splits if detected_day exists in original df
if "detected_day" in df.columns:
    for d in sorted(df["detected_day"].dropna().unique(), key=lambda x: str(x)):
        mask = df["detected_day"] == d
        std.loc[mask].to_csv(STAGING / f"standardized_cleaned_day{d}.csv", index=False)

# Save issues
if not issues.empty:
    issues.to_csv(REPORTS / "cleaning_issues.csv", index=False)

# summary
summary = {
    "rows_in": int(df.shape[0]),
    "rows_out": int(std.shape[0]),
    "missing_customer_id": int(missing_cust.shape[0]),
    "missing_policy_id": int(missing_policy.shape[0]),
    "standardized_file": str(std_path),
    "issues_file": str(REPORTS / "cleaning_issues.csv") if not issues.empty else None
}
with open(REPORTS / "standardize_summary.json", "w") as fh:
    json.dump(summary, fh, indent=2, default=str)

print("STANDARDIZATION COMPLETE")
print("Rows in:", summary["rows_in"])
print("Rows out:", summary["rows_out"])
print("Missing customer_id:", summary["missing_customer_id"])
print("Missing policy_id:", summary["missing_policy_id"])
print("Standardized file written to:", summary["standardized_file"])
if summary["issues_file"]:
    print("Issues written to:", summary["issues_file"])
