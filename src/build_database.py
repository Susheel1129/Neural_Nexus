"""
Build a small SQLite warehouse from the standardized CSV output.

Tables
- dim_date: calendar attributes keyed by date_id (YYYYMMDD int)
- dim_address: unique country/region/state/city/postal_code combinations
- dim_customer: customers with FK to address and DOB date_id
- dim_policy: policy master data
- fact_policy_payments: measures per customer-policy record with date FKs

Input
- data/staging/standardized_cleaned_all_fixed.csv (preferred)
- fallback: data/staging/standardized_cleaned_all.csv

Output
- data/warehouse/insurance.db

Usage
python src/build_database.py
"""

import sqlite3
from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path.cwd()
STAGING = BASE / "data" / "staging"
WAREHOUSE = BASE / "data" / "warehouse"
WAREHOUSE.mkdir(parents=True, exist_ok=True)

PREFERRED_INPUT = STAGING / "standardized_cleaned_all_fixed.csv"
FALLBACK_INPUT = STAGING / "standardized_cleaned_all.csv"


def load_source() -> pd.DataFrame:
    path = PREFERRED_INPUT if PREFERRED_INPUT.exists() else FALLBACK_INPUT
    if not path.exists():
        raise SystemExit("Missing standardized CSV. Run the ETL first.")
    df = pd.read_csv(path, dtype=str, low_memory=False)
    # Normalize expected date columns to datetime
    date_cols = [
        "dob",
        "effective_start_dt",
        "effective_end_dt",
        "policy_start_dt",
        "policy_end_dt",
        "next_premium_dt",
        "actual_premium_paid_dt",
    ]
    for c in date_cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    # Numeric cleanup
    num_cols = ["premium_amt", "total_policy_amt", "premium_amt_paid_tilldate"]
    for c in num_cols:
        if c in df.columns:
            df[c] = (
                df[c]
                .astype(str)
                .str.replace(",", "", regex=False)
                .str.replace("$", "", regex=False)
                .str.strip()
                .replace({"": np.nan, "nan": np.nan, "None": np.nan})
            )
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def build_dim_date(df: pd.DataFrame, conn: sqlite3.Connection) -> pd.DataFrame:
    """Create dim_date from all date-like columns."""
    date_cols = [
        "dob",
        "effective_start_dt",
        "effective_end_dt",
        "policy_start_dt",
        "policy_end_dt",
        "next_premium_dt",
        "actual_premium_paid_dt",
    ]
    all_dates = pd.Series(dtype="datetime64[ns]")
    for c in date_cols:
        if c in df.columns:
            all_dates = pd.concat([all_dates, df[c]])
    all_dates = all_dates.dropna().drop_duplicates().sort_values()
    if all_dates.empty:
        dim_date = pd.DataFrame(columns=["date_id", "full_date"])
    else:
        dim_date = pd.DataFrame({"full_date": all_dates})
        dim_date["date_id"] = dim_date["full_date"].dt.strftime("%Y%m%d").astype(int)
        dim_date["year"] = dim_date["full_date"].dt.year
        dim_date["quarter"] = dim_date["full_date"].dt.quarter
        dim_date["month"] = dim_date["full_date"].dt.month
        dim_date["day"] = dim_date["full_date"].dt.day
        dim_date["day_name"] = dim_date["full_date"].dt.day_name()
        dim_date["weekofyear"] = dim_date["full_date"].dt.isocalendar().week.astype(int)
    dim_date.to_sql("dim_date", conn, if_exists="replace", index=False)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_date_id ON dim_date(date_id);")
    return dim_date


def build_dim_address(df: pd.DataFrame, conn: sqlite3.Connection) -> pd.DataFrame:
    addr_cols = ["country", "region", "state_or_province", "city", "postal_code"]
    present = [c for c in addr_cols if c in df.columns]
    addr = df[present].fillna("").drop_duplicates().reset_index(drop=True)
    addr.insert(0, "address_id", addr.index + 1)
    addr.to_sql("dim_address", conn, if_exists="replace", index=False)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_address ON dim_address(country, region, state_or_province, city, postal_code);")
    return addr


def build_dim_customer(df: pd.DataFrame, dim_date: pd.DataFrame, dim_addr: pd.DataFrame, conn: sqlite3.Connection) -> pd.DataFrame:
    needed = [
        "customer_id",
        "customer_name",
        "customer_segment",
        "marital_status",
        "gender",
        "dob",
        "country",
        "region",
        "state_or_province",
        "city",
        "postal_code",
    ]
    present = [c for c in needed if c in df.columns]
    cust = df[present].drop_duplicates(subset=["customer_id"]).copy()
    # Map dob to date_id
    if "dob" in cust.columns and not dim_date.empty:
        date_map = dict(zip(dim_date["full_date"], dim_date["date_id"]))
        cust["dob_id"] = cust["dob"].map(date_map)
    else:
        cust["dob_id"] = np.nan
    # Map address
    if not dim_addr.empty:
        cust = cust.merge(
            dim_addr,
            how="left",
            on=[c for c in ["country", "region", "state_or_province", "city", "postal_code"] if c in dim_addr.columns],
        )
    cust.rename(columns={"customer_id": "customer_key"}, inplace=True)
    dim_cust_cols = [
        "customer_key",
        "customer_name",
        "customer_segment",
        "marital_status",
        "gender",
        "dob_id",
        "address_id",
    ]
    dim_customer = cust[dim_cust_cols].drop_duplicates()
    dim_customer.to_sql("dim_customer", conn, if_exists="replace", index=False)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_customer_key ON dim_customer(customer_key);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dim_customer_addr ON dim_customer(address_id);")
    return dim_customer


def build_dim_policy(df: pd.DataFrame, conn: sqlite3.Connection) -> pd.DataFrame:
    cols = [
        "policy_id",
        "policy_name",
        "policy_type_id",
        "policy_type",
        "policy_type_desc",
        "policy_term",
    ]
    present = [c for c in cols if c in df.columns]
    dim_policy = df[present].drop_duplicates(subset=["policy_id"]).copy()
    dim_policy.to_sql("dim_policy", conn, if_exists="replace", index=False)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_policy_id ON dim_policy(policy_id);")
    return dim_policy


def build_fact(df: pd.DataFrame, dim_date: pd.DataFrame, dim_addr: pd.DataFrame, conn: sqlite3.Connection) -> pd.DataFrame:
    # Date map
    date_map = dict(zip(dim_date["full_date"], dim_date["date_id"]))
    def map_date(col: str):
        if col not in df.columns:
            return pd.Series([np.nan] * len(df))
        return df[col].map(date_map)

    fact = pd.DataFrame()
    fact["customer_key"] = df["customer_id"]
    fact["policy_id"] = df["policy_id"]
    fact["effective_start_date_id"] = map_date("effective_start_dt")
    fact["effective_end_date_id"] = map_date("effective_end_dt")
    fact["policy_start_date_id"] = map_date("policy_start_dt")
    fact["policy_end_date_id"] = map_date("policy_end_dt")
    fact["next_premium_date_id"] = map_date("next_premium_dt")
    fact["actual_premium_paid_date_id"] = map_date("actual_premium_paid_dt")

    measures = ["premium_amt", "total_policy_amt", "premium_amt_paid_tilldate"]
    for m in measures:
        if m in df.columns:
            fact[m] = df[m]
    # Compute days_delay if possible
    if "next_premium_dt" in df.columns and "actual_premium_paid_dt" in df.columns:
        fact["days_delay"] = (df["actual_premium_paid_dt"] - df["next_premium_dt"]).dt.days
    # Late fee toy calc: 2.5% of premium_amt per 30 days delay (example)
    if "premium_amt" in fact.columns and "days_delay" in fact.columns:
        fact["late_fee_est"] = fact["premium_amt"] * 0.025 * (fact["days_delay"].clip(lower=0) / 30).fillna(0)

    # Attach address via region/state when possible
    if not dim_addr.empty:
        addr_keys = ["country", "region", "state_or_province", "city", "postal_code"]
        present = [c for c in addr_keys if c in df.columns and c in dim_addr.columns]
        if present:
            merged = df[present].copy()
            merged["__rowid"] = range(len(df))
            tmp = merged.merge(dim_addr, how="left", on=present)
            fact = fact.join(tmp.set_index("__rowid")["address_id"])

    fact.to_sql("fact_policy_payments", conn, if_exists="replace", index=False)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fact_customer ON fact_policy_payments(customer_key);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fact_policy ON fact_policy_payments(policy_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fact_dates ON fact_policy_payments(next_premium_date_id, actual_premium_paid_date_id);")
    return fact


def main():
    df = load_source()
    conn = sqlite3.connect(WAREHOUSE / "insurance.db")
    try:
        dim_date = build_dim_date(df, conn)
        dim_addr = build_dim_address(df, conn)
        dim_cust = build_dim_customer(df, dim_date, dim_addr, conn)
        dim_policy = build_dim_policy(df, conn)
        fact = build_fact(df, dim_date, dim_addr, conn)
        conn.commit()
        print("Database built at data/warehouse/insurance.db")
        print(f"dim_date rows: {len(dim_date)}, dim_address rows: {len(dim_addr)}, dim_customer rows: {len(dim_cust)}, dim_policy rows: {len(dim_policy)}, fact rows: {len(fact)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

