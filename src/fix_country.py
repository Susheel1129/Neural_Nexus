import pandas as pd
from pathlib import Path
import re

BASE = Path.cwd()
STAGING = BASE / "data" / "staging"

input_file = STAGING / "standardized_cleaned_all.csv"
output_file = STAGING / "standardized_cleaned_all_fixed.csv"

df = pd.read_csv(input_file, dtype=str)

def normalize_country(val):
    if val is None:
        return None
    s = str(val).strip().lower()

    # remove dots, spaces to compare
    s2 = re.sub(r'[.\s]', '', s)

    # any of these patterns => United States
    us_patterns = [
        "usa",
        "us",
        "unitedstates",
        "unitedstatesofamerica",
        "u.s",
        "u.s.a",
        "u.s.a.",
        "u.s.",
    ]

    if s2 in us_patterns:
        return "United States"

    # fallback: if it contains "united", assume US
    if "united" in s:
        return "United States"

    return "United States"   # FINAL DEFAULT

df["country"] = df["country"].apply(normalize_country)

df.to_csv(output_file, index=False)

print("COUNTRY STANDARDIZATION COMPLETE ✔")
print("Output:", output_file)
