#  INSURANCE POLICY DATA ENGINEERING PIPELINE
# HCL TECH HACKATHON 
## Team: Neural Nexus

### Team Members
Kannam Susheel Kumar (22CS01058)  
Bapathu Deekshitha (22ME01036)  
Meena Kalyani (22ME01051)  
Kanuri Venkata Ramana (22CS01035)

## Hackathon Use Case 
<img width="905" height="1280" alt="image" src="https://github.com/user-attachments/assets/40668ffb-1a13-45ab-8aab-326450c5769a" />
<img width="912" height="1280" alt="image" src="https://github.com/user-attachments/assets/9ffc9a8c-4185-4083-88af-42f9a077cacb" />
<img width="889" height="1280" alt="image" src="https://github.com/user-attachments/assets/8bcc90a2-c310-492f-8b23-d8fd57606c74" />
<img width="891" height="1280" alt="image" src="https://github.com/user-attachments/assets/ce5dbab7-4268-4aec-8838-58bbe23a64a2" />


Building a complete end-to-end ETL + Data Warehouse solution built for multi-region, multi-day insurance policy data.
This project demonstrates real-world data engineering best practices including:

Daily batch ingestion

Standardized cleaning

Consolidation into a master dataset

Star Schema Data Warehouse

Business rule processing (late fees)

Automated analytical reporting

##  Project Architecture

Raw data arrives from 4 U.S. regions (East, West, South, Central) for day0, day1, and day2.

Instead of merging everything at once, this pipeline follows day-wise ingestion, producing clean daily datasets first and then merging them globally.

This simulates how enterprise insurers load data into warehouses incrementally.

## 1️⃣ ingest.py — Raw Data Ingestion (Day-wise Merge Across Regions)

Each day has 4 regional CSVs:

Example (Day0):

US_East/day0.csv
US_West/day0.csv
US_South/day0.csv
US_Central/day0.csv

✔ What ingest.py does:
Step A — Combine all regions for each day

For each day (day0, day1, day2), the script:

Reads all region files

Extracts region name from folder

Adds source_file column

Performs light cleaning (trimming whitespace)

Merges all regions into a single dataset for that day

Outputs:

staging/cleaned_day0.csv
staging/cleaned_day1.csv
staging/cleaned_day2.csv

Step B — Merge all cleaned-day files
staging/raw_combined.csv


This becomes the single source of truth for the cleaning phase.

Command
python src/ingest.py

## 2️⃣ cleaning.py — Deep Cleaning & Standardization (DD-MM-YYYY Format)

This script performs full cleaning on the combined dataset (raw_combined.csv) to create a fully standardized dataset for DW loading.

Below are the detailed cleaning operations.

✔ Standardizing Column Names

Convert to lowercase

Replace spaces with underscores

Trim whitespace

Fix duplicate column names (region, region_1, etc.)

Ensures all files share a consistent schema.

✔ Normalizing Gender

Raw inputs:

m, M, male, MALE → Male
f, F, female → Female


Final values:

Male
Female

✔ Normalizing Marital Status

Examples:

Raw	Cleaned
single	Single
SINGLE	Single
married	Married
divorced	Divorced

Uses .str.title() to ensure consistency.

✔ Standardizing Country Name → ALWAYS “USA”

Variations such as:

United States, United States of America, US, U.S.A, America


All become:

USA

✔ Combining Customer Name Components

Constructs a clean full name:

Customer Name = First + Middle + Last


Handles missing middle names & fallback full name columns.

Examples:

John A Smith
Mary Johnson

✔ Cleaning Region Fields

Many files contain both Region and region.

Script merges all variants and standardizes:

east → East  
WEST → West  
south → South  
central → Central

✔ Converting All Date Columns → DD-MM-YYYY

All date fields are parsed with error handling and reformatted:

05-01-2012
31-12-2014
10-08-1990


Invalid or missing dates are set to:

NULL

✔ Cleaning Numeric Columns

Fixes formats like:

"10,000", "$800", " 4500 "


into:

10000.00
800.00
4500.00

✔ Removing Duplicate Rows

Ensures no repeated records after merging day0/day1/day2 data.

✔ Handling Empty or Invalid Values

Empty values like:

'', '   ', 'nan', 'None'


are replaced with:

NULL


Allowing clean loading into MySQL.

Output
staging/cleaned.csv
reports/cleaning_summary.json

Command
python src/cleaning.py

## 3️⃣ dw_loader.py — Build & Populate the Data Warehouse

This script creates a Star Schema and loads all data into MySQL.
![WhatsApp Image 2025-12-12 at 20 08 26](https://github.com/user-attachments/assets/1a6864c2-3147-4683-b459-9059c80ae885)



✔ Dimension Tables

dim_customer

dim_policy

dim_location

dim_date

✔ Fact Table

fact_premium_payments

✔ What happens:

Unique values inserted into dimensions

Surrogate keys generated

Fact table populated with:

Customer Key

Policy Key

Location Key

Payment Date Key

Premium Amounts

Late Days

Command
python src/dw_loader.py

## 4️⃣ load_late_fee_rules.py — Load Excel Late-Fee Rules

Imports:

UseCase - Late_Fees_Calculation_Formula.xlsx


into the DW:

late_fee_rules


Rules include:

Flat fees

Percentage fees

Region-based rules

Month-based rules

Late-day slabs

Command
python src/load_late_fee_rules.py

## 5️⃣ compute_late_fees.py — Late Fee Calculation Engine

This script computes late fees by joining:

fact_premium_payments
late_fee_rules

✔ Computes:

late_days

flat fee

percentage fee

final late_fee_amount

Updates fact table for downstream reporting.

## Queries
A)<img width="811" height="292" alt="image" src="https://github.com/user-attachments/assets/61cd08b9-bf71-470f-bdcd-96d766a30a5a" />
   result: ![WhatsApp Image 2025-12-12 at 19 01 48](https://github.com/user-attachments/assets/fc2ab80d-15ce-40a0-8c01-7cbf0ef1b8e5)

B)<img width="928" height="176" alt="image" src="https://github.com/user-attachments/assets/66e68dcc-bd28-4784-9e4d-299f0b90bc2d" />
   result : ![WhatsApp Image 2025-12-12 at 19 01 59](https://github.com/user-attachments/assets/9c779c17-7387-4864-9872-6847dcafeb20)
C)<img width="980" height="267" alt="image" src="https://github.com/user-attachments/assets/56b2b28b-5431-446c-97d5-ab9f8c430255" />
   result: <img width="1246" height="222" alt="image" src="https://github.com/user-attachments/assets/1437d958-e6f2-4693-930c-2e1fc635c568" />
D)<img width="932" height="302" alt="image" src="https://github.com/user-attachments/assets/70224f1a-678c-42d3-af48-07425cb92445" />
   result: <img width="1280" height="220" alt="image" src="https://github.com/user-attachments/assets/1e599246-000f-4957-8b0f-0f147257b880" />
E)<img width="840" height="374" alt="image" src="https://github.com/user-attachments/assets/347d4a29-e423-44e4-8679-71c5343126da" />
   result: <img width="1280" height="502" alt="image" src="https://github.com/user-attachments/assets/0abe7f64-f7d6-4c27-92db-39904fabda9a" />
F)<img width="586" height="258" alt="image" src="https://github.com/user-attachments/assets/16ab8584-4d06-4013-8396-346440bc3761" />
   result: <img width="766" height="381" alt="image" src="https://github.com/user-attachments/assets/735a8735-c2fe-49e3-9b59-39ea4c4c28f4" />
G)<img width="1085" height="298" alt="image" src="https://github.com/user-attachments/assets/624a8d22-1e6a-49af-b332-3217ade784c9" />
   result: <img width="1280" height="387" alt="image" src="https://github.com/user-attachments/assets/26906aa2-8b5d-4773-bdb3-91cd144a6f33" />
















