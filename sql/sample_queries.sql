.headers on
.mode column

-- Sample A–G queries against SQLite warehouse (insurance.db).
-- Run with: sqlite3 data/warehouse/insurance.db ".read sql/sample_queries.sql"

.print 'A) Total customers by region (filter blanks, sorted desc)'
SELECT region, COUNT(DISTINCT customer_key) AS customers
FROM dim_customer dc
LEFT JOIN dim_address da ON dc.address_id = da.address_id
WHERE region IS NOT NULL AND TRIM(region) <> ''
GROUP BY region
ORDER BY customers DESC, region ASC;
.print ''

.print 'B) Total policies by policy type (filter junk types, sorted desc)'
SELECT policy_type, COUNT(*) AS policy_count
FROM dim_policy
WHERE policy_type IS NOT NULL AND TRIM(policy_type) <> '' AND policy_type NOT LIKE '%1900%'
GROUP BY policy_type
ORDER BY policy_count DESC, policy_type ASC;
.print ''

.print 'C) Premium and total policy amount by region (filter blanks, sorted desc)'
SELECT da.region,
       SUM(fp.premium_amt) AS total_premium,
       SUM(fp.total_policy_amt) AS total_policy_amount
FROM fact_policy_payments fp
LEFT JOIN dim_customer dc ON fp.customer_key = dc.customer_key
LEFT JOIN dim_address da ON dc.address_id = da.address_id
WHERE da.region IS NOT NULL AND TRIM(da.region) <> ''
GROUP BY da.region
ORDER BY total_premium DESC, da.region ASC;
.print ''

.print 'D) Late payments: average days_delay and estimated late fees (positive delays only)'
SELECT da.region,
       AVG(fp.days_delay) AS avg_delay_days,
       SUM(fp.late_fee_est) AS estimated_late_fees
FROM fact_policy_payments fp
LEFT JOIN dim_customer dc ON fp.customer_key = dc.customer_key
LEFT JOIN dim_address da ON dc.address_id = da.address_id
WHERE fp.days_delay IS NOT NULL AND fp.days_delay > 0
  AND da.region IS NOT NULL AND TRIM(da.region) <> ''
GROUP BY da.region
ORDER BY avg_delay_days DESC, da.region ASC;
.print ''

.print 'E) Policies nearing end within next year (from policy_end_date)'
SELECT fp.policy_id,
       dp.policy_name,
       dd.full_date AS policy_end_date,
       dc.customer_name,
       da.region
FROM fact_policy_payments fp
JOIN dim_policy dp ON fp.policy_id = dp.policy_id
JOIN dim_customer dc ON fp.customer_key = dc.customer_key
LEFT JOIN dim_address da ON dc.address_id = da.address_id
LEFT JOIN dim_date dd ON fp.policy_end_date_id = dd.date_id
WHERE dd.full_date BETWEEN date('now') AND date('now','+1 year')
  AND da.region IS NOT NULL AND TRIM(da.region) <> ''
ORDER BY dd.full_date ASC, fp.policy_id ASC;
.print ''

.print 'F) Top 10 customers by premium_amt_paid_tilldate'
SELECT dc.customer_name,
       SUM(fp.premium_amt_paid_tilldate) AS paid_to_date
FROM fact_policy_payments fp
JOIN dim_customer dc ON fp.customer_key = dc.customer_key
GROUP BY dc.customer_name
HAVING paid_to_date IS NOT NULL
ORDER BY paid_to_date DESC, dc.customer_name ASC
LIMIT 10;
.print ''

.print 'G) Payment punctuality by policy type (filter junk policy_type, sort by lowest delay)'
SELECT dp.policy_type,
       AVG(fp.days_delay) AS avg_delay_days,
       SUM(CASE WHEN fp.days_delay <= 0 THEN 1 ELSE 0 END) AS on_time_or_early,
       COUNT(*) AS total_payments
FROM fact_policy_payments fp
JOIN dim_policy dp ON fp.policy_id = dp.policy_id
WHERE dp.policy_type IS NOT NULL AND TRIM(dp.policy_type) <> '' AND dp.policy_type NOT LIKE '%1900%'
GROUP BY dp.policy_type
ORDER BY avg_delay_days ASC, dp.policy_type ASC;
.print ''

