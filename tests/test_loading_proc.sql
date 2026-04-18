
===============================================================================
Quick Smoke-Test Queries
===============================================================================

-- Row counts
SELECT 'dim_date'      AS tbl, COUNT(*) AS n FROM gold.dim_date
UNION ALL SELECT 'dim_customers',  COUNT(*) FROM gold.dim_customers
UNION ALL SELECT 'dim_products',   COUNT(*) FROM gold.dim_products
UNION ALL SELECT 'fact_sales',     COUNT(*) FROM gold.fact_sales;

-- Load log
SELECT TOP 20 * FROM gold.dw_load_log ORDER BY log_id DESC;

-- Orphan check
SELECT COUNT(*) AS unresolved_customers
FROM gold.fact_sales fs
WHERE NOT EXISTS (SELECT 1 FROM gold.dim_customers c WHERE c.customer_key = fs.customer_key);

-- SCD sanity: no two current rows for same BK
SELECT customer_id, COUNT(*) AS cnt
FROM gold.dim_customers
WHERE scd_is_current = 1
GROUP BY customer_id HAVING COUNT(*) > 1;
