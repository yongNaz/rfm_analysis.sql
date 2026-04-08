-- Data Cleaning

-- ROW COUNT & UNIQUENESS
SELECT
 COUNT(*) AS total_rows,
 COUNT(DISTINCT 'Row ID') AS unique_row_id,
 COUNT(DISTINCT 'Order ID') AS unique_orders,
 COUNT(DISTINCT 'Customer ID') AS unique_customers
FROM `sales2.SalesPerformance`;

-- CHECK MISSING VALUES
SELECT
 COUNTIF('Order ID' IS NULL) AS missing_order_id,
 COUNTIF('Order Date' IS NULL) AS missing_order_date,
 COUNTIF('Ship Date' IS NULL) AS missing_ship_date,
 COUNTIF('Customer ID' IS NULL) AS missing_customer_id,
 COUNTIF('Customer Name' IS NULL) AS missing_customer_name,
 COUNTIF('City' IS NULL) AS missing_city,
 COUNTIF('State' IS NULL) AS missing_state,
 COUNTIF('Postal Code' IS NULL) AS missing_postal_code,
 COUNTIF('Sales' IS NULL) AS missing_sales,
 COUNTIF('Profit' IS NULL) AS missing_profit
FROM `sales2.SalesPerformance`;

-- CHECK DUPLICATE ROWS
SELECT 'Row ID', COUNT(*) AS duplicate_count,
FROM `sales2.SalesPerformance`
GROUP BY 'Row ID',
HAVING COUNT(*) > 1;

-- CHECK DATA TYPES ISSUES (INVALID CASTING)
SELECT *
FROM `sales2.SalesPerformance`
WHERE SAFE_CAST('Sales' AS FLOAT64) IS NULL
OR SAFE_CAST('Profit' AS FLOAT64) IS NULL
OR SAFE_CAST('Quantity' AS INT64) IS NULL;

-- VALIDATE DATA FORMAT
SELECT *
FROM `sales2.SalesPerformance`
WHERE PARSE_DATE('%Y-%m-%d', 'Order Date') IS NULL
OR PARSE_DATE('%Y-%m-%d', 'Ship Date') IS NULL;

-- CHECK SUSPICIOUS VALUES
SELECT
 MIN(
  
 )