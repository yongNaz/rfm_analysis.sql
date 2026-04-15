-- Data Cleaning (Customer Churn And Retention)

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
 MIN(Sales) AS min_sales,
 MAX(Sales) AS max_sales,
 MIN(Profit) AS min_profit,
 MAX(Profit) AS max_profit,
 MIN(Discount) AS min_discount,
 MAX(Discount) AS max_discount
FROM `sales2.SalesPerformance`;


-- DETECT INVALID BUSINESS LOGIC
SELECT *
FROM `sales2.SalesPerformance`
WHERE
 SAFE_CAST(`Sales` AS FLOAT64) < 0
 OR SAFE_CAST(`Discount` AS FLOAT64) > 1
 OR `Ship Date` < `Order Date`;


-- CHECK TEXT INCONSISTENCIES
SELECT DISTINCT `City`
FROM `sales2.SalesPerformance`
ORDER BY `City`;


-- CHECK CATEGORY DISTRIBUTION
SELECT `Category`, COUNT(*) AS count,
FROM `sales2.SalesPerformance`
GROUP BY `Category`;


--  CHECK CARDINALITY 
SELECT
 COUNT(DISTINCT `Product ID`) AS unique_products,
 COUNT(DISTINCT `Category`) AS unique_categories,
 COUNT(DISTINCT `Sub-Category`) AS unique_subcategories
FROM `sales2.SalesPerformance`;


-- STEP 2: CREATE CLEAN TABLE

-- CREATE CLEAN BASE TABLE
CREATE OR REPLACE TABLE `sales2.SalesPerformance_cleaned` AS
SELECT
  SAFE_CAST(`Row ID` AS INT64) AS row_id,
  `Order ID` AS order_id,
  `Customer ID` AS customer_id,
  `Customer Name` AS customer_name,

  -- Dates 
  `Order Date` AS order_date,
  `Ship Date` AS ship_date,

  -- Feature: shipping duration
  DATE_DIFF(`Ship Date`, `Order Date`, DAY) AS shipping_days,

  -- Standardized text
  TRIM(UPPER(`City`)) AS city,
  TRIM(UPPER(`State`)) AS state,
  TRIM(UPPER(`Segment`)) AS segment,
  TRIM(UPPER(`Category`)) AS category,
  TRIM(UPPER(`Sub-Category`)) AS sub_category,

  -- Numeric
  SAFE_CAST(`Sales` AS FLOAT64) AS sales,
  SAFE_CAST(`Profit` AS FLOAT64) AS profit,
  SAFE_CAST(`Quantity` AS INT64) AS quantity,
  SAFE_CAST(`Discount` AS FLOAT64) AS discount

FROM `sales2.SalesPerformance`
WHERE `Order ID` IS NOT NULL
  AND `Ship Date` >= `Order Date`;


-- REMOVE DUPLICATES
CREATE OR REPLACE TABLE `sales2.SalesPerformance_cleaned` AS
SELECT *
FROM (
  SELECT *,
  ROW_NUMBER() OVER (PARTITION BY row_id ORDER BY order_date) AS rn
  FROM `sales2.SalesPerformance_cleaned`
)
WHERE rn = 1;


-- ADD ANALYSIS FEATURES(FOR CHURN ANALSIS)
CREATE OR REPLACE TABLE `sales2.SalesPerformance_final` AS
SELECT
  *,

  -- Time features
  EXTRACT(YEAR FROM order_date) AS order_year,
  EXTRACT(MONTH FROM order_date) AS order_month,

  FORMAT_DATE('%Y-%m', order_date) AS order_year_month,

  -- Customer activity tracking
  MIN(order_date) OVER (PARTITION BY customer_id) AS first_order_date,
  MAX(order_date) OVER (PARTITION BY customer_id) AS last_order_date,

  -- Recency 
  DATE_DIFF(CURRENT_DATE(), 
            MAX(order_date) OVER (PARTITION BY customer_id), DAY) AS recency_days,

  -- Frequency
  COUNT(order_id) OVER (PARTITION BY customer_id) AS total_orders,

  -- Monetary
  SUM(sales) OVER (PARTITION BY customer_id) AS total_sales,

  -- Profit flag
  CASE 
    WHEN profit < 0 THEN 'LOSS'
    ELSE 'PROFIT'
  END AS profit_flag

FROM `sales2.SalesPerformance_cleaned`;


-- CREATE CUSTOMER LEVEL TABLE (FOR CHURN)
CREATE OR REPLACE TABLE `sales2.customer_summary` AS
SELECT
  customer_id,
  customer_name,

  MIN(order_date) AS first_order_date,
  MAX(order_date) AS last_order_date,

  DATE_DIFF(
  (SELECT MAX(order_date) FROM `sales2.SalesPerformance_final`),
  MAX(order_date),DAY) 
  AS recency_days,

  COUNT(order_id) AS total_orders,
  SUM(sales) AS total_sales,
  SUM(profit) AS total_profit

FROM `sales2.SalesPerformance_final`
GROUP BY customer_id, customer_name;


-- BUSINESS LOGIC (CHURN)
CREATE OR REPLACE TABLE `sales2.customer_churn` AS
SELECT *,
  CASE 
    WHEN recency_days > 90 THEN 'CHURNED'
    ELSE 'ACTIVE'
  END AS customer_status
FROM `sales2.customer_summary`;


