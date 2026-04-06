CREATE OR REPLACE TABLE `rfm26-492505.sales.rfm_analysis_results` AS
WITH rfm_base AS (
  SELECT
    CustomerID,
    -- Analysis date set to 2011-12-10 for your 2010-2011 dataset
    DATE_DIFF(DATE('2011-12-10'), DATE(MAX(InvoiceDate)), DAY) AS recency,
    COUNT(DISTINCT InvoiceNo) AS frequency,
    SUM(Quantity * UnitPrice) AS monetary
  FROM `rfm26-492505.sales.salesdata`
  WHERE CustomerID IS NOT NULL 
    AND Quantity > 0 
    AND UnitPrice > 0
  GROUP BY CustomerID
),
rfm_scores AS (
  SELECT *,
    -- Recency: Low days = High Score (NTILE sorts DESC)
    NTILE(5) OVER (ORDER BY recency DESC) AS R,
    -- Frequency & Monetary: High value = High Score (NTILE sorts ASC)
    NTILE(5) OVER (ORDER BY frequency ASC) AS F,
    NTILE(5) OVER (ORDER BY monetary ASC) AS M
  FROM rfm_base
),
rfm_segmented AS (
  SELECT *,
    CONCAT(CAST(R AS STRING), CAST(F AS STRING)) AS RF_Score
  FROM rfm_scores
)
SELECT 
  *,
  CASE 
    WHEN RF_Score IN ('55', '54', '45') THEN 'Champions'
    WHEN RF_Score IN ('52', '53', '42', '43', '34', '35', '44') THEN 'Loyal Customers'
    WHEN RF_Score IN ('51', '41', '31') THEN 'Promising'
    WHEN RF_Score IN ('32', '33', '24', '25') THEN 'Need Attention'
    WHEN RF_Score IN ('22', '23') THEN 'About to Sleep'
    WHEN RF_Score IN ('14', '15', '25') THEN 'At Risk'
    WHEN RF_Score IN ('12', '13') THEN 'Hibernating'
    WHEN RF_Score = '11' THEN 'Lost'
    ELSE 'Other'
  END AS customer_segment
FROM rfm_segmented;