--Query 01: calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month)
SELECT 
  Format_date('%Y%m',Parse_date('%Y%m%d',date )) AS month
  ,COUNT(totals.visits) AS visits
  ,SUM(totals. pageviews) AS pageviews
  ,SUM(totals.transactions) AS transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
where _table_suffix between '0101' and '0331' 
GROUP BY month
ORDER BY month;

--Query 02: Bounce rate per traffic source in July 2017 (Bounce_rate = num_bounce/total_visit) (order by total_visit DESC)
WITH raw_data AS(
    SELECT 
      trafficSource. source AS source
      ,SUM(totals. visits) AS total_visits
      ,COUNT(totals. bounces) AS total_no_of_bounces
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
    GROUP BY trafficSource. source
    ORDER BY total_visits DESC)
SELECT 
  source 
  ,total_visits
  ,total_no_of_bounces
  ,ROUND(100*total_no_of_bounces/total_visits, 3) AS bounce_rate
FROM raw_data;

--Query 03: Revenue by traffic source by week, by month in June 2017
With raw_data AS(
    SELECT *
      ,'Month' as time_type  
      ,Format_date('%Y%m',Parse_date('%Y%m%d',date)) AS time
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*` 
    UNION ALL
    SELECT *
      ,'Week' as time_type  
      ,Format_date('%Y%W',Parse_date('%Y%m%d',date)) AS time
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*` )
SELECT
  time_type 
  ,raw_data.time
  ,trafficSource.source
  ,SUM(product.productRevenue)/1000000 AS revenue
FROM raw_data
  ,UNNEST (hits) AS hits
  ,UNNEST (hits.product) AS product
WHERE product.productRevenue IS NOT NULL
GROUP BY trafficSource.source,time_type,raw_data.time
ORDER BY revenue DESC;

-- Query 04: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017.
  With purchaser as(
    SELECT 
        Format_date('%Y%m',Parse_date('%Y%m%d',date )) AS month
        ,SUM(totals.pageviews) AS pageviews_purchaser
        ,COUNT(DISTINCT fullVisitorId) AS num_purchaser
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
        ,UNNEST (hits) AS hits
        ,UNNEST (hits.product) AS product
    where _table_suffix between '0601' and '0731' 
        AND totals.transactions >=1
        AND product.productRevenue IS NOT NULL
    GROUP BY month),

    non_purchaser AS (
    SELECT 
        Format_date('%Y%m',Parse_date('%Y%m%d',date )) AS month
        ,SUM(totals.pageviews) AS pageviews_nonpurchaser
        ,COUNT(DISTINCT fullVisitorId) AS num_nonpurchaser
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
        ,UNNEST (hits) AS hits
        ,UNNEST (hits.product) AS product
    where _table_suffix between '0601' and '0731' 
        AND totals.transactions IS NULL
        AND product.productRevenue IS NULL
    GROUP BY month)

SELECT
    month 
    ,ROUND(pageviews_purchaser/num_purchaser,8) AS avg_pageviews_purchase
    ,ROUND(pageviews_nonpurchaser/num_nonpurchaser,8) AS avg_pageviews_non_purchase
FROM purchaser
LEFT JOIN non_purchaser
USING (month)
ORDER BY month;


-- Query 05: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in July 2017.
SELECT 
  '201707'AS Month
  ,SUM(totals.transactions)/COUNT(DISTINCT fullVisitorId) AS Avg_total_transactions_per_user
 FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
  ,UNNEST (hits) AS hits
  ,UNNEST (hits.product) AS product
WHERE totals.transactions >=1
AND product.productRevenue IS NOT NULL;


-- Query 06: Average number of transactions per user that made a purchase in July 2017
SELECT 
  '201707' AS month
  ,ROUND((SUM(product.productRevenue)/1000000)/SUM(totals.visits),2) AS avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
  ,UNNEST (hits) AS hits
  ,UNNEST (hits.product) AS product
WHERE totals.transactions IS NOT NULL
AND product.productRevenue IS NOT NULL;


-- Query 07:  Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
  WITH raw_data AS (
      SELECT DISTINCT fullVisitorId AS id
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
      ,UNNEST (hits) AS hits
      ,UNNEST (hits.product) AS product
  WHERE product.productRevenue IS NOT NULL
      AND  totals.transactions >=1
      AND product.v2ProductName = "YouTube Men's Vintage Henley")

SELECT product.v2ProductName AS other_purchased_products
    ,SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` AS data
  ,UNNEST (data.hits) AS hits
  ,UNNEST (hits.product) AS product
WHERE data.fullVisitorId in (SELECT raw_data.id FROM raw_data)
  AND product.productRevenue IS NOT NULL
  AND  data.totals.transactions >=1
  AND product.v2ProductName <> "YouTube Men's Vintage Henley"
GROUP BY product.v2ProductName
ORDER BY quantity DESC;


--Query 08:  Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. For example, 100% product view then 40% add_to_cart and 10% purchase. Add_to_cart_rate = number product  add to cart/number product view. Purchase_rate = number product purchase/number product view. The output should be calculated in product level.

With view AS (
    SELECT 
      Format_date('%Y%m',Parse_date('%Y%m%d',date )) AS month
      ,COUNT(hits.eCommerceAction.action_type) AS num_product_view
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
      ,UNNEST (hits) AS hits
    where _table_suffix between '0101' and '0331' 
      AND hits.eCommerceAction IS NOT NULL
      AND hits.eCommerceAction.action_type = '2'
    GROUP BY month
    ORDER BY month),

    addtocard AS (
    SELECT 
      Format_date('%Y%m',Parse_date('%Y%m%d',date )) AS month
      ,COUNT(hits.eCommerceAction.action_type) AS num_addtocart
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
      ,UNNEST (hits) AS hits
    where _table_suffix between '0101' and '0331' 
      AND hits.eCommerceAction IS NOT NULL
      AND hits.eCommerceAction.action_type = '3'
    GROUP BY month
    ORDER BY month),

    purchase AS (
    SELECT 
      Format_date('%Y%m',Parse_date('%Y%m%d',date )) AS month
      ,COUNT(hits.eCommerceAction.action_type) AS num_purchase
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
      ,UNNEST (hits) AS hits
      ,UNNEST (hits.product) AS product
    where _table_suffix between '0101' and '0331' 
      AND hits.eCommerceAction IS NOT NULL
      AND hits.eCommerceAction.action_type = '6'
      AND product.productRevenue is not null
    GROUP BY month
    ORDER BY month)

SELECT month,num_product_view, num_addtocart, num_purchase
,100*num_addtocart/num_product_view AS add_to_cart_rate
,100*num_purchase/num_product_view AS purchase_rate

FROM (
  SELECT month,num_product_view, num_addtocart, num_purchase
  FROM view
    INNER JOIN addtocard USING ( month)
    INNER JOIN purchase USING ( month)
  ORDER BY month);




