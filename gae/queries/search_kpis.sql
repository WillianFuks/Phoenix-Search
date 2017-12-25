#standardSQL
CREATE TEMP FUNCTION removeAccents(phrase STRING) RETURNS STRING AS ((
SELECT
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(phrase,
                r'[àáâäåã]', 'a'),
              r'[èéêëẽ]', 'e'),
            r'[ìíîïĩ]', 'i'),
          r'[òóôöøõ]', 'o'),
        r'[ùúûüũ]', 'u'),
      r'ç', 'c'),
    r'ÿ', 'y'),
  r'ñ', 'n')
));

CREATE TEMP FUNCTION slugify(phrase STRING) RETURNS STRING AS ((
  SELECT 
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(removeAccents(LOWER(phrase)),
                r'\s+', '-'), # replaces space with '-'
              r'&', '-e-'), # replaces & with '-e-'
            r'[^\w-]+', ''), # replaces non-word chars
          r'--+', '-'), # replaces multiple '-' with single one
        r'^-+', ''), # trim '-' from start of text
      r'-+$', '') # trim '-' from end of text
));

CREATE TEMP FUNCTION isSearch(search_phrase STRING, URL STRING) RETURNS BOOL AS ((
  SELECT LOGICAL_AND(STRPOS(URL, x) > 0 OR STRPOS(LOWER(URL), removeAccents(LOWER(x))) > 0) FROM UNNEST(SPLIT(search_phrase, ' ')) x
));

CREATE TEMP FUNCTION extractConfigSku(sku STRING) RETURNS STRING AS (
  CASE WHEN (CHAR_LENGTH(sku) - CHAR_LENGTH(REGEXP_REPLACE(sku, r'-', '')) = 3) OR (CHAR_LENGTH(sku) - CHAR_LENGTH(REGEXP_REPLACE(sku, r'-', '')) = 1) THEN REGEXP_EXTRACT(sku, r'(.*)-[0-9A-Z]+')
     ELSE sku END
);

CREATE TEMP FUNCTION processPurchases(skus_clicked ARRAY<STRING>, purchased_skus ARRAY<STRUCT<sku STRING, revenue FLOAT64> >) RETURNS FLOAT64 AS ((
  SELECT SUM(revenue) FROM UNNEST(purchased_skus) WHERE EXISTS(SELECT 1 FROM UNNEST(skus_clicked) sku_clicked WHERE sku_clicked = sku)
));

CREATE TEMP FUNCTION buildFinalResult(hits ARRAY<STRUCT<search STRING, freq INT64, click INT64, net_revenue FLOAT64, bounce INT64>>, rvn FLOAT64) RETURNS STRUCT<search_data ARRAY<STRUCT<search STRING, freq INT64, clicks INT64, net_revenue FLOAT64, bounce INT64>>, search_flg INT64, net_search_flg INT64, search_rvn FLOAT64, net_search_rvn FLOAT64, u_conversion INT64, u_search_conversion INT64, net_clicks INT64> AS ((
 # this solution is kinda ugly but we do so Datastudio can process final results reliably. 
 STRUCT(ARRAY(SELECT AS STRUCT search, SUM(freq) AS freq, SUM(click) AS clicks, SUM(net_revenue) AS net_revenue, SUM(bounce) AS bounce FROM UNNEST(hits) GROUP BY search) AS data, CASE WHEN EXISTS(SELECT 1 FROM UNNEST(hits) WHERE freq > 0) THEN 1 END AS search_flg, CASE WHEN EXISTS(SELECT 1 FROM UNNEST(hits) WHERE net_revenue > 0) THEN 1 END AS net_search_flg, CASE WHEN EXISTS(SELECT 1 FROM UNNEST(hits) WHERE freq > 0) THEN rvn END AS search_rvn, (SELECT SUM(net_revenue) FROM UNNEST(hits)) AS net_search_rvn, IF(rvn > 0, 1, NULL) AS u_conversion, (CASE WHEN EXISTS(SELECT 1 FROM UNNEST(hits) WHERE freq > 0) AND rvn > 0 THEN 1 END) AS u_search_conversion, (SELECT SUM(click) FROM UNNEST(hits) WHERE freq > 0 and net_revenue > 0) AS net_clicks)
));


WITH `data` AS(
  SELECT "1" AS fullvisitorid, 1 AS visitid,  "20171220" AS date, STRUCT<totalTransactionRevenue FLOAT64>  (100000000.0) AS totals, ARRAY<STRUCT<hitNumber INT64, page STRUCT<pagePath STRING>, ecommerceAction STRUCT<action_type STRING>, eventInfo STRUCT<eventCategory STRING, eventAction STRING, eventLabel STRING>, product ARRAY<STRUCT<productSku STRING, isClick BOOL, productQuantity INT64, productPrice FLOAT64> >>> 
    [STRUCT(1 AS hitNumber, STRUCT("/" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT(NULL AS eventCategory, NULL AS eventAction, NULL AS eventLabel) AS eventInfo,
     [STRUCT("" AS productSku, False AS isClick, 0 AS productQuantity, 0.0 AS productPrice)] AS product),
     
     STRUCT(2 AS hitNumber, STRUCT("/?q=fake+search" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT(NULL AS eventCategory, NULL AS eventAction, NULL AS eventLabel) AS eventInfo,
     [STRUCT("sku0" AS productSku, False AS isClick, 0 AS productQuantity, 0.0 AS productPrice), STRUCT("sku1" AS productSku, False AS isClick, 0 AS productQuantity, 0.0 AS productPrice)] AS product),
     
     STRUCT(3 AS hitNumber, STRUCT("/?q=fake+search" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT(NULL AS eventCategory, NULL AS eventAction, NULL AS eventLabel) AS eventInfo,
     [STRUCT("sku0" AS productSku, True AS isClick, 0 AS productQuantity, 0.0 AS productPrice)] AS product),
     
     STRUCT(4 AS hitNumber, STRUCT("/checkout" AS pagePath) AS page,
     STRUCT("6" AS action_type) AS ecommerceAction,
     STRUCT(NULL AS eventCategory, NULL AS eventAction, NULL AS eventLabel) AS eventInfo,
     [STRUCT("sku0-000" AS productSku, False AS isClick, 1 AS productQuantity, 100000000.0 AS productPrice)] AS product)] hits

UNION ALL
  
    SELECT "2" AS fullvisitorid, 1 as visitid, "20171220" AS date, STRUCT<totalTransactionRevenue FLOAT64>  (NULL) AS totals, ARRAY<STRUCT<hitNumber INT64, page STRUCT<pagePath STRING>, ecommerceAction STRUCT<action_type STRING>, eventInfo STRUCT<eventCategory STRING, eventAction STRING, eventLabel STRING>, product ARRAY<STRUCT<productSku STRING, isClick BOOL, productQuantity INT64, productPrice FLOAT64> >>> 
    [STRUCT(1 AS hitNumber, STRUCT("/" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT(NULL AS eventCategory, NULL AS eventAction, NULL AS eventLabel) AS eventInfo,
     [STRUCT("" AS productSku, False AS isClick, 0 AS productQuantity, 0.0 AS productPrice)] AS product),
     
     STRUCT(2 AS hitNumber, STRUCT("/" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT("search" AS eventCategory, "submit" AS eventAction, "search string" AS eventLabel) AS eventInfo,
     NULL AS product),
     
     STRUCT(3 AS hitNumber, STRUCT("/?q=search+string" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT(NULL AS eventCategory, NULL AS eventAction, NULL AS eventLabel) AS eventInfo,
     [STRUCT("" AS productSku, False AS isClick, 0 AS productQuantity, 0.0 AS productPrice)] AS product)] hits

UNION ALL     
     
    SELECT "2" AS fullvisitorid, 2 as visitid, "20171220" AS date, STRUCT<totalTransactionRevenue FLOAT64>  (NULL) AS totals, ARRAY<STRUCT<hitNumber INT64, page STRUCT<pagePath STRING>, ecommerceAction STRUCT<action_type STRING>, eventInfo STRUCT<eventCategory STRING, eventAction STRING, eventLabel STRING>, product ARRAY<STRUCT<productSku STRING, isClick BOOL, productQuantity INT64, productPrice FLOAT64> >>> 
    [STRUCT(1 AS hitNumber, STRUCT("/" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT(NULL AS eventCategory, NULL AS eventAction, NULL AS eventLabel) AS eventInfo,
     [STRUCT("" AS productSku, False AS isClick, 0 AS productQuantity, 0.0 AS productPrice)] AS product),
     
     STRUCT(2 AS hitNumber, STRUCT("/" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT("search" AS eventCategory, "submit" AS eventAction, "search string" AS eventLabel) AS eventInfo,
     NULL AS product),
     
     STRUCT(3 AS hitNumber, STRUCT("/?q=search+string" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT(NULL AS eventCategory, NULL AS eventAction, NULL AS eventLabel) AS eventInfo,
     [STRUCT("" AS productSku, False AS isClick, 0 AS productQuantity, 0.0 AS productPrice)] AS product),     
     
     
     STRUCT(4 AS hitNumber, STRUCT("/?q=search+string" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT(NULL AS eventCategory, NULL AS eventAction, NULL AS eventLabel) AS eventInfo,
     [STRUCT("sku0" AS productSku, True AS isClick, 0 AS productQuantity, 0.0 AS productPrice)] AS product),
     
     STRUCT(5 AS hitNumber, STRUCT("/" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT("search" AS eventCategory, "submit" AS eventAction, "search another string" AS eventLabel) AS eventInfo,
     NULL AS product),
     
     STRUCT(6 AS hitNumber, STRUCT("/?q=search+another+string" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT(NULL AS eventCategory, NULL AS eventAction, NULL AS eventLabel) AS eventInfo,
     [STRUCT("sku0" AS productSku, True AS isClick, 0 AS productQuantity, 0.0 AS productPrice)] AS product)] hits
     
 UNION ALL
 
     SELECT "3" AS fullvisitorid, 1 as visitid, "20171220" AS date, STRUCT<totalTransactionRevenue FLOAT64>  (200000000.0) AS totals, ARRAY<STRUCT<hitNumber INT64, page STRUCT<pagePath STRING>, ecommerceAction STRUCT<action_type STRING>, eventInfo STRUCT<eventCategory STRING, eventAction STRING, eventLabel STRING>, product ARRAY<STRUCT<productSku STRING, isClick BOOL, productQuantity INT64, productPrice FLOAT64> >>> 
    [STRUCT(1 AS hitNumber, STRUCT("/" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT(NULL AS eventCategory, NULL AS eventAction, NULL AS eventLabel) AS eventInfo,
     [STRUCT("" AS productSku, False AS isClick, 0 AS productQuantity, 0.0 AS productPrice)] AS product),
     
     STRUCT(2 AS hitNumber, STRUCT("/" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT("search" AS eventCategory, "submit" AS eventAction, "search string" AS eventLabel) AS eventInfo,
     [STRUCT("" AS productSku, False AS isClick, 0 AS productQuantity, 0.0 AS productPrice)] AS product),
     
     STRUCT(3 AS hitNumber, STRUCT("/?q=search+string" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT(NULL AS eventCategory, NULL AS eventAction, NULL AS eventLabel) AS eventInfo,
     [STRUCT("sku0" AS productSku, True AS isClick, 0 AS productQuantity, 0.0 AS productPrice)] AS product),
     
     STRUCT(4 AS hitNumber, STRUCT("/checkout" AS pagePath) AS page,
     STRUCT("6" AS action_type) AS ecommerceAction,
     STRUCT(NULL AS eventCategory, NULL AS eventAction, NULL AS eventLabel) AS eventInfo,
     [STRUCT("sku0-000" AS productSku, False AS isClick, 2 AS productQuantity, 100000000.0 AS productPrice)] AS product)    
     ] hits
     
UNION ALL
 
     SELECT "4" AS fullvisitorid, 1 as visitid, "20171220" AS date, STRUCT<totalTransactionRevenue FLOAT64>  (100000000.0) AS totals, ARRAY<STRUCT<hitNumber INT64, page STRUCT<pagePath STRING>, ecommerceAction STRUCT<action_type STRING>, eventInfo STRUCT<eventCategory STRING, eventAction STRING, eventLabel STRING>, product ARRAY<STRUCT<productSku STRING, isClick BOOL, productQuantity INT64, productPrice FLOAT64> >>> 
    [STRUCT(1 AS hitNumber, STRUCT("/" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT(NULL AS eventCategory, NULL AS eventAction, NULL AS eventLabel) AS eventInfo,
     [STRUCT("" AS productSku, False AS isClick, 0 AS productQuantity, 0.0 AS productPrice)] AS product),
     
     STRUCT(2 AS hitNumber, STRUCT("/" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT("search" AS eventCategory, "submit" AS eventAction, "Sêãrchí CrÃzĩ Éstrìng" AS eventLabel) AS eventInfo,
     NULL AS product),
     
     STRUCT(3 AS hitNumber, STRUCT("/?q=Sêãrchí CrÃzĩ Éstrìng" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT(NULL AS eventCategory, NULL AS eventAction, NULL AS eventLabel) AS eventInfo,
     [STRUCT("sku0" AS productSku, True AS isClick, 0 AS productQuantity, 0.0 AS productPrice)] AS product),

     STRUCT(4 AS hitNumber, STRUCT("/?q=Searchi%20Crazi%20Estring&sort=discount" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT(NULL AS eventCategory, NULL AS eventAction, NULL AS eventLabel) AS eventInfo,
     [STRUCT("sku0" AS productSku, True AS isClick, 0 AS productQuantity, 0.0 AS productPrice)] AS product),

     STRUCT(5 AS hitNumber, STRUCT("/?q=Searchi%20Crazi%20Estring&sort=discount" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT("search" AS eventCategory, "submit" AS eventAction, "Searchi crÃzĩ Éstrìng" AS eventLabel) AS eventInfo,
     [STRUCT("" AS productSku, False AS isClick, 0 AS productQuantity, 0.0 AS productPrice)] AS product),
     
     STRUCT(6 AS hitNumber, STRUCT("/?q=Searchi%20crazi%20Estring&sort=discount" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT(NULL AS eventCategory, NULL AS eventAction, NULL AS eventLabel) AS eventInfo,
     [STRUCT("sku0" AS productSku, True AS isClick, 0 AS productQuantity, 0.0 AS productPrice)] AS product),
     
     STRUCT(7 AS hitNumber, STRUCT("/checkout" AS pagePath) AS page,
     STRUCT("6" AS action_type) AS ecommerceAction,
     STRUCT(NULL AS eventCategory, NULL AS eventAction, NULL AS eventLabel) AS eventInfo,
     [STRUCT("sku0-000" AS productSku, False AS isClick, 1 AS productQuantity, 100000000.0 AS productPrice)] AS product)    
     ] hits
     
     UNION ALL
 
     SELECT "4" AS fullvisitorid, 2 as visitid, "20171220" AS date, STRUCT<totalTransactionRevenue FLOAT64>  (100000000.0) AS totals, ARRAY<STRUCT<hitNumber INT64, page STRUCT<pagePath STRING>, ecommerceAction STRUCT<action_type STRING>, eventInfo STRUCT<eventCategory STRING, eventAction STRING, eventLabel STRING>, product ARRAY<STRUCT<productSku STRING, isClick BOOL, productQuantity INT64, productPrice FLOAT64> >>> 
    [STRUCT(1 AS hitNumber, STRUCT("/" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT(NULL AS eventCategory, NULL AS eventAction, NULL AS eventLabel) AS eventInfo,
     [STRUCT("" AS productSku, False AS isClick, 0 AS productQuantity, 0.0 AS productPrice)] AS product),
     
     STRUCT(2 AS hitNumber, STRUCT("/" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT("search" AS eventCategory, "submit" AS eventAction, "Seãrchí Crazĩ estrìng" AS eventLabel) AS eventInfo,
     [STRUCT("" AS productSku, False AS isClick, 0 AS productQuantity, 0.0 AS productPrice)] AS product),
     
     STRUCT(3 AS hitNumber, STRUCT("/?q=Seãrchí Crazĩ estrìng" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT(NULL AS eventCategory, NULL AS eventAction, NULL AS eventLabel) AS eventInfo,
     NULL AS product),

     STRUCT(4 AS hitNumber, STRUCT("/?q=Searchi%20Crazi%20estring&sort=discount" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT(NULL AS eventCategory, NULL AS eventAction, NULL AS eventLabel) AS eventInfo,
     [STRUCT("sku1" AS productSku, True AS isClick, 0 AS productQuantity, 0.0 AS productPrice)] AS product),

     STRUCT(5 AS hitNumber, STRUCT("/?q=Searchi%20Crazi%20estring&sort=discount" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT("search" AS eventCategory, "submit" AS eventAction, "search other string" AS eventLabel) AS eventInfo,
     NULL AS product),
     
     STRUCT(6 AS hitNumber, STRUCT("/?q=search%20other%20string&sort=discount" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT(NULL AS eventCategory, NULL AS eventAction, NULL AS eventLabel) AS eventInfo,
     [STRUCT("sku0" AS productSku, True AS isClick, 0 AS productQuantity, 0.0 AS productPrice)] AS product),
     
     STRUCT(7 AS hitNumber, STRUCT("/checkout" AS pagePath) AS page,
     STRUCT("6" AS action_type) AS ecommerceAction,
     STRUCT(NULL AS eventCategory, NULL AS eventAction, NULL AS eventLabel) AS eventInfo,
     [STRUCT("sku0-000" AS productSku, False AS isClick, 1 AS productQuantity, 100000000.0 AS productPrice), STRUCT("sku1-000" AS productSku, False AS isClick, 1 AS productQuantity, 150000000.0 AS productPrice)] AS product)    
     ] hits
     
UNION ALL
 
     SELECT "4" AS fullvisitorid, 1 as visitid, "20171221" AS date, STRUCT<totalTransactionRevenue FLOAT64>  (100000000.0) AS totals, ARRAY<STRUCT<hitNumber INT64, page STRUCT<pagePath STRING>, ecommerceAction STRUCT<action_type STRING>, eventInfo STRUCT<eventCategory STRING, eventAction STRING, eventLabel STRING>, product ARRAY<STRUCT<productSku STRING, isClick BOOL, productQuantity INT64, productPrice FLOAT64> >>> 
    [STRUCT(1 AS hitNumber, STRUCT("/" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT(NULL AS eventCategory, NULL AS eventAction, NULL AS eventLabel) AS eventInfo,
     [STRUCT("" AS productSku, False AS isClick, 0 AS productQuantity, 0.0 AS productPrice)] AS product),
     
     STRUCT(2 AS hitNumber, STRUCT("/" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT("search" AS eventCategory, "submit" AS eventAction, "Seãrchí Crazĩ estrìng" AS eventLabel) AS eventInfo,
     NULL AS product),
     
     STRUCT(3 AS hitNumber, STRUCT("/?q=Seãrchí Crazĩ estrìng" AS pagePath) AS page,
     STRUCT("0" AS action_type) AS ecommerceAction,
     STRUCT(NULL AS eventCategory, NULL AS eventAction, NULL AS eventLabel) AS eventInfo,
     [STRUCT("sku0" AS productSku, True AS isClick, 0 AS productQuantity, 0.0 AS productPrice)] AS product)] hits
)

SELECT
  date,
  SUM(revenue) user_revenue,
  buildFinalResult(ARRAY_CONCAT_AGG(hits), SUM(revenue)) results
  FROM(
    SELECT
    fv,
    date,
    revenue,
    ARRAY(SELECT AS STRUCT search, 1 AS freq, MAX(IF(sku_clicked IS NOT NULL, 1, 0)) click, processPurchases(ARRAY_AGG(DISTINCT sku_clicked IGNORE NULLS), products_purchased) net_revenue, MAX(bounce) bounce FROM UNNEST(hits) WHERE search IS NOT NULL GROUP BY search) hits
    FROM(
      SELECT
         fv,
         date,
         revenue,
         ARRAY(SELECT AS STRUCT slugify(FIRST_VALUE(lbl) OVER (PARTITION BY flg ORDER BY hn)) search, IF(isSearch(FIRST_VALUE(lbl) OVER (PARTITION BY flg ORDER BY hn), pp) AND EXISTS(SELECT 1 FROM UNNEST(product) WHERE isClick), ARRAY(SELECT productSku FROM UNNEST(product))[SAFE_OFFSET(0)], NULL) sku_clicked, IF(isSearch(FIRST_VALUE(lbl) OVER (PARTITION BY flg ORDER BY hn), pp) AND hn = MAX(hn) OVER(), 1, NULL) bounce FROM UNNEST(hits)) hits,
         ARRAY(SELECT AS STRUCT extractConfigSku(productSku) sku, SUM(productQuantity * productPrice / 1e6) revenue FROM UNNEST(hits), UNNEST(product) WHERE act_type = '6' GROUP BY 1) products_purchased
      FROM(
        SELECT
          fullvisitorid fv,
          totals.totalTransactionRevenue / 1e6 revenue,
          date,
          ARRAY(SELECT AS STRUCT hitNumber hn, page.pagePath pp, eventInfo.eventCategory cat, IF(eventInfo.eventCategory = 'search', eventInfo.eventLabel, NULL) lbl, SUM(IF(eventInfo.eventCategory = 'search', 1, 0)) OVER(ORDER BY hitNumber) flg, ecommerceAction.action_type act_type, ARRAY(SELECT AS STRUCT productSku, isClick, productQuantity, productPrice FROM UNNEST(product)) product FROM UNNEST(hits)) hits
        #FROM `data`
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE TRUE
          #AND REGEXP_EXTRACT(_TABLE_SUFFIX, r'.*_(\d+)$') BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) ) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
          AND REGEXP_EXTRACT(_TABLE_SUFFIX, r'.*_(\d+)') = '{date}'
          AND EXISTS(SELECT 1 FROM UNNEST(hits) WHERE REGEXP_CONTAINS(page.hostname, r'{hostname}'))
          AND NOT REGEXP_CONTAINS(LOWER(geonetwork.networklocation), r'{geonetworklocation}')
      )
    )
  )
GROUP BY fv, date
