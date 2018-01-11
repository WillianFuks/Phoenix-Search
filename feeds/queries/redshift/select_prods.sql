CREATE TEMPORARY TABLE allowed_skus_{indx} (
  sku VARCHAR(20)
);

INSERT INTO allowed_skus_{indx} VALUES {skus_list};

SELECT
  prods.sku_config AS sku,
  product_name AS title,
  CONCAT('https://dafitistatic-a.akamaihd.net/p/', CONCAT(CONCAT(REPLACE(product_name, ' ', '-'), '-'), CONCAT(REVERSE(src_bob_fk_catalog_config), '-1-zoom.jpg'))) img_template_1,
  CONCAT('https://dafitistatic-a.akamaihd.net/p/', CONCAT(CONCAT(REPLACE(product_name, ' ', '-'), '-'), CONCAT(REVERSE(src_bob_fk_catalog_config), '-2-zoom.jpg'))) img_template_2,
  CONCAT('https://dafitistatic-a.akamaihd.net/p/', CONCAT(CONCAT(REPLACE(product_name, ' ', '-'), '-'), CONCAT(REVERSE(src_bob_fk_catalog_config), '-3-zoom.jpg'))) img_template_3,
  CONCAT('https://dafitistatic-a.akamaihd.net/p/', CONCAT(CONCAT(REPLACE(product_name, ' ', '-'), '-'), CONCAT(REVERSE(src_bob_fk_catalog_config), '-4-zoom.jpg'))) img_template_4,
  product_url,
  COALESCE(stock.quantity, 0) AS stock_count,
  COALESCE(seller.company_name, 'GFG') AS owner
FROM star_schema.dim_product_config AS prods
INNER JOIN allowed_skus_{indx} askus
  ON prods.sku_config = askus.sku
LEFT JOIN raw_bob_dafiti_br.seller AS seller
  ON prods.fk_seller = seller.id_seller
LEFT JOIN (SELECT sku_config, SUM(quantity) quantity FROM raw_solr_memcached.raw_stock GROUP BY 1) AS stock
  ON prods.sku_config = stock.sku_config
WHERE TRUE
  AND prods.fk_company = {fk_company};
