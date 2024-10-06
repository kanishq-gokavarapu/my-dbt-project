WITH product AS ( SELECT DISTINCT  sku, style, category, product_size
FROM {{ ref('stg_amazon_sales') }})
SELECT * FROM product