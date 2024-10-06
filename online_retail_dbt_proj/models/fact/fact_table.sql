{{ config(materialized='table')}}
with sales AS ( SELECT order_id, order_date, order_status, sku, ASIN, courier_status, ship_postal,
qty AS quantity_sold, amount AS revenue FROM {{ ref('stg_amazon_sales') }} )

SELECT * FROM sales