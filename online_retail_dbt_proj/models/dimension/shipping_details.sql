WITH shipping_details AS ( SELECT DISTINCT order_id, order_status, courier_status,fulfilment, sales_channel,
 ship_service_level, B2B,fulfilled_by,promotion_ids
FROM {{ ref('stg_amazon_sales') }})
SELECT * FROM shipping_details