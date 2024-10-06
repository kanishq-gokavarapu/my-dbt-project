WITH raw_data AS 
(SELECT distinct order_id ,order_date , order_status ,fulfilment ,sales_channel ,ship_service_level ,style ,sku ,category ,
product_size ,ASIN ,courier_status ,qty ,currency ,Amount ,ship_city ,ship_state , ship_postal  ,ship_count ,
promotion_ids ,B2B ,fulfilled_by 
FROM {{ source('retail_analytics', 'amazon_sale_report') }} )
SELECT * FROM raw_data WHERE order_id IS NOT NULL