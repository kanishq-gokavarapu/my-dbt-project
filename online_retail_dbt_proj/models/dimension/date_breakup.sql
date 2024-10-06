WITH dates AS (
    SELECT DISTINCT
        order_Date ,
        EXTRACT(YEAR FROM order_Date) AS year,
        EXTRACT(MONTH FROM order_Date) AS month,
        EXTRACT(DAY FROM order_Date) AS day,
        EXTRACT(WEEK FROM order_Date) AS week,
        CASE 
            WHEN EXTRACT(MONTH FROM order_Date) IN (12, 1, 2) THEN 'Winter'
            WHEN EXTRACT(MONTH FROM order_Date) IN (3, 4, 5) THEN 'Spring'
            WHEN EXTRACT(MONTH FROM order_Date) IN (6, 7, 8) THEN 'Summer'
            WHEN EXTRACT(MONTH FROM order_Date) IN (9, 10, 11) THEN 'Autumn'
        END AS season
    FROM {{ ref('stg_amazon_sales')}}
)
SELECT * FROM dates