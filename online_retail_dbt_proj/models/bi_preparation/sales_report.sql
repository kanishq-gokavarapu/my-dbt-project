{{ config(materialized='table')}}
WITH sales_report AS (
    SELECT
        f.order_id,
        f.order_date,
        f.quantity_sold,
        f.revenue,
        p.style,
        p.category,
        c.city,
        c.state as state_name,
        d.year as year_num,
        d.month as month_num,
        d.day as day_num,
        d.season
    FROM {{ ref('fact_table') }} f
    JOIN {{ ref('product_detail') }} p ON f.sku = p.sku
    JOIN {{ ref('address_details') }} c ON f.ship_postal = c.ship_postal
    JOIN {{ref('date_breakup')}} d on f.order_Date = d.order_Date
    where quantity_sold > 0
)

SELECT order_id, order_date,sum(quantity_sold),sum(revenue),style,category,city,state_name, year_num, month_num, day_num, season
FROM sales_report group by order_id, order_date, style,category,city,state_name, year_num, month_num, day_num, season