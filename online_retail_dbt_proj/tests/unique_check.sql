WITH duplicates AS (
    SELECT
        order_id,
        sku,
        order_status,
        courier_status,
        COUNT(*) AS count
    FROM
        {{ ref('fact_table') }} 
    GROUP BY
        order_id,
        sku,
        order_status,
        courier_status
    HAVING
        COUNT(*) > 1
)

SELECT * FROM duplicates