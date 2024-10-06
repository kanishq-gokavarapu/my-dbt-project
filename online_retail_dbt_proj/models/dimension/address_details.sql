WITH address_detail AS ( SELECT DISTINCT stg.ship_postal,  ci.state, stg.ship_count,
case when ci.city is null then stg.ship_city else ci.city end as city
FROM {{ ref('stg_amazon_sales') }} stg
left JOIN
{{ ref('pincodes') }} ci on stg.ship_postal = ci.pincode
)
SELECT * FROM address_detail