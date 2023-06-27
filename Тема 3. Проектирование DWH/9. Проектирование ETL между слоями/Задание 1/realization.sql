INSERT INTO cdm.dm_settlement_report (
	restaurant_id,
	restaurant_name,
	settlement_date,
	orders_count,
	orders_total_sum,
	orders_bonus_payment_sum,
	orders_bonus_granted_sum,
	order_processing_fee,
	restaurant_reward_sum
)
WITH 
count_orders AS (
	SELECT 
		dm_o.id              AS order_id,
		dm_o.restaurant_id   AS restaurant_id,
		dm_r.restaurant_name AS restaurant_name,
		dm_t."date"          AS settlement_date,
		COUNT(*) OVER (PARTITION BY dm_o.restaurant_id, dm_t."date") AS orders_count
	FROM dds.dm_orders AS dm_o
	LEFT JOIN dds.dm_timestamps  AS dm_t ON dm_o.timestamp_id  = dm_t.id
	LEFT JOIN dds.dm_restaurants AS dm_r ON dm_o.restaurant_id = dm_r.id
	WHERE dm_o.order_status = 'CLOSED'
)
SELECT 
	co.restaurant_id          AS restaurant_id,
	co.restaurant_name        AS restaurant_name,
	co.settlement_date        AS settlement_date,
	max(co.orders_count)      AS orders_count,
	sum(fps.total_sum)        AS orders_total_sum,
	sum(fps.bonus_payment)    AS bonus_payment_sum,
	sum(fps.bonus_grant)      AS bonus_granted_sum,
	sum(fps.total_sum * 0.25) AS order_processing_fee,
	sum(fps.total_sum - (fps.total_sum * 0.25) - fps.bonus_payment) AS restaurant_reward_sum
FROM dds.fct_product_sales AS fps 
LEFT JOIN count_orders AS co ON fps.order_id = co.order_id
GROUP BY co.restaurant_id, co.restaurant_name, co.settlement_date
ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
SET
    orders_count             = EXCLUDED.orders_count,
    orders_total_sum         = EXCLUDED.orders_total_sum,
    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
    order_processing_fee     = EXCLUDED.order_processing_fee,
    restaurant_reward_sum    = EXCLUDED.restaurant_reward_sum
;