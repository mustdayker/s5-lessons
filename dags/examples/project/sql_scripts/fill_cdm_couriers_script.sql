INSERT INTO cdm.dm_courier_ledger (
	courier_id,
	courier_name,
	settlement_year,
	settlement_month,
	orders_count,
	orders_total_sum,
	rate_avg,
	order_processing_fee,
	courier_order_sum,
	courier_tips_sum,
	courier_reward_sum
)
WITH rewards_final AS (
WITH rewards_cte AS (
	SELECT 
		courier_id,
		order_id,
		EXTRACT(YEAR  FROM order_ts)::int AS "year",
		EXTRACT(MONTH FROM order_ts)::int AS "month",
		rate,
		AVG(rate) OVER(PARTITION BY courier_id, EXTRACT(YEAR FROM order_ts), EXTRACT(MONTH FROM order_ts))::float AS rate_avg_month,
		"sum",
		tip_sum,
		CASE 
			WHEN (AVG(rate) OVER(PARTITION BY courier_id, EXTRACT(YEAR FROM order_ts), EXTRACT(MONTH FROM order_ts))::float) < 4 
			THEN 0.05
			WHEN (AVG(rate) OVER(PARTITION BY courier_id, EXTRACT(YEAR FROM order_ts), EXTRACT(MONTH FROM order_ts))::float) >= 4 
			 AND (AVG(rate) OVER(PARTITION BY courier_id, EXTRACT(YEAR FROM order_ts), EXTRACT(MONTH FROM order_ts))::float) < 4.5 
			THEN 0.07
			WHEN (AVG(rate) OVER(PARTITION BY courier_id, EXTRACT(YEAR FROM order_ts), EXTRACT(MONTH FROM order_ts))::float) >= 4.5 
			 AND (AVG(rate) OVER(PARTITION BY courier_id, EXTRACT(YEAR FROM order_ts), EXTRACT(MONTH FROM order_ts))::float) < 4.9 
			THEN 0.07
			WHEN (AVG(rate) OVER(PARTITION BY courier_id, EXTRACT(YEAR FROM order_ts), EXTRACT(MONTH FROM order_ts))::float) >= 4.9 
			THEN 0.1
		END AS reward_part	
	FROM dds.dm_delivers AS dd
	ORDER BY courier_id
)
SELECT 
	courier_id,
	"year",
	"month",
	rate,
	rate_avg_month,
	"sum",
	tip_sum,
	reward_part,	
	COALESCE ((CASE
		WHEN reward_part = 0.05 AND ("sum" * reward_part) < 100 THEN 100
		WHEN reward_part = 0.07 AND ("sum" * reward_part) < 150 THEN 150 
		WHEN reward_part = 0.08 AND ("sum" * reward_part) < 175 THEN 175
		WHEN reward_part = 0.1  AND ("sum" * reward_part) < 200 THEN 200
	END), 0) AS reward_compensation,
	COALESCE ((CASE
		WHEN reward_part = 0.05 AND ("sum" * reward_part) >= 100 THEN ("sum" * reward_part)
		WHEN reward_part = 0.07 AND ("sum" * reward_part) >= 150 THEN ("sum" * reward_part) 
		WHEN reward_part = 0.08 AND ("sum" * reward_part) >= 175 THEN ("sum" * reward_part)
		WHEN reward_part = 0.1  AND ("sum" * reward_part) >= 200 THEN ("sum" * reward_part)
	END), 0) AS reward_sum
FROM rewards_cte AS rcte
)
SELECT 
	rf.courier_id          AS courier_id,
	MAX(dc.courier_name)   AS courier_name,
	rf."year"              AS settlement_year,
	rf."month"             AS settlement_month,
	COUNT(rf.courier_id)   AS orders_count,
	SUM(rf."sum")          AS orders_total_sum,
	MAX(rf.rate_avg_month) AS rate_avg,
	SUM(rf."sum" * 0.25)   AS order_processing_fee,
	SUM(reward_compensation + reward_sum)  AS courier_order_sum,
	SUM(tip_sum)                           AS courier_tips_sum,
	SUM((reward_compensation + reward_sum + tip_sum) * 0.95) AS courier_reward_sum
FROM rewards_final AS rf
JOIN dds.dm_couriers AS dc ON rf.courier_id = dc.courier_id
GROUP BY rf.courier_id, rf."year", rf."month"
ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
SET 
    courier_name         = EXCLUDED.courier_name,
    orders_count         = EXCLUDED.orders_count,
    orders_total_sum     = EXCLUDED.orders_total_sum,
    rate_avg             = EXCLUDED.rate_avg,
    order_processing_fee = EXCLUDED.order_processing_fee,
    courier_order_sum    = EXCLUDED.courier_order_sum,
    courier_tips_sum     = EXCLUDED.courier_tips_sum,
    courier_reward_sum   = EXCLUDED.courier_reward_sum
;