INSERT INTO cdm.dm_courier_ledger (
	courier_id,
	courier_name,
	settlement_year,
	settlement_month,
	orders_count, -- общее количнство заказов у курьера в отчетный месяц
	orders_total_sum, -- общая сумма заказов у курьера в отчетный месяц
	rate_avg, -- средний рейтинг курьера в отчетный месяц
	order_processing_fee, -- сумма, удержанная компанией за обработку заказов
	courier_order_sum, -- общая сумма, которая положена курьеру за доставленные им заказы
	courier_tips_sum, -- общая сумма чаевых
	courier_reward_sum -- итоговая сумма, которую необходимо перечислить курьеру за отчетный месяц
)
-- Значит, план такой:
WITH rewards_final AS ( -- Это общий CTE, который использует подготовленные данные из CTE "rewards_cte"
WITH rewards_cte AS ( -- Промежуточный CTE, который построчно обогатит данные
	SELECT            -- которые будут использованы для последующих вычислений
		courier_id,
		order_id,
		EXTRACT(YEAR  FROM order_ts)::int AS "year", -- Год заказа
		EXTRACT(MONTH FROM order_ts)::int AS "month", -- Месяц заказа
		rate, -- Рейтинг полученный курьером за конкретный заказ
		-- Средний рейтинг полученный курьером в отчетный месяц:
		AVG(rate) OVER(PARTITION BY courier_id, EXTRACT(YEAR FROM order_ts), EXTRACT(MONTH FROM order_ts))::float AS rate_avg_month,
		"sum", -- Сумма конкретного заказа
		tip_sum, -- Чаевые за конкретный заказ
		-- Далее получаем процент, который получит курьер от конкретного заказа
		-- Используя средний рейтинг курьера, который у него сформировался в течении месяца
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
		END AS reward_part	-- Обзываем это поле reward_part
	FROM dds.dm_delivers AS dd
	ORDER BY courier_id
)
--
-- Далее делаем запрос к данным подготовленным CTE "rewards_cte"
-- Данные не агрегируются, а выводятся так же построчно
-- Тут мы дополнительно обогощаем данные суммой, 
-- которую получит курьер за конкретный заказ
SELECT 
	courier_id, -- ID курьера, берем напрямую из CTE "rewards_cte"
	"year",     -- Год заказа, берем напрямую из CTE "rewards_cte"
	"month",    -- Месяц заказа, берем напрямую из CTE "rewards_cte"
	rate,       -- Рейтинг, полученный за конкретный заказ, берем напрямую из CTE "rewards_cte"
	rate_avg_month, -- Срейдний рейтинг курьера за отчетный месяц, берем напрямую из CTE "rewards_cte"
	"sum", 		-- Сумма за конкретный заказ, берем напрямую из CTE "rewards_cte"
	tip_sum, 	-- Чаевые за конкретный заказ, берем напрямую из CTE "rewards_cte"
	reward_part, -- Процент, который получит курьер от конкретного заказа
	             --	Этот процент нам подготовил CTE "rewards_cte"
	--
	-- Далее мы обогощаем наши данные суммой
	-- которая положена курьеру за конкретный заказ
	-- используя логику, что сумма не может быть меньше определенного уровня
	-- в зависимости от рейтинга курьера
	-- Эти данные мы разобьем по двум полям и в последствии сложим вместе
	--
	-- Первое поле это компенсация за заказ если сумма процента оказалась ниже пороговой
	COALESCE ((CASE
		WHEN reward_part = 0.05 AND ("sum" * reward_part) < 100 THEN 100
		WHEN reward_part = 0.07 AND ("sum" * reward_part) < 150 THEN 150 
		WHEN reward_part = 0.08 AND ("sum" * reward_part) < 175 THEN 175
		WHEN reward_part = 0.1  AND ("sum" * reward_part) < 200 THEN 200
	END), 0) AS reward_compensation, 
	-- Второе поле это процент заказа, если сумма выше пороговой
	COALESCE ((CASE
		WHEN reward_part = 0.05 AND ("sum" * reward_part) >= 100 THEN ("sum" * reward_part)
		WHEN reward_part = 0.07 AND ("sum" * reward_part) >= 150 THEN ("sum" * reward_part) 
		WHEN reward_part = 0.08 AND ("sum" * reward_part) >= 175 THEN ("sum" * reward_part)
		WHEN reward_part = 0.1  AND ("sum" * reward_part) >= 200 THEN ("sum" * reward_part)
	END), 0) AS reward_sum
FROM rewards_cte AS rcte
)
-- Теперь формируем итоговый запрос с агрегацией данных
-- используя данные подготовленные CTE "rewards_final"
SELECT 
	rf.courier_id          AS courier_id,       -- ID курьера: группируемое поле
	MAX(dc.courier_name)   AS courier_name,     -- Имя курьера: одинаковое по всем строкам, берем любое, например MAX
	rf."year"              AS settlement_year,  -- Отчетный год: группируемое поле
	rf."month"             AS settlement_month, -- Отчетный месяц: группируемое поле
	COUNT(rf.courier_id)   AS orders_count,     -- Количество заказов за отчетный месяц
	SUM(rf."sum")          AS orders_total_sum, -- Сумма заказов за отчетный месяц
	MAX(rf.rate_avg_month) AS rate_avg,         -- Средний рейтин, одинаковый по всем строкам, берем любое из значений, например MAX
	SUM(rf."sum" * 0.25)   AS order_processing_fee, -- сумма, удержанная компанией за обработку заказов
	SUM(reward_compensation + reward_sum)  AS courier_order_sum, -- общая сумма, которая положена курьеру за доставленные им заказы
	SUM(tip_sum)                           AS courier_tips_sum,  -- общая сумма чаевых
	SUM((reward_compensation + reward_sum + tip_sum) * 0.95) AS courier_reward_sum -- итоговая сумма, которую необходимо перечислить курьеру
FROM rewards_final AS rf
JOIN dds.dm_couriers AS dc ON rf.courier_id = dc.courier_id -- Джойним таблицу курьеров чтобы забрать имя
GROUP BY rf.courier_id, rf."year", rf."month" -- Для уникальности данных Группируем по 3 полям
ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE -- Чтобы не возникало дублей обновляем данные при конфликте уникальности
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