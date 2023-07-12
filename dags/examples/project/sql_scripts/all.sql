DROP TABLE IF EXISTS cdm.dm_courier_ledger; 
CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
	id                   serial,
	courier_id           varchar,
	courier_name         varchar,
	settlement_year      int,
	settlement_month     int,
	orders_count         int,
	orders_total_sum     numeric(14,2),
	rate_avg             numeric(14,2),
	order_processing_fee numeric(14,2),
	courier_order_sum    numeric(14,2),
	courier_tips_sum     numeric(14,2),
	courier_reward_sum   numeric(14,2)
);

----------------------------------------------

DROP TABLE IF EXISTS dds.dm_couriers; 
CREATE TABLE IF NOT EXISTS dds.dm_couriers (
	id                   serial,
	courier_id           varchar,
	courier_name         varchar
);

----------------------------------------------

DROP TABLE IF EXISTS dds.dm_delivers; 
CREATE TABLE IF NOT EXISTS dds.dm_delivers (
	id          serial,
	order_id    varchar, 
	order_ts    timestamp, 
	delivery_id varchar, 
	courier_id  varchar, 
	address     varchar, 
	delivery_ts timestamp, 
	rate        int,
	"sum"       numeric(14,2),
	tip_sum     numeric(14,2)
);

--------------------------------------------------

DROP TABLE IF EXISTS stg.st_delivers; 
CREATE TABLE IF NOT EXISTS stg.st_delivers (
	id          serial,
	delivery_ts timestamp, 
	delivery_id varchar, 
	object_value text
);


DROP TABLE IF EXISTS stg.st_couriers; 
CREATE TABLE IF NOT EXISTS stg.st_couriers (
	id                   serial,
	courier_id           varchar,
	courier_name         varchar
);























