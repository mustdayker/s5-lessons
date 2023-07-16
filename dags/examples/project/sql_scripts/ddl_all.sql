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
	courier_reward_sum   numeric(14,2),
	-- для исключения дублей добавляем ограничения на 3 поля сразу
	CONSTRAINT unique_constraint UNIQUE (courier_id, settlement_year, settlement_month)
);

----------------------------------------------

DROP TABLE IF EXISTS dds.dm_couriers; 
CREATE TABLE IF NOT EXISTS dds.dm_couriers (
	id           serial,
	courier_id   varchar NOT NULL UNIQUE,
	courier_name varchar NOT NULL
);

----------------------------------------------

DROP TABLE IF EXISTS dds.dm_delivers; 
CREATE TABLE IF NOT EXISTS dds.dm_delivers (
	id          serial,
	order_id    varchar   NOT NULL, 
	order_ts    timestamp NOT NULL, 
	delivery_id varchar   NOT NULL UNIQUE, 
	courier_id  varchar   NOT NULL, 
	address     varchar, 
	delivery_ts timestamp NOT NULL, 
	rate        int,
	"sum"       numeric(14,2),
	tip_sum     numeric(14,2)
);

--------------------------------------------------

DROP TABLE IF EXISTS stg.st_delivers; 
CREATE TABLE IF NOT EXISTS stg.st_delivers (
	id           serial,
	delivery_ts  timestamp NOT NULL, 
	delivery_id  varchar   NOT NULL UNIQUE, 
	object_value TEXT     NOT NULL
);


DROP TABLE IF EXISTS stg.st_couriers; 
CREATE TABLE IF NOT EXISTS stg.st_couriers (
	id           serial,
	courier_id   varchar NOT NULL UNIQUE,
	courier_name varchar NOT NULL
);

