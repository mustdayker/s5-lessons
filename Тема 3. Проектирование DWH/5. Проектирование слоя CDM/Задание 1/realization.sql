CREATE TABLE IF NOT EXISTS cdm.dm_settlement_report(
    id                  serial NOT NULL,
    restaurant_id       varchar(512) NOT NULL,
    restaurant_name     varchar(1024) NOT NULL,
    settlement_date     date NOT NULL,
    orders_count        int NOT NULL,
    orders_total_sum    NUMERIC(14,2) NOT NULL,
    orders_bonus_payment_sum NUMERIC(14,2) NOT NULL,
    orders_bonus_granted_sum NUMERIC(14,2) NOT NULL,
    order_processing_fee     NUMERIC(14,2) NOT NULL,
    restaurant_reward_sum    NUMERIC(14,2) NOT NULL
);