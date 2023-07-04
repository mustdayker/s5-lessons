CREATE TABLE IF NOT EXISTS stg.ordersystem_orders(
    id           serial       NOT NULL,
    object_id    varchar(512) NOT NULL,
    object_value TEXT         NOT NULL,
    update_ts    timestamp    NOT NULL,
    CONSTRAINT ordersystem_orders_pkey PRIMARY KEY (id)
);