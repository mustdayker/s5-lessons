print('\n___________ НАЧАЛО _____________\n')

import time
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
import numpy as np


# __________________________________ Курьеры ____________________________________


def dds_couriers(): 
    
    print('\n __________ FILL couriers _______________')
    
    conn = psycopg2.connect("host='localhost' port='15432' dbname='de' user='jovyan' password='jovyan'")
    cur = conn.cursor()
    
    sql_script = """
    INSERT INTO dds.dm_couriers (courier_id, courier_name)
    SELECT 
        courier_id,
        courier_name
    FROM stg.st_couriers AS sc
    ON CONFLICT (courier_id) DO UPDATE
    SET courier_name = EXCLUDED.courier_name
    ;
    """

    cur.execute(sql_script)
    
    conn.commit()
    cur.close()
    conn.close()

    return 300

cour = dds_couriers()

print(f'КУРЬЕРЫ: {cour}')


# __________________________________ Доставки ____________________________________





def dds_deliveries(): 
    
    print('\n __________ FILL deliveries _______________')
    
    conn = psycopg2.connect("host='localhost' port='15432' dbname='de' user='jovyan' password='jovyan'")
    cur = conn.cursor()
    
    sql_script = """
    INSERT INTO dds.dm_delivers (
            order_id,
            order_ts,
            delivery_id,
            courier_id,
            address,
            delivery_ts,
            rate,
            "sum",
            tip_sum
        )
    SELECT
        object_value::JSON->>'order_id'                 AS order_id,
        (object_value::JSON->>'order_ts')::timestamp    AS order_ts,
        delivery_id                                     AS delivery_id,
        object_value::JSON->>'courier_id'               AS courier_id,
        object_value::JSON->>'address'                  AS address,
        delivery_ts::timestamp                          AS delivery_ts,
        (object_value::JSON->>'rate')::int              AS rate,
        (object_value::JSON->>'sum')::numeric(14,2)     AS "sum",
        (object_value::JSON->>'tip_sum')::numeric(14,2) AS tip_sum
    FROM stg.st_delivers
    ON CONFLICT (delivery_id) DO UPDATE
    SET
        order_id    = EXCLUDED.order_id,
        order_ts    = EXCLUDED.order_ts,
        courier_id  = EXCLUDED.courier_id,
        address     = EXCLUDED.address,
        delivery_ts = EXCLUDED.delivery_ts,
        rate        = EXCLUDED.rate,
        "sum"       = EXCLUDED."sum",
        tip_sum     = EXCLUDED.tip_sum
    ;
    """

    cur.execute(sql_script)
    
    conn.commit()
    cur.close()
    conn.close()

    return 300

deliver = dds_deliveries()

print(f'ДОСТАВКИ: {deliver}')
print()

