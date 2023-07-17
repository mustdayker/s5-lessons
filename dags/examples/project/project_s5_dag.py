from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.models.variable import Variable

import time
import json
import numpy as np
import requests
import psycopg2
import pandas as pd

from datetime import datetime, timedelta
from psycopg2.extras import execute_values


# Параметры подключения к БД Postgres
PG_CONN_ID = "PG_WAREHOUSE_CONNECTION"

nickname = Variable.get("NICKNAME") # 'kosarev_dmitry'
cohort = Variable.get("COHORT") # '14'
api_key = Variable.get("API_KEY") # '25c27781-8fde-4b30-a22e-524044a7580f'
base_url = Variable.get("BASE_URL") # 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net' 

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-API-KEY': api_key, 
}


# __________________________________ Курьеры ____________________________________
# Функция забирает данные о курьерах по API

def get_couriers(): 
    
    print('\n __________ GET couriers _______________')
    
    sort_field = 'id'
    sort_direction = 'asc'
    limit = 10
    offset = 0
    
    # Запускаем бесконечный цикл, который перебирает данные из источника по {limit} строк, пока они не закончатся
    while True:
        # conn = psycopg2.connect("host='localhost' port='15432' dbname='de' user='jovyan' password='jovyan'")
        pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        # Необходимые параметры упаковываем в переменную
        get_couriers_var = f'/couriers?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'
        
        response = requests.get(f'{base_url}{get_couriers_var}', headers=headers) 
        couriers = response.json() 
        
        # Если даные в истонички закончилсиь выходим из цикла
        if len(couriers) == 0:
            conn.commit()
            cur.close()
            conn.close()
            break

        
        sql_script = """
        INSERT INTO stg.st_couriers(courier_id, courier_name)
        VALUES (%(courier_id)s, %(courier_name)s)
        ON CONFLICT (courier_id) DO UPDATE
        SET courier_name = EXCLUDED.courier_name
        ;
        """
        
        # Циклом перебираем все элементы из полученного массива и рассовываем их по нужным полям
        for i in range(len(couriers)):
            values_dict = {
                    "courier_id": couriers[i]["_id"],
                    "courier_name": couriers[i]["name"]
                    }

            cur.execute(sql_script, values_dict)
        
        conn.commit()
        offset += limit

    return 300


# __________________________________ Доставки ____________________________________
# Функция забирает данные о доставках по API

def get_deliveries(ds): 
    
    print('\n __________ GET deliveries _______________')
    
    sort_field = 'id'
    sort_direction = 'asc'
    limit = 50
    offset = 0
    
    
    
    # Забираем данные за последние 7 дней / Реализация внутри функции
    # today = datetime.today()
    # r_today = datetime(today.year, today.month, today.day)
    
    # date_from = r_today - timedelta(days=7)
    # date_to = r_today - timedelta(days=1)

    # Реализация с помощью контекста DAG
    print("Дата запуска:", ds)

    run_day = datetime.strptime(ds, "%Y-%m-%d")

    date_from = run_day - timedelta(days=1)
    date_to = run_day


    while True:
        # conn = psycopg2.connect("host='localhost' port='15432' dbname='de' user='jovyan' password='jovyan'")
        pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        get_deliveries_var = f'/deliveries?&from={date_from}&to={date_to}&sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'
        response = requests.get(f'{base_url}{get_deliveries_var}', headers=headers) 
        
        deliveries = response.json()

        if len(deliveries) == 0:
            conn.commit()
            cur.close()
            conn.close()
            break

        sql_script = """
        INSERT INTO stg.st_delivers(delivery_ts, delivery_id, object_value)
        VALUES (%(delivery_ts)s, %(delivery_id)s, %(object_value)s)
        ON CONFLICT (delivery_id) DO UPDATE
        SET 
            delivery_ts = EXCLUDED.delivery_ts,
            object_value = EXCLUDED.object_value
        ;
        """

        for i in range(len(deliveries)):
            values_dict = {
                    "delivery_ts": deliveries[i]["delivery_ts"],
                    "delivery_id": deliveries[i]["delivery_id"],
                    "object_value": json.dumps(deliveries[i]) # Упаковываем строковые данные в JSON
                    }

            cur.execute(sql_script, values_dict)
        
        conn.commit()
        offset += limit

    return 300


with DAG(
    "project_s5_v1", 
    start_date=datetime(2023, 7, 10), 
    schedule_interval='@daily',
    catchup=True
    ) as dag:
    
    # Таска забирает данные о курьерах по API 
    # и кладет их в stg.st_couriers (courier_id, courier_name)
    get_courier = PythonOperator(
        task_id="get_couriers",
        python_callable=get_couriers
    )

    # Таска забирает данные о достиавках по API 
    # и кладет их в stg.st_delivers (delivery_ts, delivery_id, object_value)
    get_deliver = PythonOperator(
        task_id="get_deliveries",
        python_callable=get_deliveries
    )

    # Таска заполняет таблицу dds.dm_couriers (courier_id, courier_name)
    # из таблицы              stg.st_couriers (courier_id, courier_name)
    dds_couriers = PostgresOperator(
        task_id='fill_dds_couriers',
        postgres_conn_id=PG_CONN_ID,
        sql="sql_scripts/fill_dds_couriers_script.sql"
    )

    # Таска заполняет таблицу dds.dm_delivers (order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, "sum", tip_sum)
    # из таблицы              stg.st_delivers (delivery_ts, delivery_id, object_value)  
    dds_delivers = PostgresOperator(
        task_id='fill_dds_delivers',
        postgres_conn_id=PG_CONN_ID,
        sql="sql_scripts/fill_dds_delivers_script.sql"
    )
    
    # Таска заполняет витрину cdm.dm_courier_ledger (
    # 	courier_id,
    # 	courier_name,
    # 	settlement_year,
    # 	settlement_month,
    # 	orders_count,
    # 	orders_total_sum,
    # 	rate_avg,
    # 	order_processing_fee,
    # 	courier_order_sum,
    # 	courier_tips_sum,
    # 	courier_reward_sum
    # )
    # Используя данные из dds.dm_couriers и dds.dm_delivers
    cdm_couriers = PostgresOperator(
        task_id='fill_cdm_couriers',
        postgres_conn_id=PG_CONN_ID,
        sql="sql_scripts/fill_cdm_couriers_script.sql"
    )
 
[get_courier, get_deliver] >> dds_couriers >> dds_delivers >> cdm_couriers