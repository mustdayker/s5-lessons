print('\n___________ НАЧАЛО _____________\n')

import time
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
import numpy as np
from psycopg2.extras import execute_values




nickname = 'kosarev_dmitry'
cohort = '14'
api_key = '25c27781-8fde-4b30-a22e-524044a7580f'

base_url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net' # Получаем адрес хоста. Он же api.endpoint

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-API-KEY': api_key, 
}

# _________________________________ Рестораны _________________________________

# def get_restaurants(): 
    
#     print('__________ GET RESTAURANTS _______________')
    
#     sort_field = 'id'
#     sort_direction = 'asc'
#     limit = 50
#     offset = 0
    
#     all_restaurants = []

#     while True:
        
#         get_restaurants_var = f'/restaurants?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'
        
#         response = requests.get(f'{base_url}{get_restaurants_var}', headers=headers)
#         restaurants = response.json()

#         if len(restaurants) == 0:
#             break

#         all_restaurants += restaurants
#         offset += limit

#     return all_restaurants

# rest = get_restaurants()

# print(f'Количество ресторанов: {len(rest)}')
# print(rest[0])


# __________________________________ Курьеры ____________________________________


def get_couriers(): 
    
    print('\n __________ GET couriers _______________')
    
    sort_field = 'id'
    sort_direction = 'asc'
    limit = 10
    offset = 0
    
    while True:
        conn = psycopg2.connect("host='localhost' port='15432' dbname='de' user='jovyan' password='jovyan'")
        cur = conn.cursor()
        get_couriers_var = f'/couriers?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'
        
        response = requests.get(f'{base_url}{get_couriers_var}', headers=headers) 
        couriers = response.json() 
        
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
        for i in range(len(couriers)):
            values_dict = {
                    "courier_id": couriers[i]["_id"],
                    "courier_name": couriers[i]["name"]
                    }

            cur.execute(sql_script, values_dict)
        
        conn.commit()
        offset += limit

    return 300

cour = get_couriers()

print(f'КУРЬЕРЫ: {cour}')


# __________________________________ Доставки ____________________________________





def get_deliveries(): 
    
    print('\n __________ GET deliveries _______________')
    
    sort_field = 'id'
    sort_direction = 'asc'
    limit = 50
    offset = 0
    

    today = datetime.today()
    r_today = datetime(today.year, today.month, today.day)
    date_from = r_today - timedelta(days=7)
    date_to = r_today - timedelta(days=1)

    while True:
        conn = psycopg2.connect("host='localhost' port='15432' dbname='de' user='jovyan' password='jovyan'")
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
                    "object_value": json.dumps(deliveries[i])
                    }

            cur.execute(sql_script, values_dict)
        
        conn.commit()
        offset += limit

    return 300

deliver = get_deliveries()

print(f'ДОСТАВКИ: {deliver}')
print()

