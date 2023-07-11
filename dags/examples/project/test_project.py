print('\n___________ НАЧАЛО _____________\n')

import time
import requests
import json
import pandas as pd
from datetime import datetime, timedelta


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

def get_restaurants(): 
    
    print('__________ GET RESTAURANTS _______________')
    
    sort_field = 'id'
    sort_direction = 'asc'
    limit = 50
    offset = 0
    
    all_restaurants = []

    get_restaurants_var = f'/restaurants?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'
        
    response = requests.get(f'{base_url}{get_restaurants_var}', headers=headers) # base_url это адрес хоста
    response.raise_for_status()
    restaurants = json.loads(response.content) 
    # ti.xcom_push(key='task_id', value=task_id) # Пушим task_id в метод обмена ti.xcom
    # print(f' Response is: {response.content}') # Выводим на экран полученные данные от сервера

    all_restaurants += restaurants
    offset += 50

    while len(restaurants) > 0:
        
        get_restaurants_var = f'/restaurants?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'
        response = requests.get(f'{base_url}{get_restaurants_var}', headers=headers) # base_url это адрес хоста
        response.raise_for_status()
        restaurants = json.loads(response.content) 
        
        all_restaurants += restaurants
        offset += 50

    return all_restaurants

rest = get_restaurants()

# print('РЕСТОРАНЫ:')
print(f'Количество ресторанов: {len(rest)}')
print(rest[0])


# __________________________________ Курьеры ____________________________________


def get_couriers(): 
    
    print('\n __________ GET couriers _______________')
    
    sort_field = 'id'
    sort_direction = 'asc'
    limit = 50
    offset = 0
    
    all_couriers = []

    get_couriers_var = f'/couriers?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'
        
    response = requests.get(f'{base_url}{get_couriers_var}', headers=headers) # base_url это адрес хоста
    response.raise_for_status()
    couriers = json.loads(response.content) 
    # ti.xcom_push(key='task_id', value=task_id) # Пушим task_id в метод обмена ti.xcom
    # print(f' Response is: {response.content}') # Выводим на экран полученные данные от сервера

    all_couriers += couriers
    offset += 50

    while len(couriers) > 0:
        
        get_couriers_var = f'/couriers?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'
        response = requests.get(f'{base_url}{get_couriers_var}', headers=headers) # base_url это адрес хоста
        response.raise_for_status()
        couriers = json.loads(response.content) 
        
        all_couriers += couriers
        offset += 50

    return all_couriers

cour = get_couriers()

# print('КУРЬЕРЫ:')
print(f'Количество курьеров: {len(cour)}')
print(cour[0])


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

    all_deliveries = []

    get_deliveries_var = f'/deliveries?&from={date_from}&to={date_to}&sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'
# /deliveries?restaurant_id={{ restaurant_id }}&from={{ from }}&to={{ to }}&sort_field={{ sort_field }}&sort_direction={{ sort_direction }}&limit={{ limit }}&offset={{ limit }}'

    response = requests.get(f'{base_url}{get_deliveries_var}', headers=headers) # base_url это адрес хоста
    response.raise_for_status()
    deliveries = json.loads(response.content) 
    # ti.xcom_push(key='task_id', value=task_id) # Пушим task_id в метод обмена ti.xcom
    # print(f' Response is: {response.content}') # Выводим на экран полученные данные от сервера

    all_deliveries += deliveries
    offset += 50

    while len(deliveries) > 0:
        
        get_deliveries_var = f'/deliveries?&from={date_from}&to={date_to}&sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'
        response = requests.get(f'{base_url}{get_deliveries_var}', headers=headers) # base_url это адрес хоста
        response.raise_for_status()
        deliveries = json.loads(response.content) 
        
        all_deliveries += deliveries
        offset += 50

    return all_deliveries

deliver = get_deliveries()

# print('ДОСТАВКИ:')
print(f'Количество доставок: {len(deliver)}')
print(deliver[0])

print()

