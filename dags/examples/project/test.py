print('___________ НАЧАЛО _____________')

import time
import requests
import json
import pandas as pd
from datetime import datetime, timedelta



api_key = '5f55e6c0-e9e5-4a9c-b313-63c01fc31460'
base_url = 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net' # Получаем адрес хоста. Он же api.endpoint
postgres_conn_id = 'postgresql_de' # Помещаем в переменную название соединения Postgres из Admin → Connections
nickname = 'kosarev_dmitry' # Поместили в переменные личные данные
cohort = '14'

# Словарь заголовоков для /generate_report и /get_report и /get_increment
headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key, # Получили его из поля Extra в Admin → Connections
    'Content-Type': 'application/x-www-form-urlencoded' # ВОПРОС - Разобраться что это такое
}

# ___________ Функция на запрос отчета ___________
def generate_report(): 
    print('__________ Making request generate_report _______________')
    response = requests.post(f'{base_url}/generate_report', headers=headers) # base_url это адрес хоста
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id'] # Получаем task id
    # ti.xcom_push(key='task_id', value=task_id) # Пушим task_id в метод обмена ti.xcom
    print(f' _____ Response is: {response.content}') # Выводим на экран полученные данные от сервера

    return task_id

task_id = generate_report()

print(f'\nTASK ID:{task_id} \n')

def get_report(task_id): 
    print('__________ Making request get_report __________')
    task_id = task_id # Тянем task_id из ti.xcom
    print(f'_____ Принятый task_id: {task_id}')
    report_id = None # Сбрасываем значение переменной report_id в значение None

    for i in range(20): # 20 попыток
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers) 
        # Делаем запрос на статус отчета. Синтаксис стандартный
        response.raise_for_status() # Проверяем код состояния HTTP развернуто см выше
        print(f'_____ Response is {response.content}') # Выводим весь полученный код состояния отчета
        status = json.loads(response.content)['status'] # Получаем значние поля status
        if status == 'SUCCESS': # Если статус збс, тогда считываем report_id из полученного JSON
            report_id = json.loads(response.content)['data']['report_id'] # Тута
            break # Прерываем цикл
        else:
            time.sleep(10) # Иначе тупим еще 10 секунд

    if not report_id: # Если report_id так и остался None
        raise TimeoutError() # то делаем так, чтобы код упал

    # ti.xcom_push(key='report_id', value=report_id) # Пушим report_id методом ti.xcom
    print(f'_____ Полученный Report_id: {report_id}')

    return report_id

report_id = get_report(task_id)

print(f'\n REPORT ID:{report_id} \n')