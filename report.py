import requests
import datetime
import time
import csv
import threading
import json


_TOKEN = '9779d42d-0729-4968-997c-6af4223bfe86'
_URL = 'https://analytics.maximum-auto.ru/vacancy-test/api/v0.1/reports'
_BEARER_TOKEN = {'Authorization': 'Bearer {}'.format(_TOKEN)}
_REPORTS = [] # Список с id и временем создания отчетов
_THREADS_LST = [] # Список threads


def create_report(id) -> int:  # Функция создание отчета
    while True:
        data = {'id': str(id)}
        response = requests.post(_URL, headers = _BEARER_TOKEN, data = json.dumps(data))
        if response.status_code == 201:
            report = {'id': id,'creation_time': datetime.datetime.now()}
            _REPORTS.append(report)
            return(response.status_code, id)
        if response.status_code == 409:
            id += 1


def get_report(id) -> None: # Функция запроса отчетов(запускается через threading)
    while True:
        response = requests.get('{0}/{1}'.format(_URL, id), headers = _BEARER_TOKEN)
        if response.status_code == 200:
            with open('reports.csv', 'a') as csvfile:
                file_writer = csv.writer(csvfile, delimiter = ";")
                for rep in _REPORTS: # Поиск по id нужного отчета в списке
                    if rep['id'] == id:
                        file_writer.writerow((rep['creation_time'].timestamp(), json.loads(response.text)['value']))
                        return
        time.sleep(1)


def main() -> None:
    id = 0
    with open('reports.csv', 'w') as csvfile:  #Создание CSV файла reports.csv с заголовками
        file_writer = csv.writer(csvfile, delimiter = ";")
        file_writer.writerow(('date_time', 'value'))
    
    while True:
        status_code, id = create_report(id)
        if status_code == 201:
            time.sleep(30)
            _THREADS_LST.append(threading.Thread(target = get_report, args = (id,))) # Создание нового thread для запроса отчета
            _THREADS_LST[len(_THREADS_LST) - 1].start()
        time.sleep(30)
        id += 1


if __name__ == '__main__':
    main()