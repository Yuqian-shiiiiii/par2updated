from sodapy import Socrata
import requests
import json
from datetime import datetime


DATA_SET = 'nc67-uf89'
DEFAULT_PAGE = 5


def get_data(app_key: str, page_size: int, page=DEFAULT_PAGE) -> list:
    if type(page_size) is not int:
        page_size = int(page_size)
    if type(page) is not int:
        page = int(page)

    client = Socrata(
        'data.cityofnewyork.us',
        app_key
    )
    result = []
    for i in range(0, page):
        try:
            r = client.get(dataset_identifier=DATA_SET, limit=page_size, offset=i*page_size)
            result += r
        except requests.exceptions.ConnectionError as err:
            raise err
        except requests.exceptions.HTTPError:
            raise Exception('Invalid app_token specified')
    return result


def file_storage(data, file):
    if data:
        with open(file, 'w', encoding='utf8') as f:
            json.dump(obj=data, fp=f, indent=4)


def format_data(data):
    result = []
    for item in data:
        try:
            item['fine_amount'] = float(item['fine_amount'])
            item['penalty_amount'] = float(item['penalty_amount'])
            item['interest_amount'] = float(item['interest_amount'])
            item['reduction_amount'] = float(item['reduction_amount'])
            item['payment_amount'] = float(item['payment_amount'])
            item['amount_due'] = float(item['amount_due'])
        except KeyError:
            pass
        try:
            item['issue_date'] = datetime.strptime(item['issue_date'], '%m/%d/%Y').date()
        except ValueError:
            continue
        result.append(item)
    return result
