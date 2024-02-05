import argparse
import requests
from benzinga import news_data
import json
from constants import BENZINGA_KEY
from db_helper_functions import *


def get_news_by_topic(page, topic):
    url = "https://api.benzinga.com/api/v2/news"

    querystring = {"token":{BENZINGA_KEY},"topics":{topic}, "pageSize":"100", "page":{page}, "dateFrom":"2019-01-04","dateTo":"2023-01-04"}
    headers = {"accept": "application/json"}

    response = requests.request("GET", url, headers=headers, params=querystring)

    json_response = response.json()
    print(json.dumps(json_response, indent=2))

print(len(get_stock_news_from_db('XOM')))
