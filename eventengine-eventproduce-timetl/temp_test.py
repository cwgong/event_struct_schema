# -*- coding: utf-8 -*-
import json
import requests
import logging.config

logging.config.fileConfig("logging.conf")
logger = logging.getLogger('main')

list_url = "http://eventengine-persistent-operator:31001/info/cluster"
list_url = 'http://information-doc-service:31001/info/schema'
try:
    params = dict()
    params["cp"] = 1
    params["ps"] = 10
    # params["timeField"] = 'publishAt'
    params["page"] = False
    params['timeField'] = 'creatAt'
    # params["startAt"] = start_time
    # params["endAt"] = end_time

    r = requests.get(list_url, params)
    # print(r.json())
    result_json = r.json()
    #
    list_data = result_json['data']
    aa = list_data['list']
    for item in aa:
        schema = item['schema']
        print(item)

except Exception as e:
    logger.exception("Exception: {}".format(e))