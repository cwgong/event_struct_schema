# -*- coding: utf-8 -*-

import io
import os
import json
import requests
import codecs
import logging.config

logging.config.fileConfig("logging.conf")
logger = logging.getLogger('main')

def get_extradata_from_api(start_time, end_time, original_data_file, extradata_file):

    # list_url = 'http://information-doc-service:31001/information/search?'
    # detail_url = 'http://information-doc-service:31001/information/detail/'
    list_url = 'http://information-doc-service:31001/info/schema'
    # detail_url = ''
    
    logger.info("get_extradata_from_api original_data_file_exist: {}".format(os.path.exists(original_data_file)))
    if not os.path.exists(original_data_file):
        ids_count = 0

        f = codecs.open(original_data_file, 'w', encoding='utf-8')
        try:
            cp = 1
            while True:
                params = dict()
                params["cp"] = cp
                params["ps"] = 1000
                params["timeField"] = 'publishAt'
                params["page"] = False
                params["startAt"] = start_time
                params["endAt"] = end_time
    
                r = requests.get(list_url, params)
                result_json = r.json()

                if len(result_json['data']) == 0:
                    break

                list_data = result_json['data']
                schema_list = list_data['list']

                for item in schema_list:
                    # schema_str = ''
                    # for schema_item in item['schema']:
                        # schema_str += schema_item['name']

                    # item['schema_str'] = schema_str

                    logger.info(item)
                    f.write(json.dumps(item, ensure_ascii=False) + "\n")

                    # try:
                    #     detail_result = requests.get(detail_url + item['id'])
                    #     detail_json = detail_result.json()
                    #     title = detail_json['data']['title']
                    # except Exception as e:
                    #     logger.info("id detail error: {}".format(item))
                    #     continue
                    # logger.info(item['id'] + ' ' + title)
                    # f.write(json.dumps(detail_json['data'], ensure_ascii=False) + "\n")
                    ids_count += 1
                cp += 1
        except Exception as e:
            logger.exception("Exception: {}".format(e))
        logger.info("all_schemas: {}".format(ids_count))
        f.close()
        return original_data_file, []
    
    else:
        old_ids = []
        with io.open(original_data_file, 'r', encoding='utf-8') as f:
            while True:
                line = f.readline()
                if len(line) > 0:
                    try:
                        json_data = json.loads(line)
                    except:
                        logger.info("json load error: {}".format(line))
                        continue
                    old_ids.append(json_data['id'])
                else:
                    break
            
        logger.info("old_ids: {}".format(len(old_ids)))
        
        new_file_data = []
        extra_ids_count = 0
        ids_count = 0
        # 每次覆盖掉 extradata_file
        f = codecs.open(extradata_file, 'w', encoding='utf-8')
        try:
            cp = 1
            while True:
                params = dict()
                params["cp"] = cp
                params["ps"] = 1000
                params["timeField"] = 'publishAt'
                params["startAt"] = start_time
                params["endAt"] = end_time
    
                r = requests.get(list_url, params)
                result_json = r.json()

                if len(result_json['data']) == 0:
                    break

                list_data = result_json['data']
                schema_list = list_data['list']

                for item in schema_list:
                    # schema_str = ''
                    # for schema_item in item['schema']:
                    #     schema_str += schema_item['name']
                    #
                    # item['schema_str'] = schema_str

                    # try:
                    #     detail_result = requests.get(detail_url + item['id'])
                    #     detail_json = detail_result.json()
                    #     title = detail_json['data']['title']
                    # except Exception as e:
                    #     continue
                    if item['id'] not in old_ids:
                        logger.info(item)
                        f.write(json.dumps(item, ensure_ascii=False) + "\n")
                        extra_ids_count += 1
                    new_file_data.append(json.dumps(item, ensure_ascii=False))
                    ids_count += 1
                cp += 1
        except Exception as e:
            logger.exception("Exception: {}".format(e))
        f.close()
        logger.info("all_ids: {}".format(ids_count))
        logger.info("extra_ids: {}".format(extra_ids_count))
        return extradata_file, new_file_data
    
def update_original_data_file(new_file_data, original_data_file):
    
    count = 0
    if len(new_file_data) == 0:
        logger.info("update_original_data_file ids: {}".format(count))
        return

    # 每次覆盖掉 original_data_file
    f = codecs.open(original_data_file, 'w', encoding='utf-8')
    for item in new_file_data:
        f.write(item+'\n')
        count += 1
    f.close()
    logger.info("update_original_data_file ids: {}".format(count))


def count_data_size(text_file):
    
    c = 0
    with io.open(text_file, "r", encoding='utf-8') as f:
        while True:
            line = f.readline()
            if len(line) > 0:
                c +=1
            else:
                break
    return c