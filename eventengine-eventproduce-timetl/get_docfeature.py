# -*- coding: utf-8 -*-

import io
import codecs
import json
from abstract_class import Abstract_Supervision
import logging.config

logging.config.fileConfig("logging.conf")
logger = logging.getLogger('main')

def get_doc_abstract(data_file, data_file_):
    
    abstract_supervison = Abstract_Supervision()
    c = 0
    c1 = 0
    f = codecs.open(data_file_, 'w', encoding='utf-8')
    with io.open(data_file, "r", encoding='utf-8') as f1:
        while True:
            line = f1.readline()
            if len(line) > 0:
                json_data = json.loads(line)
                if 'title' in json_data and 'content' in json_data and 'dataSource' in json_data:
                    
                    c += 1
                    title = json_data['title'].strip()
                    content = json_data['content']
                    dataSource = json_data['dataSource']
                    # 找摘要
                    paragraphs = abstract_supervison.split(content, dataSource)
                    abstract = abstract_supervison.get_abstract(paragraphs)
                    if abstract != '':
                        json_data['content'] = abstract
                        f.write(json.dumps(json_data, ensure_ascii=False) + "\n")
                        c1 += 1
                    else:
                        logger.info("no abstract: {}".format(json_data['id'] + ' ' + title))
                    
            else:
                break
    f.close()
    logger.info("doc: {}".format(c))
    logger.info("doc with abstract: {}".format(c1))
    
    return data_file_


























