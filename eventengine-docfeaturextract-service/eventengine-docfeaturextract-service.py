# -*- coding: utf-8 -*-

import sys
sys.path.append('./featureExtract')

# 自定义版本
from _version import __version__

# python
import io
import json

# tornado
import logging
import tornado.escape
import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.options
from tornado.escape import json_decode

# business logical
import datetime
import codecs
from abstract_class import Abstract_Supervision
from opinion_class import Opinion_Supervision
    
VERSION = "0.1"
   
# config
def parse_conf_file(config_file):
    
    config = {}
    with io.open(config_file, 'r', encoding='utf8') as f:
        config = json.load(f)
    return config  

class PredictHandler(tornado.web.RequestHandler):      

    def get(self):  

        try:
            logging.info("get ...")
            begin = datetime.datetime.now()
            
            list_of_doc = self.get_argument("list_of_doc")
            list_of_doc = json_decode(list_of_doc)
            logging.info("\nlist_of_doc size: ---------> " + str(len(list_of_doc)))
            
            # predict...
            self.handler_predict(list_of_doc)
            
            end = datetime.datetime.now()
            time = end - begin
            logging.info("\nget success! ------> " + "  time: " + str(time))
        except Exception as e:
            logging.exception(e)
    
    def post(self):

        try:
            logging.info("post ...")
            begin = datetime.datetime.now()
            
            body_data = json_decode(self.request.body)
            list_of_doc = body_data
            logging.info("\nlist_of_doc size: ---------> " + str(len(list_of_doc)))
            
            self.handler_predict(list_of_doc)

            end = datetime.datetime.now()
            time = end - begin
            logging.info("\nget success! ------> " + "  time: " + str(time))
        except Exception as e:
            logging.exception(e)

    
    def handler_predict(self, list_of_doc):
        
        abstract_supervison = Abstract_Supervision()
        opinion_supervison = Opinion_Supervision()
        
        result = []
        for doc in list_of_doc:
            try:
                doc_id = doc['id']
                title = doc['title']
                url = doc['url']
                logging.info('doc_id: ' + doc_id)
                logging.info('title: ' + title)
                logging.info('url: ' + url)
                
                content = doc["content"]
                dataSource = doc["dataSource"]

                paragraphs = abstract_supervison.split(content, dataSource)
                abstract = abstract_supervison.get_abstract(paragraphs)
                if abstract != '':
                    temp_dic = {}
                    temp_dic['infoId'] = doc_id
                    temp_dic['clusterType'] = "机器聚类"
                    temp_dic['featureType'] = ["abstract"]
                    temp_dic['content'] = abstract
                    result.append(temp_dic)
                    logging.info('abstract: {}'.format(abstract))
                    
                if len(paragraphs) < 100:
                    for p in paragraphs:
                        s_list = p.split("。")
                        for s in s_list:
                            exist = opinion_supervison.contain_features(s)
                            if exist:
                                org, position, author, opinion = opinion_supervison.get_opinion(s)       
                                if opinion != '':
                                    temp_dic = {}
                                    temp_dic['infoId'] = doc_id
                                    temp_dic['clusterType'] = "机器聚类"
                                    temp_dic['content'] = opinion
                                    temp_dic['org'] = org
                                    temp_dic['position'] = position
                                    temp_dic['author'] = author
                                    temp_dic['featureType'] = ["opinion"]
                                    result.append(temp_dic)
                                    logging.info('org:'+ org + ' position:' + position + ' author:' + author + ' opinion:' + opinion)

            except Exception as e:
                logging.info(e)
        
        logging.info('results size: '.format(len(result)))

        result_json = json.dumps(result, ensure_ascii=False)
        self.write(result_json) 
        logging.info('writ result json finisehd!')
       
    
class Application(tornado.web.Application):
    
    def __init__(self, config):
        handlers = [
            (config['url'], PredictHandler), 
        ]
        settings = dict(
            debug = bool(config['debug']),
        )
        tornado.web.Application.__init__(self, handlers, **settings)

        
def main(argv):
    
    if sys.version_info < (3,):
        reload(sys)
        sys.setdefaultencoding("utf-8")
   
    if VERSION != __version__:
        logging.info("version error!")
        exit(-1)
    
    if len(argv) < 2:
        print('arg error.')
        exit(-2)  
        
    config = parse_conf_file(argv[1])
    tornado.options.parse_config_file(config['log_config_file'])
    logging.info("Server Inititial ... ")
    
    abstract_supervison = Abstract_Supervision()
    opinion_supervison = Opinion_Supervision()
    app = Application(config)
    server = tornado.httpserver.HTTPServer(app)
    server.bind(config['port'])
    server.start(config['process_num'])
    logging.info("Server Inititial Success! ")
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    
    main(sys.argv)