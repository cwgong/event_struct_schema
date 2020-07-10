# -*- coding: utf-8 -*-

import json
import requests
import logging
from singleton import singleton
from ltp_class import HIT_LTP

@singleton
class Opinion_Supervision():

    def __init__(self):
        self.features = ["表示","认为","称","指出","说","强调","看来","介绍","点评","预计","提出","提到","分析"]
        self.opinioners = ['资深专家及从业人士','证券部人士', '投行人士', '投行业内人士', '创投界人士', '市场分析人士','私募分析人士','专家','资深专家','从业人士','业内人士']
        self.sort_opinioners = sorted(self.opinioners, key=lambda x:len(x), reverse = True)
        # 工具类
        MODELDIR = 'ltp_data_v3.4.0'
        self.hit_ltp = HIT_LTP(MODELDIR)
        logging.info("Opinion_Supervision initial ... ")
        
    def contain_features(self, s):
        if len(s) > 15:
            for fea in self.features:
                if fea in s:
                    return True 
        return False
    
    def split_sentence(self, sen):
        nlp_url = 'http://hanlp-nlp-service:31001/hanlp/segment/segment'
        try:
            cut_sen = dict()
            cut_sen['content'] = sen
            cut_sen['customDicEnable'] = True
            data = json.dumps(cut_sen).encode("UTF-8")
            cut_response = requests.post(nlp_url, data=data, headers={'Connection':'close'})
            cut_response_json = cut_response.json()
            return cut_response_json['data']
        except Exception as e:
            logging.exception("Exception: {}".format(e))
            logging.exception("hanlp-nlp-service error")
            logging.exception("sentence: {}".format(sen))
            return []
        
    def get_opinion(self, s):
        
        org = ''
        position = ''
        author = ''
        opinion = ''
        
        terms = self.split_sentence(s)
        core_words_info = self.hit_ltp.get_core_words(s)
        
        offe = 0
        for core_word_info in core_words_info:
            if core_word_info['word'] in self.features:
                offe = core_word_info['offe']
                opinion = s
                break
        
        for term in terms:
            if term['offset'] < offe:
                if 'nr' in term['ner'] or 'rw' in term['ner'] or 'nr' in term['nature']:
                    author = term['word']
                    break
                
        for term in terms:
            if term['offset'] < offe:
                if 'zqgs' in term['ner']:
                    author = term['word']
                    break
                
        if author == '' and offe != 0:
            s_ = s[0: offe]
            for opinioner in self.sort_opinioners:
                if opinioner in s_:
                    author = opinioner
                    break
                 
        return org, position, author, opinion
    
        

if __name__ == '__main__':
    
    s = '刘辰涵表示，新增额度最早在2020年1月份才可以发行、拨付、使用见效，且年内三季度即将今年的专项债额度全部发行完毕，故四季度可能不会有专项债发行供给；其次，由于四季度已经提前一个季度进行下一年度的专项债筹备，基建和社融有望年初发力，进而有利于明年开年经济运行。'
    
    opinion_supervison = Opinion_Supervision()
    
    exist = opinion_supervison.contain_features(s)
    print('exist: ', exist)
    
    org, position, author, opinion = opinion_supervison.get_opinion(s)     
    
    print('org: ', org)
    print('position: ', position)
    print('author: ', author)
    print('opinion: ', opinion)
                                
             
    
    
    