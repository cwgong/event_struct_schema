# -*- coding: utf-8 -*-
import requests
import json

# data = {}
def requests_post(url, data):

    data = json.dumps(data).encode("UTF-8")
    
    response = requests.post(url, data = data)
    response = response.json()
    
    return response

# params = {}
def requests_get(url, params):

    response = requests.get(url, params = params)
    response = response.json()
    
    return response

def post_test():
    
    url = 'http://10.0.0.157:9981/featureExtract'
    
    doc = {'id':'121313123123',
           'title':"光弘科技独立董事梁烽先生逝世",
           'url':"www.baidu.com",
           'dataSource':"",
           'content':"<p><p>7月25日午间公告，公司董事会沉痛公告，近日从独立董事梁烽先生家属获知，梁烽先生因病不幸逝世。梁烽先生现任公司第一届董事会独立董事、审计委员会主任委员。</p></p><p><p>梁烽先生去世后，公司董事会成员减少至8人，其中独立董事减少至2人，导致公司董事会中独立董事所占比例低于1/3，根据相关法律、法规规定，公司董事会将尽快按照相关程序增补新的独立董事并及时公告。在新的独立董事选举产生之前，公司独立董事事务暂由陈汉亭先生、彭丽霞女士两位独立董事履行。</p></p><p><p>习近平指出，这是最后一个晚上</p></p>"}
    list_of_doc = [doc]
    
    results = requests_post(url, list_of_doc)
    
    for r in results:
        print(r)
    
if __name__ == "__main__":
    
    post_test()
    
    