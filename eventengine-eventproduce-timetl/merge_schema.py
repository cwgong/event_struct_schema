import re
import io
import json
import logging.config

logging.config.fileConfig("logging.conf")
logger = logging.getLogger('main')

def event_merge(eventschema_file):

    opendomain_schema_sen, fixativedomain_schema_sen, schema_atc, classify_schema_dic = classify_events(eventschema_file)
    synonym_info = load_synonym('./CoreSynonym.txt')
    opendomain_clusters = []
    conference_clusters = []
    investigation_clusters = []
    naturaldisaster_clusters = []
    classify_clusters_dic = {}
    schema_atc_clusters = []

    for i in range(len(schema_atc)):
        event_info = schema_atc[i]
        condition = 0
        for j in range(len(schema_atc_clusters)):
            # 只取每个簇的第一个
            event_info_ = schema_atc_clusters[j][0]
            score = atc_merge_condition(event_info, event_info_, synonym_info)
            if score == 1:
                condition += 1
                schema_atc_clusters[j].append(schema_atc[i])
                break

        if condition == 0:
            schema_atc_clusters.append([schema_atc[i]])

    for i in range(len(opendomain_schema_sen)):
        event_info = opendomain_schema_sen[i]
        condition = 0
        for j in range(len(opendomain_clusters)):
            # 只取每个簇的第一个
            event_info_ = opendomain_clusters[j][0]
            score = opendomain_merge_condition(event_info, event_info_, synonym_info)
            if score == 1:
                condition += 1
                opendomain_clusters[j].append(opendomain_schema_sen[i])
                break

        if condition == 0:
            opendomain_clusters.append([opendomain_schema_sen[i]])

    for i in range(len(classify_schema_dic['conference_schema'])):
        event_info = classify_schema_dic['conference_schema'][i]
        condition = 0
        for j in range(len(conference_clusters)):
            # 只取每个簇的第一个
            event_info_ = conference_clusters[j][0]
            score = conference_merge_condition(event_info, event_info_, synonym_info)
            if score == 1:
                condition += 1
                conference_clusters[j].append(classify_schema_dic['conference_schema'][i])
                break

        if condition == 0:
            conference_clusters.append([classify_schema_dic['conference_schema'][i]])

    for i in range(len(classify_schema_dic['investigation_schema'])):
        event_info = classify_schema_dic['investigation_schema'][i]
        condition = 0
        for j in range(len(investigation_clusters)):
            # 只取每个簇的第一个
            event_info_ = investigation_clusters[j][0]
            score = investigation_merge_condition(event_info, event_info_, synonym_info)
            if score == 1:
                condition += 1
                investigation_clusters[j].append(classify_schema_dic['investigation_schema'][i])
                break

        if condition == 0:
            investigation_clusters.append([classify_schema_dic['investigation_schema'][i]])

    for i in range(len(classify_schema_dic['naturaldisaster_schema'])):
        event_info = classify_schema_dic['naturaldisaster_schema'][i]
        condition = 0
        for j in range(len(naturaldisaster_clusters)):
            # 只取每个簇的第一个
            event_info_ = naturaldisaster_clusters[j][0]
            score = naturaldisaster_merge_condition(event_info, event_info_, synonym_info)
            if score == 1:
                condition += 1
                naturaldisaster_clusters[j].append(classify_schema_dic['naturaldisaster_schema'][i])
                break

        if condition == 0:
            naturaldisaster_clusters.append([classify_schema_dic['naturaldisaster_schema'][i]])

    classify_clusters_dic['naturaldisaster_clusters'] = naturaldisaster_clusters
    classify_clusters_dic['investigation_clusters'] = investigation_clusters
    classify_clusters_dic['conference_clusters'] = conference_clusters

    return opendomain_clusters,schema_atc_clusters,classify_clusters_dic


def classify_events(events_file):
    opendomain_schema_sen = []
    schema_atc = []
    fixativedomain_schema_sen = []
    conference_schema = []
    investigation_schema = []
    naturaldisaster_schema = []
    classify_schema_dic = {}

    with io.open(events_file,'r',encoding='utf-8') as f:
        while True:
            line = f.readline()                         #加一个判断类型的环节，如果是字典，说明是开放域，可以直接使用；如果是列表，说明一个列表里面装了一句话中包含的所有类型的schema
            if len(line) > 0:
                try:
                    schema_dic = json.loads(line)
                except:
                    logger.info("json load error: {}".format(line))
                    continue
                if schema_dic['extractScope'] == '开放域句子级':
                    opendomain_schema_sen.append(schema_dic)
                elif schema_dic['extractScope'] == '固定域句子级':
                    fixativedomain_schema_sen.append(schema_dic)
                elif schema_dic['extractScope'] == '篇章级':
                    schema_atc.append(schema_dic)
            else:
                break
    for schema_item in fixativedomain_schema_sen:
        if schema_item['schemaType'] == '会议':
            conference_schema.append(schema_item)
        elif schema_item['schemaType'] == '考察调研':
            investigation_schema.append(schema_item)
        elif schema_item['schemaType'] == '自然灾害':
            naturaldisaster_schema.append(schema_item)

    classify_schema_dic['conference_schema'] = conference_schema
    classify_schema_dic['investigation_schema'] = investigation_schema
    classify_schema_dic['naturaldisaster_schema'] = naturaldisaster_schema

    return opendomain_schema_sen,fixativedomain_schema_sen,schema_atc,classify_schema_dic



def load_synonym(file_path):
    synonym_info = {}

    with io.open(file_path, "r", encoding='utf-8') as f:
        while True:
            line = f.readline()
            line = line.strip()
            if len(line) > 0:
                if '=' in line:
                    words_in_line = line.split('=')[1].strip().split(' ')
                    # print(words_in_line)
                    for word in words_in_line:
                        if word not in synonym_info:
                            synonym_info[word] = set(words_in_line)
                        else:
                            synonym_info[word] = synonym_info[word] | set(words_in_line)

            else:
                break

    return synonym_info


def event_merge_condition(event_info, event_info_, synonym_info={}):
    triple_info = event_info['triple_info']
    sub = triple_info['triple'][0]
    v = triple_info['triple'][1]
    obj = triple_info['triple'][2]
    TMP = event_info['TMP']     #聚类时间这一要素暂时未加上，后期可考虑加上。可考虑这一情景事件肯定是不同的，加入两句话都有TMP，但是两句话出现的时间不同，此时可确切的认为两件事不可以merge
    # NERs = event_info['NERs']

    triple_info_ = event_info_['triple_info']
    sub_ = triple_info_['triple'][0]
    v_ = triple_info_['triple'][1]
    obj_ = triple_info_['triple'][2]
    TMP_ = event_info_['TMP']
    # NERs_ = event_info_['NERs']
    try:
        sub_score = len(set(sub) & set(sub_)) / min(
            [len(set(sub)), len(set(sub_))])  # 用两个triple中sub的所有的字数长度除以较短的sub的字数，得分评估了较短sub与较长sub的长度差距
    except:
        sub_score = 0

    # 排除掉空字符串的比较 ‘’与‘’
    if sub == sub_:
        sub_score = 1

    try:
        obj_score = len(set(obj) & set(obj_)) / min([len(set(obj)), len(set(obj_))])
    except:
        obj_score = 0

    if obj == obj_:
        obj_score = 1

    # 动作的包含关系
    v_score = len(set(v) & set(v_)) / min([len(set(v)), len(set(v_))])

    if v in v_ or v_ in v:
        v_score = 1
    # 动作的同义关系
    if v in synonym_info:
        if v_ in synonym_info[v]:
            v_score = 1
    if v_ in synonym_info:
        if v in synonym_info[v_]:
            v_score = 1
    if sub_score > 0.66 and obj_score > 0.66 and v_score > 0.66:
        return 1

    return 0

def choose_represent(opendomain_clusters, schema_atc_clusters, classify_clusters_dic):

    for item_cluster in opendomain_clusters:        #遍历装了所有簇的列表
        cluster_schema = []
        for item_schema in item_cluster:            #遍历一个簇中的所有schema
            schema = item_schema['schema']          #取出schema字典中的schema字段
            for item in schema:                     #遍历schema中的所有组成成分
                schema_name = item['name']
                for clutser in cluster_schema:

    pass

def opendomain_merge_condition(event_info, event_info_, synonym_info={}):

    schema_info = event_info['schema']
    sub = schema_info[0]['name']        #后期如果希望将聚类的词改为基词，可以在该处将取出的值进行替换为取出基词
    verb = schema_info[1]['name']
    obj = schema_info[2]['name']
    TMP = event_info['occurAt']

    schema_info_ = event_info_['schema']
    sub_ = schema_info_['name']
    verb_ = schema_info_['name']
    obj_ = schema_info_['name']
    TMP_ = event_info_['occurAt']

    try:
        sub_score = len(set(sub) & set(sub_)) / min(
            [len(set(sub)), len(set(sub_))])  # 用两个triple中sub的所有的字数长度除以较短的sub的字数，得分评估了较短sub与较长sub的长度差距。聚类时间这一要素暂时未加上，后期可考虑加上。可考虑这一情景事件肯定是不同的，加入两句话都有TMP，但是两句话出现的时间不同，此时可确切的认为两件事不可以merge
    except:
        sub_score = 0

    # 排除掉空字符串的比较 ‘’与‘’
    if sub == sub_:
        sub_score = 1

    try:
        obj_score = len(set(obj) & set(obj_)) / min([len(set(obj)), len(set(obj_))])
    except:
        obj_score = 0

    if obj == obj_:
        obj_score = 1

    # 动作的包含关系
    v_score = len(set(verb) & set(verb_)) / min([len(set(verb)), len(set(verb_))])

    if verb in verb_ or verb_ in verb:
        v_score = 1
    # 动作的同义关系
    if verb in synonym_info:
        if verb_ in synonym_info[verb]:
            v_score = 1
    if verb_ in synonym_info:
        if verb in synonym_info[verb_]:
            v_score = 1
    if sub_score > 0.66 and obj_score > 0.66 and v_score > 0.66:
        return 1

    return 0


def investigation_merge_condition(event_info, event_info_, synonym_info={}):
    triple_info = event_info['triple_info']
    sub = triple_info['triple'][0]
    v = triple_info['triple'][1]
    obj = triple_info['triple'][2]
    TMP = event_info['TMP']
    # NERs = event_info['NERs']

    triple_info_ = event_info_['triple_info']
    sub_ = triple_info_['triple'][0]
    v_ = triple_info_['triple'][1]
    obj_ = triple_info_['triple'][2]
    TMP_ = event_info_['TMP']
    # NERs_ = event_info_['NERs']
    try:
        sub_score = len(set(sub) & set(sub_)) / min(
            [len(set(sub)), len(set(sub_))])  # 用两个triple中sub的所有的字数长度除以较短的sub的字数，得分评估了较短sub与较长sub的长度差距
    except:
        sub_score = 0

    # 排除掉空字符串的比较 ‘’与‘’
    if sub == sub_:
        sub_score = 1

    try:
        obj_score = len(set(obj) & set(obj_)) / min([len(set(obj)), len(set(obj_))])
    except:
        obj_score = 0

    if obj == obj_:
        obj_score = 1

    # 动作的包含关系
    v_score = len(set(v) & set(v_)) / min([len(set(v)), len(set(v_))])

    if v in v_ or v_ in v:
        v_score = 1
    # 动作的同义关系
    if v in synonym_info:
        if v_ in synonym_info[v]:
            v_score = 1
    if v_ in synonym_info:
        if v in synonym_info[v_]:
            v_score = 1
    if sub_score > 0.66 and obj_score > 0.66 and v_score > 0.66:
        return 1

    return 0


def naturaldisaster_merge_condition(event_info, event_info_, synonym_info={}):
    triple_info = event_info['triple_info']
    sub = triple_info['triple'][0]
    v = triple_info['triple'][1]
    obj = triple_info['triple'][2]
    TMP = event_info['TMP']
    # NERs = event_info['NERs']

    triple_info_ = event_info_['triple_info']
    sub_ = triple_info_['triple'][0]
    v_ = triple_info_['triple'][1]
    obj_ = triple_info_['triple'][2]
    TMP_ = event_info_['TMP']
    # NERs_ = event_info_['NERs']
    try:
        sub_score = len(set(sub) & set(sub_)) / min(
            [len(set(sub)), len(set(sub_))])  # 用两个triple中sub的所有的字数长度除以较短的sub的字数，得分评估了较短sub与较长sub的长度差距
    except:
        sub_score = 0

    # 排除掉空字符串的比较 ‘’与‘’
    if sub == sub_:
        sub_score = 1

    try:
        obj_score = len(set(obj) & set(obj_)) / min([len(set(obj)), len(set(obj_))])
    except:
        obj_score = 0

    if obj == obj_:
        obj_score = 1

    # 动作的包含关系
    v_score = len(set(v) & set(v_)) / min([len(set(v)), len(set(v_))])

    if v in v_ or v_ in v:
        v_score = 1
    # 动作的同义关系
    if v in synonym_info:
        if v_ in synonym_info[v]:
            v_score = 1
    if v_ in synonym_info:
        if v in synonym_info[v_]:
            v_score = 1
    if sub_score > 0.66 and obj_score > 0.66 and v_score > 0.66:
        return 1

    return 0


def atc_merge_condition(event_info, event_info_, synonym_info={}):
    triple_info = event_info['triple_info']
    sub = triple_info['triple'][0]
    v = triple_info['triple'][1]
    obj = triple_info['triple'][2]
    TMP = event_info['TMP']
    # NERs = event_info['NERs']

    triple_info_ = event_info_['triple_info']
    sub_ = triple_info_['triple'][0]
    v_ = triple_info_['triple'][1]
    obj_ = triple_info_['triple'][2]
    TMP_ = event_info_['TMP']
    # NERs_ = event_info_['NERs']
    try:
        sub_score = len(set(sub) & set(sub_)) / min(
            [len(set(sub)), len(set(sub_))])  # 用两个triple中sub的所有的字数长度除以较短的sub的字数，得分评估了较短sub与较长sub的长度差距
    except:
        sub_score = 0

    # 排除掉空字符串的比较 ‘’与‘’
    if sub == sub_:
        sub_score = 1

    try:
        obj_score = len(set(obj) & set(obj_)) / min([len(set(obj)), len(set(obj_))])
    except:
        obj_score = 0

    if obj == obj_:
        obj_score = 1

    # 动作的包含关系
    v_score = len(set(v) & set(v_)) / min([len(set(v)), len(set(v_))])

    if v in v_ or v_ in v:
        v_score = 1
    # 动作的同义关系
    if v in synonym_info:
        if v_ in synonym_info[v]:
            v_score = 1
    if v_ in synonym_info:
        if v in synonym_info[v_]:
            v_score = 1
    if sub_score > 0.66 and obj_score > 0.66 and v_score > 0.66:
        return 1

    return 0


def conference_merge_condition(event_info, event_info_, synonym_info={}):
    schema_info = event_info['schema']
    conference_org = schema_info[0]['conference_org']
    conference_name = schema_info[1]['conference_name']
    conference_person = schema_info[2]['conference_person']
    TMP = event_info['occurAt']

    schema_info_ = event_info_['schema']
    conference_org_ = event_info_[0]['conference_org']
    conference_name_ = event_info_[1]['conference_name']
    conference_person_ = event_info_[2]['conference_person']
    TMP_ = event_info_['occurAt']

    try:
        conference_org_score = len(set(conference_org) & set(conference_org_)) / min(
            [len(set(conference_org)), len(set(conference_org_))])  # 用两个triple中sub的所有的字数长度除以较短的sub的字数，得分评估了较短sub与较长sub的长度差距
    except:
        conference_org_score = 0

    # 排除掉空字符串的比较 ‘’与‘’
    if conference_org == conference_org_:
        conference_org_score = 1

    try:
        conference_name_score = len(set(conference_name) & set(conference_name_)) / min([len(set(conference_name)), len(set(conference_name_))])
    except:
        conference_name_score = 0

    if conference_name == conference_name_:
        conference_name_score = 1

    # 动作的包含关系
    try:
        conference_person_score = len(set(conference_person) & set(conference_person_)) / min([len(set(conference_person)), len(set(conference_person_))])
    except:
        conference_person_score = 0

    if conference_person == conference_person_:
        conference_person_score = 1

    # 动作的同义关系
    # if v in synonym_info:
    #     if v_ in synonym_info[v]:
    #         v_score = 1
    # if v_ in synonym_info:
    #     if v in synonym_info[v_]:
    #         v_score = 1
    if conference_org_score > 0.66 and conference_name_score > 0.66 and conference_person_score > 0.66:
        return 1

    return 0