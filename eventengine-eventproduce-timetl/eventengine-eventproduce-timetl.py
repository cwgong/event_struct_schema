# -*- coding: utf-8 -*-

# 1、新增的 schema cluster 需要 merge 到 “最新状态” final event
# 2、对于已被 merge 到 “最新状态” final event 中的 schema cluster，状态改变后需要更新 hot 
# 3、推 kafka
# 4、对于每一个 final event
# -------------------------------------------------- #
# load topic info
# min publishAt: (start, end)
# max piblishAt: (start, end)
# 实时： 近 7 天 ————> max piblishAt: (7 days ago, current time)
# doc cluster（上同）：
#           1、▽增量（size > 2）
#           2、当前 topic 关联 (全量 or 近7天):
#                   重跑时会找不到 docluster id（没问题）。
# 重跑：min piblishAt
# 
# -------------------------------------------------- #

from threading import Timer
import time_utils
import download_data
from get_docfeature import get_doc_abstract
import save_result
import cluster
import merge_schema

# 解决 mac os torch 报错 libomp
import os

os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"

import logging.config

logging.config.fileConfig("logging.conf")
logger = logging.getLogger('main')


def doing_job():
    logging.info('doing_job() ...')

    # -------------------------------------------------- #
    # step 1
    # get increment by compare
    # -------------------------------------------------- #
    compare_gap = 3  # Params
    start_time = time_utils.n_days_ago(compare_gap)
    end_time = time_utils.current_time()

    logger.info("compare_gap: {}".format(compare_gap))
    logger.info("start_time: {}".format(start_time))
    logger.info("end_time: {}".format(end_time))

    original_data_file = './logs/original_data_file.txt'
    extradata_file = './logs/extradata_file.txt'

    # 获取增量数据
    # 其中data_file是包含所有数据的一个文件名
    data_file, temp_data = download_data.get_extradata_from_api(start_time, end_time, original_data_file,
                                                                extradata_file)



    # -------------------------------------------------- #
    # step 2
    # doc 蕨类所需特征抽取 increment = data_file
    # doc ——> abstract
    # -------------------------------------------------- #
    # data_file_ = './logs/abstract_file.txt'
    # data_file_ = get_doc_abstract(data_file, data_file_)

    # 太少的“有效”数据不做聚类
    data_size = download_data.count_data_size(data_file)
    logging.info("data_size: {}".format(data_size))
    if data_size < 50:
        logger.info("data size <  {}".format(data_size))
        return
    # 覆盖掉 original_data_file
    download_data.update_original_data_file(temp_data, original_data_file)
    temp_data = []  # 清空

    # -------------------------------------------------- #
    # step 3
    # doc cluster
    # -------------------------------------------------- #
    # segment & NER
    opendomain_clusters, schema_atc_clusters, classify_clusters_dic = merge_schema.event_merge(data_file)

    merge_schema.choose_represent(opendomain_clusters, schema_atc_clusters, classify_clusters_dic)

    save_result.save_cluster_result_to_api(opendomain_clusters, schema_atc_clusters, classify_clusters_dic)

    ner_content_data, word_content_data, text_data, raw_data, info_ids = cluster.fetch_data(data_file)
    # 增量聚类
    origin_cluster_file_path = 'logs/origin_cluster.txt'
    origin_cluster_result = cluster.load_origin_cluster_result(origin_cluster_file_path, end_time)  #此处可默认没有簇文件，聚类功能完成后再完成此部分代码
    cluster_result = cluster.cluster(origin_cluster_result, ner_content_data, word_content_data, text_data, raw_data)

    # -------------------------------------------------- #
    # step 4
    # save local & api
    # -------------------------------------------------- #
    save_result.save_cluster_result_to_file(cluster_result, origin_cluster_file_path)
    save_result.save_cluster_result_to_api(cluster_result)

    logging.info('doing_job() over!')


def excute_timing():
    global t

    try:
        doing_job()
    except Exception as e:
        logger.exception(e)

    time_gap = int((4) * 60 * 60 * 1000)  # 间隔 n 小时（由于执行时间较长，可能为45分钟左右)
    t = Timer(time_gap / 1000, excute_timing)
    t.start()


if __name__ == '__main__':

    # doing_job()

    try:
        time_gap = int((1 / 3600) * 60 * 60 * 1000)
        t = Timer(time_gap / 1000, excute_timing)
        t.start()
    except Exception as e:
        logger.exception(e)
