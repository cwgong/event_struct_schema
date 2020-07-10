# -*- coding: utf-8 -*-

from threading import Timer
import time_utils
import download_data
import save_result

import logging.config

logging.config.fileConfig("logging.conf")
logger = logging.getLogger('main')

# 1、新增的 doc cluster 需要 merge 到 “最新状态” topic
# 2、对于已被 merge 到 “最新状态” topic 中的 doc cluster，状态改变后需要更新
# 3、推 kafka
# 4、按（min publishAt，max publishAt） 搜索（重跑、跑历史）
# -------------------------------------------------- #
# load topic info
# min publishAt: (start, end)
# max piblishAt: (start, end)
# 实时： 近 7 天 ————> max piblishAt: (7 days ago, current time)
# doc cluster（上同）：
#           1、▽增量（size > 3）
#           2、当前 topic 关联 (全量 or 近7天):
#                   重跑时会找不到 docluster id（没问题）。
# 重跑：(min piblishAt，current time)
# 
# -------------------------------------------------- #
def doing_job():
    logging.info('doing_job() ...')
    
    # -------------------------------------------------- #
    # get topic now
    # -------------------------------------------------- #
    compare_gap = 7 # Params
    start_time = time_utils.n_days_ago(compare_gap)
    end_time = time_utils.current_time()
    
    # 10w per year
    # humanFlag = 1, delFlag = 1, mergeFlag = 1, nomal
    topics_info = load_topics("max_publishAt", start_time, end_time)
    
    # humanFlag = 1, delFlag = 1, mergeFlag = 1
    old_docluster_ids = get_docluster_ids(topics_info)
    
    doclusters_info = load_doclusters("max_publishAt", start_time, end_time, size = 3)
    
    increament_docluster_ids = 
    
    '''
    min publishAt
    max publishAt
    topic
    hot
    keywords = []
    features = []
    abstract = ''
    all_titles = []
    cluster_ids = []
    '''
      
    # 可能一对多，按顺序 humanFlag = 1, delFlag = 1, nomal
    topic_classify(topics_info, increament_docluster_ids)
        # 全量更新
        update_topics_info
    
    
    
    
    compare_gap = 5 # Params
    start_time = time_utils.n_days_ago(compare_gap)
    end_time = time_utils.current_time()

    logger.info("compare_gap: {}".format(compare_gap))
    logger.info("start_time: {}".format(start_time))
    logger.info("end_time: {}".format(end_time))
    
    original_data_file = './logs/original_data_file.txt'
    extradata_file = './logs/extradata_file.txt'
    
    # 获取增量数据(只选择 size > 3 的簇 merge topic)
    size = 3
    data_file, temp_data = download_data.get_extradata_from_api(start_time, end_time, original_data_file, extradata_file, size)
    
    # 太少的数据不做聚类
    data_size = download_data.count_data_size(data_file)
    logging.info("data_size: {}".format(data_size))   
    if data_size < 1:
        logger.info("data size < 100: {}".format(data_size))
        return
    
    # 覆盖掉 original_data_file
    download_data.update_original_data_file(temp_data, original_data_file)    
    temp_data = [] # 清空
    
    # -------------------------------------------------- #
    # load topic info
    # -------------------------------------------------- #
                
                
    # -------------------------------------------------- #
    # merge
    # -------------------------------------------------- #
    
    
    # -------------------------------------------------- #
    # save
    # -------------------------------------------------- #
 
    save_result.save_cluster_result_to_file(cluster_result, origin_cluster_file_path)

    logging.info('doing_job() over!')



def excute_timing(): 
    
    global t
    
    try:
        doing_job()
    except Exception as e:
        logger.exception(e) 
        
    time_gap = int((4)*60*60*1000) # 间隔 n 小时（由于执行时间较长，可能为45分钟左右）
    t = Timer(time_gap/1000, excute_timing)
    t.start()
    
    
    
if __name__ == '__main__':
    
    try:
        time_gap = int((1/3600)*60*60*1000) 
        t = Timer(time_gap/1000, excute_timing)
        t.start()
    except Exception as e:
        logger.exception(e) 