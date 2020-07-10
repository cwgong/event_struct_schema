# -*- encoding:utf8 -*-

import json
import logging.config
import codecs

logging.config.fileConfig("logging.conf")
logger = logging.getLogger('main')


def save_cluster_result_to_file(cluster_result, origin_cluster_file_path):
    
    fout = codecs.open(origin_cluster_file_path, 'w', encoding='utf-8')
    for item in cluster_result:
        score = item['score']
        title = item['title']
        keywords = item['keywords']
        cluster_id = item['id']
        info_ids = item['info_ids']
        all_titles = item['all_titles']
        publish_time = item['publish_time']
        min_publishtime = item['min_publishtime']
        max_publishtime = item['max_publishtime']
        title_keywords_dic = item['title_keywords_dic']
        content_keywords = item['content_keywords']
        strObj = json.dumps(item, ensure_ascii=False)
        fout.write(strObj+'\n')
        if len(info_ids) > 3:
            logger.info('score: '+str(score))
            logger.info('title: '+str(title))
            logger.info('cluster_id: '+str(cluster_id))
            logger.info('hot: '+str(len(info_ids)))
            logger.info('info_ids: '+str(info_ids))
            logger.info('keywords: '+str(keywords))
            logger.info('title_keywords_dic: '+str(title_keywords_dic))
            logger.info('all_titles: '+str(all_titles))
            logger.info('content_keywords: '+str(content_keywords))
            logger.info('publish_time: '+str(publish_time))
            logger.info('min_publishtime: '+str(min_publishtime))
            logger.info('max_publishtime: '+str(max_publishtime))
            logger.info('--------------------------')
        
    fout.close()


if __name__ == '__main__':
    
    print('save_cluster_result...')
