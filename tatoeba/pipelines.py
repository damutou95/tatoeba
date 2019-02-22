# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
import hashlib
import time
import logging
from pymongo.errors import DuplicateKeyError


class TatoebaPipeline(object):

    def process_item(self, item, spider):
        client = pymongo.MongoClient(host='127.0.0.1', port=27017)
        db = client['tatoeba']
        col = db[f'{item["language"]}']
        hash = hashlib.md5()
        hash.update((item['chinese'] + item['target']).encode('utf-8'))
        hashCode = hash.hexdigest()
        data = {'_id': hashCode, 'chinese': item['chinese'], 'target': item['target'], 'time': time.strftime('%Y.%m.%d %H:%M:%S', time.localtime())}
        try:
            col.insert(data)
            logging.info('#' * 20 + '成功插入一条数据！')
        except DuplicateKeyError:
            logging.info('#' * 20 + '成功过滤重复请求！')

        return item

