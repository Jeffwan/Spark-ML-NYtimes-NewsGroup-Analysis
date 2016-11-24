# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

# https://github.com/dfdeshom/scrapy-kafka/blob/master/requirements.txt

from scrapy.utils.serialize import ScrapyJSONEncoder
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer

from pymongo import MongoClient
from scrapy.exceptions import DropItem
from scrapy.conf import settings
import logging

class NytimesPipeline(object):
    def process_item(self, item, spider):
        return item

class MongoDBPipeline(object):

    def __init__(self):
        client = MongoClient(settings['MONGODB_SERVER'], settings['MONGODB_PORT'])
        #  client = MongoClient('localhost', 27017)
        db = client[settings['MONGODB_DB']]
        self.collection = db[settings['MONGODB_COLLECTION']]

    def process_item(self, item, spider):
        valid = True
        for data in item:
            if not data:
                valid = False
                raise DropItem("Missing %s of news from %s" %(data, item['url']))
        if valid:
            self.collection.insert(dict(item))
            logging.debug('Item wrote to MongoDB database %s/%s' %(settings['MONGODB_DB'], settings['MONGODB_COLLECTION']))
        return item


class KafkaPipeline(object):
    def __init__(self, producer, topic):
        self.producer = producer
        self.topic = topic
        self.encoder = ScrapyJSONEncoder()

    def process_item(self, item, spider):
        item = dict(item)
        item['spider'] = spider.name
        msg = self.encoder.encode(item)
        self.producer.send_message(self.topic, msg)

    @classmethod
    def from_settings(cls, settings):
        k_hosts = settings.get('SCRAPY_KFKA_HOSTS', ['localhost:9092'])
        topic = settings.get('SCRAPY_KFKA_ITEM_PIPELINE_TOPIC', 'scrapy_kafka_item')
        kafka = KafkaClient(k_hosts)
        conn = SimpleProducer(kafka)
        return cls(conn, topic)
