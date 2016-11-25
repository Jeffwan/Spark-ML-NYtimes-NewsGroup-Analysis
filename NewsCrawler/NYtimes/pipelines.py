# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

# https://github.com/dfdeshom/scrapy-kafka/blob/master/requirements.txt
# https://github.com/dpkp/kafka-python new Kafka API
# http://kafka-python.readthedocs.io/en/master/simple.html?highlight=SimpleProducer  (deprecated)

from scrapy.utils.serialize import ScrapyJSONEncoder
from kafka.client import KafkaClient
from kafka import KafkaProducer


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

        # Key is null, value is JSON object.
        #(null, {
        #"category": "international",
        #"title": "Its Charter Expired, Export-Import Bank Will Keep the Doors Open",
        #"author": "By JACKIE CALMES",
        #"spider": "NYtimes",
        #"link": "http://www.nytimes.com/2015/07/01/business/international/though-charter-is-expiring-export-import-bank-will-keep-its-doors-open.html",
        #"date": "June 30, 2015",
        #"article": ["Advertisemen.."]

        self.producer.send(self.topic, msg)

    @classmethod
    def from_settings(cls, settings):
        kafka_hosts = settings.get('SCRAPY_KAFKA_HOSTS')
        topic = settings['SCRAPY_KAFKA_ITEM_PIPELINE_TOPIC']

        producer = KafkaProducer(bootstrap_servers = kafka_hosts)
        return cls(producer, topic)
