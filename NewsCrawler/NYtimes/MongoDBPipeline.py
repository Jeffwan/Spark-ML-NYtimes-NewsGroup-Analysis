import pymongo from scrapy.exceptions
import DropItem from scrapy.conf
import settings from scrapy
import log
class MongoDBPipeline(object):

    def __init__(self):
        connection = pymongo.Connection(settings['MONGODB_SERVER'], settings['MONGODB_PORT'])
        db = connection[settings['MONGODB_DB']]
        self.connection = db[settings['MONGODB_COLLECTION']]

    def process_item(self, item, spider):
        valid = True
        for data in item:
            if not data:
                valid = False
                raise DropItem("Missing %s of news from %s" %(data, item['url']))
            if valid:
                self.collection.insert(dict(item))
                log.msg('Item wrote to MongoDB database %s/%s'
                %(settings['MONGODB_DB'], settings['MONGODB_COLLECTION']), level=log.DEBUG, spider=spider)

        return item
