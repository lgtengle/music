# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json
import codecs
from scrapy.exceptions import DropItem

class MusicPipeline(object):
    def __init__(self):
        self.file = codecs.open('user.json', 'ab+', encoding = 'utf-8')
        self.ids_seen = set()

    def process_item(self, item, spider):
        # line = json.dumps(dict(item), ensure_ascii=False) + '\n'
        # self.file.write(line)
        # return item
        if  item['id'] in  self.ids_seen:
            raise DropItem("Duplicate item found: %s" % item)
        else:
            self.ids_seen.add(item['id'])
            line = json.dumps(dict(item), ensure_ascii=False) + '\n'
            self.file.write(line)
            return item

    def close_spider(self, spider):
        self.file.close()
