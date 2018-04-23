# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html
from scrapy.item import Item, Field


class MusicItem(Item):
    pass

class XiamiItem(Item):
    user = Field()
    song = Field()
    artist = Field()
    time = Field()
    scratch_time = Field()
    platform = Field()

class XiamiUser(Item):
    id = Field()
    name = Field()
    sex = Field()