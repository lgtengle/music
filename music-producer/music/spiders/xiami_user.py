#!/usr/bin/python
# -*- coding:utf-8 -*-
from scrapy import Spider
from music.items import XiamiUser
from scrapy import Request
from wsgiref.simple_server import make_server

class DmozSpider(Spider):

    def __init__(self):
        self.headers = {
            'Content-Type':'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept-Encoding':'gzip, deflate',
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'
        }

    name = "xiami_user"
    allowed_domains = ["www.xiami.com"]

    start_urls = [
        "http://www.xiami.com/space/fans/u/3069638",
        "http://www.xiami.com/space/following/u/3069638"
    ]
    def parse(self, response):
        try:
            if (response.url.find("/page/") < 0):#添加下一页
                page = len(response.xpath('//*[@id="column695"]/div[2]/div[1]/div/div[3]/div[2]/a'))
                for i in range(2, page-1):
                    if (i > 4):
                        break;
                    else:
                        yield Request(response.url + "/page/" + i)
        except BaseException:
            print("error--------url:"+response.url)

        #获取用户信息
        for p in response.xpath('//*[@id="p-nowrap"]/div[1]/div[2]/div[1]/div/div[3]/div[1]/ul/li'):
            item = XiamiUser()
            id = p.xpath('./div[2]/p[1]/a/@href').extract()[0][3:]
            name = p.xpath('./div[2]/p[1]/a/text()').extract()[0]
            sex = p.xpath('./div[2]/text()').extract()[0].replace("\n", "").replace("\t", "").strip()

            item['id'] = id
            item['name'] = name
            item['sex'] = sex
            yield Request("http://www.xiami.com/space/fans/u/" + id)
            yield Request("http://www.xiami.com/space/following/u/" + id)
            yield item
